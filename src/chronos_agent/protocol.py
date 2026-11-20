"""Transport and data types for Chronos Control's existing v1 agent API."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import IntEnum
from ftplib import FTP, all_errors as ftp_errors
from http.client import HTTPException
from itertools import chain
import json
import math
import os
from pathlib import Path
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4


class AgentError(Exception):
    """Chronos API or transport failure."""


class ProtocolError(AgentError):
    """Unexpected HTTP response or unsupported server data."""


class TransportError(AgentError):
    """HTTP exchange failed without an application response."""


class UncertainWrite(AgentError):
    """The server may have applied an operation without acknowledging it."""


class ApiError(AgentError):
    def __init__(self, operation: str, code: int, message: str):
        self.code = code
        super().__init__(f"{operation}: Chronos {code}: {message}")


class Status(IntEnum):
    FAILED = -2
    ABORTED = -1
    SCHEDULED = 0
    SETUP = 1
    RUNNING = 2
    FINISHED = 3


class Phase(IntEnum):
    PREPARE = 1
    WARMUP = 2
    EXECUTE = 3
    ANALYZE = 4
    CLEAN = 5

    def excluded(self, mask: int) -> bool:
        return bool(mask & (1 << (self - 1)))


@dataclass(frozen=True)
class Job:
    id: int
    status: Status
    system: str
    environment: str
    phases: int
    cdl: str = field(repr=False)
    cem_setup: dict[str, Any] | None = field(default=None, repr=False)


@dataclass(frozen=True)
class CemNode:
    unique_id: str
    version: int
    environment: str

    def __post_init__(self) -> None:
        if (not self.unique_id.strip() or not self.environment.strip()
                or type(self.version) is not int or self.version < 1):
            raise ValueError("CEM requires a node ID, a positive version, and an environment.")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _url(value: str):
    if any(c.isspace() or ord(c) < 32 for c in value):
        raise ValueError("URL contains whitespace or control characters.")
    parsed = urlsplit(value)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username is not None or "?" in value or "#" in value):
        raise ValueError("Expected an HTTP(S) URL without credentials, a query or a fragment.")
    return parsed


def _origin(parsed) -> tuple[str, str, int]:
    return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


def _segments(fields: dict[str, str | int]) -> str:
    return "".join(f"/{key}={quote(str(value), safe='')}" for key, value in fields.items())


def _object(value: Any, name: str) -> dict:
    if not isinstance(value, dict):
        raise ProtocolError(f"{name} must be an object.")
    return value


def _job_id(value: Any) -> int:
    if type(value) is not int or value < 1:
        raise ProtocolError("Expected a positive integer job ID.")
    return value


def _status(value: Any) -> Status:
    if type(value) is int and value in Status._value2member_map_:
        return Status(value)
    raise ProtocolError("Unknown job status.")


def _archive_parts(size: int) -> tuple[dict[str, str], bytes, bytes]:
    boundary = uuid4().hex
    prefix = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="result"; filename="result.zip"\r\n'
        "Content-Type: application/zip\r\n\r\n"
    ).encode("ascii")
    suffix = f"\r\n--{boundary}--\r\n".encode("ascii")
    headers = {
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "Content-Length": str(len(prefix) + size + len(suffix)),
    }
    return headers, prefix, suffix


def _job(value: Any) -> Job:
    data = _object(value, "Job response")
    if any(not isinstance(data.get(key), str) for key in ("system", "environment", "cdl")):
        raise ProtocolError("Job response requires system, environment and CDL strings.")
    phases = data.get("phases")
    if type(phases) is not int or not 0 <= phases <= 31:
        raise ProtocolError("Invalid job phase exclusion mask.")
    return Job(_job_id(data.get("id")), _status(data.get("status")), data["system"],
               data["environment"], phases, data["cdl"])


class Client:
    """Legacy v1 transport with retries for reads."""

    _response_limit = 2_097_152

    def __init__(
        self, base_url: str, *, timeout: float = 10, read_retries: int = 2,
        retry_delay: float = 0.25, front_controller: bool = False,
        upload_origin: str | None = None, ftp_host: str | None = None,
    ):
        parsed = _url(base_url)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("HTTP timeout must be positive and finite.")
        if type(read_retries) is not int or not 0 <= read_retries <= 5:
            raise ValueError("Read retries must be an integer between 0 and 5.")
        if not math.isfinite(retry_delay) or not 0 <= retry_delay <= 5:
            raise ValueError("Retry delay must be between 0 and 5 seconds.")
        self.base_url = base_url.rstrip("/")
        self.prefix = self.base_url + ("/index.php?/api/v1/" if front_controller else "/api/v1/")
        self.origin = _origin(parsed)
        self.upload_origins = {self.origin}
        if upload_origin:
            upload = _url(upload_origin)
            if upload.path not in ("", "/"):
                raise ValueError("Upload origin must contain only scheme, host and optional port.")
            self.upload_origins.add(_origin(upload))
        self.ftp_host = ftp_host
        self.timeout = timeout
        self.read_retries = read_retries
        self.retry_delay = retry_delay
        self.opener = build_opener(_NoRedirect())

    def _request(self, operation, method, url, *, fields=None, body=None, headers=None,
                 retry=False, mutates=False, probe_timeout=None):
        headers = {"Accept": "application/json", **(headers or {})}
        if fields is not None:
            body = urlencode(fields).encode("ascii")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        attempts = 1 + (self.read_retries if retry else 0)
        timeout = self.timeout if probe_timeout is None else min(self.timeout, probe_timeout)
        for attempt in range(attempts):
            try:
                return self._exchange(operation, Request(url, body, headers, method=method), timeout)
            except TransportError as error:
                if attempt + 1 == attempts:
                    if mutates:
                        raise UncertainWrite(
                            f"{operation}: acknowledgement lost. Inspect the server state before retrying."
                        ) from error
                    raise
                time.sleep(min(2, self.retry_delay * 2**attempt))

    def _exchange(self, operation, request, timeout):
        try:
            response = self.opener.open(request, timeout=timeout)
        except HTTPError as error:
            if error.code in (408, 429) or 500 <= error.code <= 599:
                error.close()
                raise TransportError(f"{operation}: HTTP {error.code}.") from error
            response = error
        except (URLError, OSError, HTTPException) as error:
            raise TransportError(f"{operation}: {error}.") from error

        with response:
            status = response.code
            try:
                raw = response.read(self._response_limit + 1)
            except (OSError, HTTPException) as error:
                raise TransportError(f"{operation}: response read failed.") from error
            if len(raw) > self._response_limit:
                limit_mib = self._response_limit // (1024 * 1024)
                raise TransportError(f"{operation}: Chronos response exceeds {limit_mib} MiB.")
            length = response.headers.get("Content-Length")
            if length is not None and (length.lstrip("0") or "0") != str(len(raw)):
                raise TransportError(f"{operation}: incomplete Chronos response.")
            try:
                envelope = json.loads(raw)
                code = envelope["status"]["code"]
                message = envelope["status"]["message"]
                if type(code) is not int or not isinstance(message, str):
                    raise ValueError("invalid envelope types")
            except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
                raise TransportError(f"{operation}: invalid Chronos envelope (HTTP {status}).") from error
            if code != 200:
                raise ApiError(operation, code, message)
            if not 200 <= status < 300:
                raise TransportError(f"{operation}: unexpected HTTP {status}.")
            return envelope.get("response")

    def poll(self, systems: tuple[str, ...], environment: str = "default") -> Job | None:
        if not systems or any(not system.strip() or "," in system for system in systems):
            raise ValueError("Pass one or more nonempty Chronos system identifiers without commas.")
        if not environment.strip() or environment.strip() == "0":
            raise ValueError("Pass an environment name accepted by the v1 API.")
        url = self.prefix + "job" + _segments(
            {"id": "next", "supports": ",".join(systems), "environment": environment}
        )
        try:
            return _job(self._request("poll system queue", "GET", url, retry=True))
        except ApiError as error:
            if error.code != 601:
                raise
            return None

    def get_job(self, job_id: int, *, probe_timeout: float | None = None) -> Job:
        url = self.prefix + "job" + _segments({"id": _job_id(job_id)})
        job = _job(self._request(
            f"read job {job_id}", "GET", url, retry=probe_timeout is None,
            probe_timeout=probe_timeout,
        ))
        if job.id != job_id:
            raise ProtocolError(f"Chronos returned another job for job {job_id}.")
        return job

    def poll_cem(self, node: CemNode) -> Job | None:
        try:
            payload = self._request("poll CEM queue", "GET", self._cem_url(node), mutates=True)
        except ApiError as error:
            if error.code != 601:
                raise
            return None
        data = _object(payload, "CEM response")
        reference = _object(data.get("job"), "CEM job")
        setup = _object(data.get("setup"), "CEM setup")
        if not isinstance(data.get("cdl"), str):
            raise ProtocolError("CEM response requires a CDL string.")
        job = self.get_job(_job_id(reference.get("id")))
        if _status(reference.get("status")) != Status.SCHEDULED:
            raise ProtocolError("CEM returned a job that was not scheduled.")
        return replace(job, cdl=data["cdl"], cem_setup=setup)

    def cem_state(self, node: CemNode, job_id: int, *, started: bool) -> None:
        action = "jobStarted" if started else "jobTerminated"
        self._request(action, "POST", self._cem_url(node) + _segments({"action": action}),
                      fields={"jobId": _job_id(job_id)}, mutates=True)

    def _cem_url(self, node: CemNode) -> str:
        return self.prefix + "cem" + _segments({
            "uniqueId": node.unique_id, "version": node.version, "environment": node.environment,
        })

    def update_job(
        self, job_id: int, *, status: Status | None = None, phase: Phase | None = None,
        progress: int | None = None, result: dict[str, Any] | None = None,
    ) -> None:
        fields = {}
        if status is not None:
            fields["status"] = int(Status(status))
        if phase is not None:
            fields["currentPhase"] = int(Phase(phase))
        if progress is not None:
            if type(progress) is not int or not 0 <= progress <= 100 or phase is not None:
                raise ValueError("Send progress (0–100) separately from a phase change.")
            fields["progress"] = progress
        if result is not None:
            if not isinstance(result, dict):
                raise ValueError("Result must be a JSON object.")
            fields["result"] = json.dumps(result, ensure_ascii=False, allow_nan=False)
        if not fields:
            raise ValueError("No job fields to update.")
        self._request(f"update job {job_id} ({', '.join(fields)})", "PATCH",
                      self.prefix + "job" + _segments({"id": _job_id(job_id)}),
                      fields=fields, mutates=True)

    def append_log(self, job_id: int, text: str) -> None:
        if text in ("", "0"):
            raise ValueError("Log text must be nonempty under the v1 API.")
        url = self.prefix + "job" + _segments({"id": _job_id(job_id), "action": "appendLog"})
        self._request(f"append log for job {job_id}", "POST", url,
                      fields={"log": text}, mutates=True)

    def upload_archive(self, job_id: int, archive: Path) -> None:
        target_url = self.prefix + "job" + _segments({
            "id": _job_id(job_id), "action": "getUploadTarget",
        })
        target = _object(
            self._request("get upload target", "POST", target_url, fields={}), "Upload target"
        )
        if target.get("method") == "ftp":
            self._ftp_upload(job_id, target, archive)
        elif target.get("method") == "http":
            self._http_upload(job_id, target, archive)
        else:
            raise ProtocolError("Unsupported Chronos upload method.")

    def _http_upload(self, job_id: int, target: dict, archive: Path) -> None:
        hostname, path = target.get("hostname"), target.get("path")
        if not isinstance(hostname, str) or path != "/api/v1/job":
            raise ProtocolError("Invalid HTTP upload target.")
        if not hostname:
            url = self.prefix + "job"
        else:
            try:
                parsed = _url(hostname)
                if _origin(parsed) not in self.upload_origins:
                    raise ValueError("Upload origin is not authorized.")
            except ValueError as error:
                raise ProtocolError("HTTP upload origin must match the server or configured upload origin.") from error
            url = hostname.rstrip("/") + path
        url += _segments({"id": job_id, "action": "upload"})
        with archive.open("rb") as source:
            headers, prefix, suffix = _archive_parts(os.fstat(source.fileno()).st_size)
            body = chain([prefix], iter(lambda: source.read(65_536), b""), [suffix])
            self._request(f"upload archive for job {job_id}", "POST", url, body=body,
                          headers=headers, mutates=True)

    def _ftp_upload(self, job_id: int, target: dict, archive: Path) -> None:
        host, username, password = (target.get(key) for key in ("hostname", "username", "password"))
        if not self.ftp_host or host != self.ftp_host:
            raise ProtocolError("FTP requires an explicitly configured matching host.")
        if target.get("path") != "/chronos/evaluation/" or target.get("filename") != f"{job_id}.zip":
            raise ProtocolError("Unexpected FTP upload path or filename.")
        if any(not isinstance(value, str) or "\n" in value or "\r" in value
               for value in (username, password)):
            raise ProtocolError("Invalid FTP credentials.")
        port = target.get("port")
        if (type(port) not in (str, int) or not str(port).isascii() or not str(port).isdigit()
                or len(str(port)) > 5 or not 1 <= int(port) <= 65535):
            raise ProtocolError("Invalid FTP port.")
        with archive.open("rb") as source, FTP(timeout=self.timeout) as ftp:
            try:
                ftp.connect(host, int(port))
                ftp.login(username, password)
                ftp.cwd(target["path"])
            except ftp_errors:
                raise TransportError("Unable to connect, log in, or select the FTP upload directory.") from None
            try:
                ftp.storbinary(f"STOR {job_id}.zip", source)
            except ftp_errors:
                raise UncertainWrite(f"FTP upload for job {job_id} was not acknowledged.") from None
            finally:
                # The completion reply to STOR establishes delivery; close without a second command.
                ftp.close()
