"""Run a digest-verified command driver for one claimed repetition."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import tempfile
import time
from typing import BinaryIO
from zipfile import ZIP_DEFLATED, ZipFile

from .agent import Cancelled, JobTimeout, RunReport, StopSignal
from .attempt_evidence import collect_driver_evidence, open_directory
from .attempt_protocol import Attempt, AttemptClient, MAX_OUTPUT, SHA256_PATTERN
from .command import _stop_group
from .protocol import AgentError, ApiError, Status, UncertainWrite


class DriverFailed(Exception):
    """The driver or a required input could not produce a measurement."""


class AttemptStopped(Exception):
    """Chronos changed the state of the claimed repetition."""


@dataclass(frozen=True, kw_only=True)
class AttemptReport(RunReport):
    attempt_id: int


def _stage(entry: dict, cache: Path, destination: Path, stop: StopSignal) -> dict[str, str | int]:
    role, digest, size = entry.get("role"), entry.get("sha256"), entry.get("sizeBytes")
    if (not isinstance(role, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,99}", role)
            or not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest)
            or type(size) is not int or size < 0):
        raise DriverFailed("The frozen manifest contains an invalid artifact identity.")

    # Open without following a symlink and verify the descriptor that supplies the bytes.
    try:
        descriptor = os.open(cache / digest, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise DriverFailed(f"Unable to open cached artifact for role {role}: {error.strerror}.") from error

    target = destination / role
    actual_digest = sha256()
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size != size:
            raise DriverFailed(f"Cached artifact for role {role} has an incorrect size or file type.")
        with target.open("xb") as output:
            remaining = size
            while remaining:
                _check_stop(stop)
                chunk = source.read(min(remaining, MAX_OUTPUT))
                if not chunk:
                    raise DriverFailed(f"Cached artifact for role {role} ended early.")
                output.write(chunk)
                actual_digest.update(chunk)
                remaining -= len(chunk)
            if source.read(1):
                raise DriverFailed(f"Cached artifact for role {role} grew during staging.")
    observed_digest = actual_digest.hexdigest()
    if observed_digest != digest:
        raise DriverFailed(f"SHA-256 mismatch for artifact role {role}.")
    target.chmod(0o500 if role == "driver" else 0o400)
    return {"sha256": observed_digest, "sizeBytes": size}


def _manifest(spec: dict) -> list[dict]:
    manifest = spec.get("artifacts")
    if (not isinstance(manifest, dict) or type(manifest.get("schemaVersion")) is not int
            or manifest["schemaVersion"] != 1 or not isinstance(manifest.get("artifacts"), list)):
        raise DriverFailed("The frozen specification requires a version 1 artifact manifest.")
    entries = manifest["artifacts"]
    if len(entries) > 30 or any(not isinstance(entry, dict) or not isinstance(entry.get("role"), str)
                               for entry in entries):
        raise DriverFailed("The frozen artifact manifest contains invalid entries.")
    roles = {entry["role"] for entry in entries}
    if len(roles) != len(entries) or "driver" not in roles:
        raise DriverFailed("The frozen manifest requires distinct roles and a driver artifact.")
    return entries


def _check_stop(stop: StopSignal) -> None:
    if stop.is_set():
        raise Cancelled("The local agent was interrupted.")


def _read_evidence(source: BinaryIO) -> bytes:
    source.seek(0)
    return source.read(MAX_OUTPUT)


def _result(source: BinaryIO) -> str:
    source.seek(0)
    raw = source.read(MAX_OUTPUT + 1)
    if len(raw) > MAX_OUTPUT:
        raise DriverFailed("Driver stdout exceeds the 1 MiB result limit.")
    try:
        text = raw.decode("utf-8")
        result = json.loads(text)
        if not isinstance(result, dict):
            raise ValueError("expected an object")
        json.dumps(result, allow_nan=False)
    except (ValueError, RecursionError) as error:
        raise DriverFailed("Driver stdout must contain a finite UTF-8 JSON result object.") from error
    return text


def _reason(error: Exception) -> str:
    text = str(error).encode("utf-8", "replace").decode()
    cleaned = "".join(char for char in text if char in "\t\r\n"
                      or 32 <= ord(char) and char not in "\x7f\ufffe\uffff")
    return cleaned.strip()[:3000] or type(error).__name__


class AttemptAgent:
    def __init__(
        self, client: AttemptClient, *, worker: str, systems: tuple[str, ...], environment: str,
        cache: Path, directory: Path, stop: StopSignal, timeout: float, status_interval: float,
        handoff: tuple[int, str] | None = None,
    ):
        self.client = client
        self.worker = worker
        self.systems = systems
        self.environment = environment
        self.cache = cache.resolve(strict=True)
        if not self.cache.is_dir():
            raise ValueError("--artifact-cache must be a directory.")
        if os.name != "posix":
            raise ValueError("Command-driver attempts require POSIX process groups.")
        self.directory = directory.resolve()
        self.stop = stop
        self.timeout = timeout
        self.status_interval = status_interval
        self.handoff = handoff

    def run_once(self) -> AttemptReport | None:
        if self.stop.is_set():
            return None
        if self.handoff is None:
            attempt = self.client.claim(self.worker, self.systems, self.environment)
        else:
            attempt = self.client.attach(*self.handoff)
        if attempt is None:
            return None
        self.directory.mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(
            prefix=f"job-{attempt.repetition_id}-attempt-{attempt.attempt_id}-", dir=self.directory,
        ))
        try:
            if attempt.status == Status.ABORTED:
                raise AttemptStopped("Chronos aborted the attempt.")
            if attempt.status != Status.SETUP:
                raise AgentError("The claimed attempt is no longer awaiting its start. Inspect it in Chronos.")
            (directory / "specification.json").write_text(attempt.specification_json + "\n", encoding="utf-8")
            outcome, message = self._execute(attempt, directory)
        except AttemptStopped as error:
            outcome, message = "stopped", str(error)
        except (AgentError, OSError) as error:
            outcome, message = "error", str(error)
        return AttemptReport(attempt.repetition_id, directory, outcome, message, attempt_id=attempt.attempt_id)

    def _execute(self, attempt: Attempt, directory: Path) -> tuple[str, str]:
        spec = json.loads(attempt.specification_json)
        environment = {
            "hostname": platform.node(), "os": platform.platform(),
            "architecture": platform.machine(), "python": platform.python_version(),
        }
        verified = {}
        with tempfile.TemporaryDirectory(prefix="driver-", dir=directory) as scratch:
            work = Path(scratch)
            inputs = work / "inputs"
            try:
                inputs.mkdir()
                for entry in _manifest(spec):
                    role = entry["role"]
                    verified[role] = _stage(entry, self.cache, inputs, self.stop)
                driver_input = work / "input.json"
                driver_input.write_text(json.dumps({
                    "repetitionId": attempt.repetition_id, "attemptId": attempt.attempt_id,
                    "specification": spec,
                    "artifactPaths": {role: str(inputs / role) for role in verified},
                }), encoding="utf-8")
                _check_stop(self.stop)
            except (Cancelled, DriverFailed, OSError) as error:
                reason = _reason(error)
                self._require_status(attempt, Status.SETUP)
                self._terminal_update(attempt, Status.FAILED, reason=reason,
                                      environment=json.dumps(environment), artifacts=json.dumps(verified))
                return ("cancelled" if isinstance(error, Cancelled) else "failed"), reason

            # Only a start acknowledgement authorizes this invocation. A status read
            # cannot prove which process received the acknowledgement.
            self.client.update(attempt, {
                "status": int(Status.RUNNING), "environment": json.dumps(environment),
                "artifacts": json.dumps(verified),
            })
            return self._run_started(attempt, directory, work, driver_input, inputs / "driver")

    def _run_started(self, attempt: Attempt, directory: Path, work: Path,
                     driver_input: Path, driver: Path) -> tuple[str, str]:
        stdout_path, stderr_path = directory / "stdout.json", directory / "stderr.log"
        archive = directory / "result.zip"
        outcome, reason, result, log = self._collect_outputs(
            attempt, work, driver_input, driver, stdout_path, stderr_path, archive,
        )
        self._require_status(attempt, Status.RUNNING)
        try:
            self.client.upload(attempt, archive)
            for offset in range(0, len(log), 32_768):
                self.client.log(attempt, log[offset:offset + 32_768])
        except (AgentError, OSError) as error:
            self._require_status(attempt, Status.RUNNING)
            reason = _reason(error) if not reason else f"{reason} Evidence delivery failed: {_reason(error)}"
            self._fail(attempt, reason, result)
            return "error", reason

        if outcome == "finished" and self.stop.is_set():
            outcome, reason = "cancelled", "The local agent was interrupted."
        if outcome != "finished":
            self._fail(attempt, reason, result)
            return outcome, reason
        try:
            self._terminal_update(attempt, Status.FINISHED, result=result)
        except ApiError as error:
            self._require_status(attempt, Status.RUNNING)
            reason = _reason(error)
            self._fail(attempt, reason, result)
            return "failed", reason
        return "finished", ""

    def _collect_outputs(self, attempt: Attempt, work: Path, driver_input: Path, driver: Path,
                         stdout_path: Path, stderr_path: Path, archive: Path) -> tuple[str, str, str | None, str]:
        outcome, reason, result = "finished", "", None
        with (open_directory(work) as work_fd, stdout_path.open("x+b") as stdout,
              stderr_path.open("x+b") as stderr):
            try:
                self._run_driver(attempt, work, driver_input, driver, stdout, stderr)
            except (Cancelled, DriverFailed, JobTimeout, OSError) as error:
                if isinstance(error, Cancelled):
                    outcome = "cancelled"
                elif isinstance(error, JobTimeout):
                    outcome = "timeout"
                else:
                    outcome = "failed"
                reason = _reason(error)
            finally:
                with ZipFile(archive, "x", compression=ZIP_DEFLATED) as bundle:
                    bundle.writestr(stdout_path.name, _read_evidence(stdout))
                    log = _read_evidence(stderr)
                    bundle.writestr(stderr_path.name, log)
                    evidence_error = collect_driver_evidence(bundle, work_fd)
                    if evidence_error:
                        bundle.writestr("evidence-error.txt", evidence_error + "\n")
            try:
                result = _result(stdout)
            except (DriverFailed, OSError) as error:
                if outcome == "finished":
                    outcome, reason = "failed", _reason(error)
        if evidence_error:
            reason = _reason(DriverFailed(f"{reason} {evidence_error}".strip()))
            if outcome == "finished":
                outcome = "failed"
        return outcome, reason, result, log.decode("utf-8", errors="replace")

    def _run_driver(self, attempt: Attempt, work: Path, driver_input: Path, driver: Path,
                    output: BinaryIO, log: BinaryIO) -> None:
        _check_stop(self.stop)
        deadline = time.monotonic() + self.timeout
        next_probe = time.monotonic() + self.status_interval
        with driver_input.open("rb") as source:
            process = subprocess.Popen(
                [str(driver)], cwd=work, env={"PATH": os.defpath}, stdin=source,
                stdout=output, stderr=log, start_new_session=True,
            )
            try:
                while process.poll() is None:
                    _check_stop(self.stop)
                    now = time.monotonic()
                    if now >= deadline:
                        raise JobTimeout(f"Driver exceeded the {self.timeout:g} second deadline.")
                    self._check_output(output, log)
                    if now >= next_probe:
                        self._require_status(attempt, Status.RUNNING, probe_timeout=min(1, deadline - now))
                        next_probe = time.monotonic() + self.status_interval
                    try:
                        process.wait(timeout=min(0.05, deadline - now))
                    except subprocess.TimeoutExpired:
                        continue
            finally:
                _stop_group(process)
            self._check_output(output, log)
        if process.returncode:
            raise DriverFailed(f"Driver exited with status {process.returncode}.")

    @staticmethod
    def _check_output(output, log) -> None:
        if any(os.fstat(stream.fileno()).st_size > MAX_OUTPUT for stream in (output, log)):
            raise DriverFailed("Driver stdout or stderr exceeded the 1 MiB evidence limit.")

    def _require_status(self, attempt: Attempt, expected: Status, *, probe_timeout=None) -> None:
        status = self.client.status(attempt, probe_timeout=probe_timeout)
        if status != expected:
            raise AttemptStopped(f"Chronos reports the attempt as {status.name.lower()}.")

    def _fail(self, attempt: Attempt, reason: str, result: str | None) -> None:
        try:
            self._terminal_update(attempt, Status.FAILED, reason=reason[:3000], result=result)
        except ApiError:
            if result is None:
                raise
            self._require_status(attempt, Status.RUNNING)
            # The raw stdout remains in the ZIP if Chronos rejects it as a result.
            self._terminal_update(attempt, Status.FAILED, reason=reason[:3000])

    def _terminal_update(self, attempt: Attempt, status: Status, **fields: str | None) -> None:
        body = {key: value for key, value in fields.items() if value is not None}
        try:
            self.client.update(attempt, {**body, "status": int(status)})
        except UncertainWrite as error:
            try:
                actual = self.client.status(attempt)
            except AgentError as read_error:
                raise error from read_error
            if actual == status:
                return
            if actual not in (Status.SETUP, Status.RUNNING):
                raise AttemptStopped(f"Chronos reports the attempt as {actual.name.lower()}.") from error
            raise
