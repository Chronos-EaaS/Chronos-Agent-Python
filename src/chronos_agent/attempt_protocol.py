"""Credentialed, attempt-scoped operations on the Chronos v1 API."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import secrets
from typing import Any

from .protocol import (
    ApiError, Client, ProtocolError, Status, _archive_parts, _job_id, _object, _segments, _status,
)


PROTOCOL = "attempt-v1"
ADAPTER = "chronos-command@1"
MAX_OUTPUT = 1024 * 1024
MAX_ARCHIVE = 2 * MAX_OUTPUT + 65_536
SHA256_PATTERN = re.compile(r"[a-f0-9]{64}")


@dataclass(frozen=True)
class Attempt:
    repetition_id: int
    attempt_id: int
    status: Status
    specification_json: str = field(repr=False)
    token: str = field(repr=False)


class AttemptClient(Client):
    """Replay a claim with the same credential and an upload with the same bytes."""

    # The response also includes the legacy CDL field, which is part of the frozen plan.
    _response_limit = 4 * MAX_OUTPUT

    def claim(self, worker: str, systems: tuple[str, ...], environment: str) -> Attempt | None:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}", worker):
            raise ValueError("Pass a stable --worker ID using letters, digits, underscores, dots, colons or dashes.")
        if not environment.strip() or environment.strip() == "0":
            raise ValueError("Pass an environment name accepted by the v1 API.")
        if len(systems) > 100 or any(not system.strip() or "," in system for system in systems):
            raise ValueError("Pass at most 100 nonempty system identifiers without commas.")
        token = secrets.token_hex(32)
        route = {"environment": environment, "adapter": ADAPTER, "worker": worker}
        if systems:
            route["supports"] = ",".join(systems)
        try:
            value = self._attempt_request("claim attempt", "GET", "next", token, route=route,
                                          retry=True, mutates=True)
        except ApiError as error:
            if error.code == 601:
                return None
            raise
        return self._decode_claim(value, token)

    def attach(self, repetition_id: int, token: str) -> Attempt:
        if not SHA256_PATTERN.fullmatch(token):
            raise ValueError("Set CHRONOS_ATTEMPT_TOKEN to the hexadecimal CEM claim credential.")
        value = self._attempt_request("read existing attempt", "GET", _job_id(repetition_id),
                                      token, retry=True)
        attempt = self._decode_claim(value, token)
        if attempt.repetition_id != repetition_id:
            raise ProtocolError("Chronos returned a different repetition.")
        return attempt

    def status(self, attempt: Attempt, *, probe_timeout: float | None = None) -> Status:
        value = self._attempt_request(
            f"read attempt {attempt.attempt_id}", "GET", attempt.repetition_id, attempt.token,
            retry=probe_timeout is None, probe_timeout=probe_timeout,
        )
        data = _object(value, "Attempt status")
        if self._identity(data) != (attempt.repetition_id, attempt.attempt_id):
            raise ProtocolError("Chronos returned a different attempt for this credential.")
        return _status(data.get("status"))

    def update(self, attempt: Attempt, fields: dict[str, str | int]) -> None:
        self._attempt_request(f"update attempt {attempt.attempt_id} ({', '.join(fields)})",
                              "PATCH", attempt.repetition_id, attempt.token,
                              fields=fields, mutates=True)

    def log(self, attempt: Attempt, text: str) -> None:
        self._attempt_request(f"append log for attempt {attempt.attempt_id}", "POST",
                              attempt.repetition_id, attempt.token, route={"action": "appendLog"},
                              fields={"log": text}, mutates=True)

    def upload(self, attempt: Attempt, archive: Path) -> None:
        # A bounded immutable body lets the shared transport resend the same ZIP.
        with archive.open("rb") as source:
            data = source.read(MAX_ARCHIVE + 1)
        if len(data) > MAX_ARCHIVE:
            raise ProtocolError("Attempt archive exceeds the local evidence limit.")
        headers, prefix, suffix = _archive_parts(len(data))
        self._attempt_request(
            f"upload archive for attempt {attempt.attempt_id}", "POST", attempt.repetition_id,
            attempt.token, route={"action": "upload"}, headers=headers, body=prefix + data + suffix,
            retry=True, mutates=True,
        )

    def _attempt_request(
        self, operation: str, method: str, repetition_id: int | str, token: str, *,
        route: dict[str, str] | None = None, headers: dict[str, str] | None = None, **options,
    ):
        path = _segments({"id": repetition_id, "protocol": PROTOCOL, **(route or {})})
        return self._request(operation, method, self.prefix + "job" + path,
                             headers={**(headers or {}), "X-Chronos-Attempt-Token": token}, **options)

    @staticmethod
    def _identity(value: dict[str, Any]) -> tuple[int, int]:
        if value.get("protocol") != PROTOCOL:
            raise ProtocolError("Chronos did not return an attempt-v1 response.")
        repetition_id = _job_id(value.get("repetitionId"))
        if _job_id(value.get("id")) != repetition_id:
            raise ProtocolError("Chronos returned conflicting repetition IDs.")
        return repetition_id, _job_id(value.get("attemptId"))

    @classmethod
    def _decode_claim(cls, value: Any, token: str) -> Attempt:
        data = _object(value, "Attempt response")
        repetition_id, attempt_id = cls._identity(data)
        spec = _object(data.get("specification"), "Frozen specification")
        digest = spec.get("sha256")
        phases = spec.get("phases")
        if (type(spec.get("schemaVersion")) is not int or spec["schemaVersion"] != 1
                or spec.get("protocol") != PROTOCOL or spec.get("adapter") != ADAPTER
                or not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest)
                or type(phases) is not int or not 0 <= phases <= 31):
            raise ProtocolError("Unsupported frozen specification for chronos-command@1.")
        try:
            frozen = json.dumps(spec, allow_nan=False)
        except (ValueError, RecursionError) as error:
            raise ProtocolError("Invalid frozen specification JSON.") from error
        return Attempt(repetition_id, attempt_id, _status(data.get("status")), frozen, token)
