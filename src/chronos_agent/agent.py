"""Lifecycle for local workloads and the Chronos v1 job queue."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import platform
import stat
from threading import Event, Lock
import time
from typing import Any, Protocol
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

from .cdl import Cdl
from .protocol import AgentError, CemNode, Client, Job, Phase, ProtocolError, Status


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Cancelled(Exception):
    """The agent has been asked to stop the workload."""


class JobTimeout(Exception):
    """The workload exceeded its local execution budget."""


class _StatusChanged(Exception):
    def __init__(self, status: Status):
        self.status = status
        super().__init__(f"Chronos job status is now {status.name.lower()}.")


class StopSignal(Protocol):
    def is_set(self) -> bool: ...


Hook = Callable[["Context"], None]


@dataclass(frozen=True)
class Hooks:
    run: Hook
    setup: Hook | None = None
    warmup: Hook | None = None
    validate: Hook | None = None
    teardown: Hook | None = None


@dataclass
class Context:
    job: Job
    cdl: Cdl
    directory: Path
    _check: Callable[[], None] = field(repr=False)
    _report_progress: Callable[[int], None] = field(repr=False)
    phase: Phase = Phase.PREPARE
    result: dict[str, Any] | None = None
    commands: list[dict[str, Any]] = field(default_factory=list, init=False)

    @property
    def cdl_path(self) -> Path:
        return self.directory / "job.xml"

    @property
    def artifacts(self) -> Path:
        return self.directory / "artifacts"

    @property
    def result_path(self) -> Path:
        return self.directory / "result.json"

    @property
    def log_path(self) -> Path:
        return self.directory / f"{self.phase.name.lower()}.log"

    def check(self) -> None:
        """Check cancellation and deadline. Call periodically from in-process hooks."""
        self._check()

    def progress(self, percent: int) -> None:
        """Send progress for the active phase; cleanup hooks use local output only."""
        if type(percent) is not int or not 0 <= percent <= 100:
            raise ValueError("Percent must be an integer from 0 to 100.")
        self.check()
        self._report_progress(percent)

    def log(self, text: str) -> None:
        """Write local diagnostic text for delivery at the phase boundary."""
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(text + "\n")


@dataclass(frozen=True)
class RunReport:
    job_id: int
    directory: Path
    outcome: str
    message: str = ""


class Agent:
    """Run a single job at a time on a system queue or a CEM node."""

    def __init__(
        self, client: Client, hooks: Hooks, *, directory: Path,
        systems: tuple[str, ...] = (), environment: str = "default",
        cem: CemNode | None = None, stop: StopSignal | None = None,
        timeout: float = 3600, cleanup_timeout: float = 30, status_interval: float = 1,
    ):
        if any(not math.isfinite(value) or value <= 0
               for value in (timeout, cleanup_timeout, status_interval)):
            raise ValueError("Execution timeout, cleanup timeout and status interval must be positive and finite.")
        if cem is not None and systems:
            raise ValueError("CEM uses the environment's server-side system selection.")
        if cem is None and not systems:
            raise ValueError("A system agent requires one or more supported systems.")
        self.client = client
        self.hooks = hooks
        self.directory = Path(directory).resolve()
        self.systems = systems
        self.environment = environment
        self.cem = cem
        self.stop = stop if stop is not None else Event()
        self.timeout = timeout
        self.cleanup_timeout = cleanup_timeout
        self.status_interval = status_interval
        self._running = Lock()
        self._halted = False

    def run_once(self) -> RunReport | None:
        """Execute one scheduled job. Return None for an empty queue or a stop before claim."""
        if not self._running.acquire(blocking=False):
            raise RuntimeError("This agent is already executing a run_once call.")
        try:
            if self._halted:
                raise ProtocolError("Agent halted after an error. Inspect the job state before restarting.")
            report = self._run_once()
            if report is not None and report.outcome == "error":
                self._halted = True
            return report
        except AgentError:
            self._halted = True
            raise
        finally:
            self._running.release()

    def _run_once(self) -> RunReport | None:
        if self.stop.is_set():
            return None
        if self.cem:
            job = self.client.poll_cem(self.cem)
            environment = f"cem-{self.cem.environment.strip()}"
        else:
            job = self.client.poll(self.systems, self.environment)
            environment = f"system-{self.environment.strip()}"
        if job is None or self.stop.is_set():
            return None
        if job.environment != environment:
            raise ProtocolError("Chronos returned a job from another environment.")
        if not self.cem and self.systems != ("demoall",) and job.system not in self.systems:
            raise ProtocolError("Chronos returned a job for an unsupported system.")
        if job.status != Status.SCHEDULED:
            return None
        # The extra read narrows the system poll/PATCH race; v1 offers no compare-and-set.
        current = self.client.get_job(job.id)
        if current.status != Status.SCHEDULED or self.stop.is_set():
            return None
        if (current.system, current.environment, current.phases, current.cdl) != (
            job.system, job.environment, job.phases, job.cdl
        ):
            raise ProtocolError("Job configuration changed between queue selection and execution.")
        return _Execution(self, job).run()


class _Execution:
    def __init__(self, agent: Agent, job: Job):
        self.agent = agent
        self.client = agent.client
        self.job = job
        self.execution_id = uuid4().hex
        self.directory = agent.directory / f"job-{job.id}-{self.execution_id}"
        self.started = _utc_now()
        self.start_time = time.monotonic()
        self.deadline = self.start_time + agent.timeout
        self.next_probe = self.start_time
        self.status = Status.SCHEDULED
        self.cem_started = False
        self.records: list[dict[str, Any]] = []
        self.pending_logs: list[Phase] = []
        self.context: Context | None = None
        self.error: Exception | None = None

    def run(self) -> RunReport:
        self.directory.mkdir(mode=0o700, parents=True)
        try:
            self._claim()
            self._workload()
        except Exception as error:
            self.error = error

        self._teardown()

        if isinstance(self.error, AgentError):
            return self._report("error", str(self.error))
        if isinstance(self.error, _StatusChanged):
            return self._observed_change(self.error)
        if self.status == Status.SCHEDULED:
            outcome = "cancelled" if isinstance(self.error, Cancelled) else "error"
            return self._report(outcome, str(self.error or "Job start interrupted."))

        try:
            return self._finish()
        except _StatusChanged as error:
            return self._observed_change(error)
        except (AgentError, OSError) as error:
            return self._report("error", str(error))

    def _claim(self) -> None:
        self._local_check()
        if self.agent.cem:
            self.client.cem_state(self.agent.cem, self.job.id, started=True)
            self.cem_started = True
        else:
            self.client.update_job(self.job.id, status=Status.SETUP)
        self.status = Status.SETUP

    def _workload(self) -> None:
        cdl_path = self.directory / "job.xml"
        cdl_path.write_bytes(self.job.cdl.encode("utf-8"))
        cdl = Cdl.parse(self.job.cdl)
        self.context = Context(self.job, cdl, self.directory, self._check, self._progress)
        self.context.artifacts.mkdir()
        hooks = self.agent.hooks
        self._phase(Phase.PREPARE, hooks.setup)
        self._check(force=True)
        self.client.update_job(self.job.id, status=Status.RUNNING)
        self.status = Status.RUNNING
        self._phase(Phase.WARMUP, hooks.warmup)
        self._phase(Phase.EXECUTE, hooks.run)
        self._phase(Phase.ANALYZE, hooks.validate)

    def _local_check(self) -> None:
        if self.agent.stop.is_set():
            raise Cancelled("Local cancellation requested.")
        if time.monotonic() >= self.deadline:
            raise JobTimeout("Local execution timeout exceeded.")

    def _check(self, *, force: bool = False) -> None:
        self._local_check()
        now = time.monotonic()
        if force or now >= self.next_probe:
            try:
                self._check_remote(timeout=max(0.001, min(2, self.deadline - now)))
            except AgentError:
                self._local_check()
                raise
            self._local_check()
            self.next_probe = time.monotonic() + self.agent.status_interval

    def _check_remote(self, *, timeout: float = 2) -> None:
        current = self.client.get_job(self.job.id, probe_timeout=timeout)
        if current.status != self.status:
            raise _StatusChanged(current.status)

    def _progress(self, percent: int) -> None:
        self.client.update_job(self.job.id, progress=percent)

    def _phase(self, phase: Phase, hook: Hook | None) -> None:
        if hook is None or phase.excluded(self.job.phases):
            return
        self._check(force=True)
        self.client.update_job(self.job.id, phase=phase)
        self._invoke(phase, hook, self._check)
        self._check(force=True)
        self._flush_logs()
        self._progress(100)

    def _invoke(self, phase: Phase, hook: Hook, check: Callable[[], None]) -> None:
        context = self.context
        assert context is not None
        context.phase = phase
        context._check = check
        start = time.monotonic()
        record = {"phase": phase.name.lower(), "startedAt": _utc_now(), "outcome": "completed"}
        try:
            check()
            hook(context)
            check()
        except Exception as error:
            record["outcome"] = type(error).__name__
            raise
        finally:
            self.pending_logs.append(phase)
            record.update(finishedAt=_utc_now(), durationSeconds=time.monotonic() - start)
            self.records.append(record)

    def _teardown(self) -> None:
        hook = self.agent.hooks.teardown
        if self.context is None or hook is None or Phase.CLEAN.excluded(self.job.phases):
            return
        announce = False
        if not isinstance(self.error, (AgentError, _StatusChanged)):
            try:
                self._check_remote()
                self.client.update_job(self.job.id, phase=Phase.CLEAN)
                announce = True
            except (AgentError, _StatusChanged) as error:
                self.error = error

        # Cleanup has its own deadline and must run even when the server is unavailable.
        cleanup_deadline = time.monotonic() + self.agent.cleanup_timeout

        def check() -> None:
            if time.monotonic() >= cleanup_deadline:
                raise JobTimeout("Local teardown timeout exceeded.")

        self.context._report_progress = lambda percent: None
        try:
            self._invoke(Phase.CLEAN, hook, check)
        except Exception as error:
            self._diagnostic(f"Teardown: {error}")
            if self.error is None:
                self.error = error
        if announce:
            try:
                self._check_remote()
                self._flush_logs()
                if self.records[-1]["outcome"] == "completed":
                    self._progress(100)
            except (AgentError, _StatusChanged, OSError) as error:
                self.error = error

    def _flush_logs(self) -> None:
        pending, self.pending_logs = self.pending_logs, []
        for phase in pending:
            self._send_phase_log(phase)

    def _send_phase_log(self, phase: Phase) -> None:
        path = self.directory / f"{phase.name.lower()}.log"
        if not path.exists():
            return
        # Keep complete raw output locally. Bound characters and requests sent to Control.
        remaining = 1_048_576
        with path.open(encoding="utf-8", errors="replace", newline="") as source:
            while remaining > 0:
                chunk = source.read(min(16_384, remaining))
                if not chunk:
                    return
                remaining -= len(chunk)
                self.client.append_log(self.job.id, f"[{phase.name.lower()}] {chunk}")
            if source.read(1):
                self.client.append_log(self.job.id, "[Agent] Remote log limit reached. Full output retained locally.\n")

    def _read_result(self) -> dict | None:
        context = self.context
        assert context is not None
        if context.result_path.is_symlink():
            raise ValueError("result.json must be a regular file.")
        from_file = context.result_path.exists()
        result = context.result
        if from_file:
            if result is not None:
                raise ValueError("Specify the result through Context.result or result.json.")
            if not stat.S_ISREG(context.result_path.lstat().st_mode):
                raise ValueError("result.json must be a regular file.")
            with context.result_path.open("rb") as source:
                raw = source.read(1_048_577)
            if len(raw) > 1_048_576:
                raise ValueError("result.json exceeds the 1 MiB limit.")
            result = json.loads(raw.decode("utf-8"))
        elif result is None:
            return None
        if not isinstance(result, dict):
            raise ValueError("The workload result must be a JSON object.")
        encoded = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if not from_file:
            if len(encoded) > 1_048_576:
                raise ValueError("The workload result exceeds the 1 MiB limit.")
            with context.result_path.open("xb") as destination:
                destination.write(encoded)
        return result

    def _finish(self) -> RunReport:
        if self.error is None:
            try:
                self._local_check()
                result = self._read_result()
                self._write_evidence("completed")
                archive = self._archive()
                self._check(force=True)
                self.client.upload_archive(self.job.id, archive)
                self._check(force=True)
                self.client.update_job(self.job.id, status=Status.FINISHED, result=result)
                self.status = Status.FINISHED
            except (Cancelled, JobTimeout, ValueError, OSError, RecursionError) as error:
                self.error = error
            else:
                self._release_cem()
                return self._report("finished")

        error = self.error
        outcome = "cancelled" if isinstance(error, Cancelled) else (
            "timeout" if isinstance(error, JobTimeout) else "failed"
        )
        message = str(error) or type(error).__name__
        self._diagnostic(message)
        self._write_evidence(outcome)
        self._check_remote()
        self._flush_logs()
        self.client.append_log(self.job.id, f"[Agent] {message[:4096]}\n")
        self._check_remote()
        status = Status.ABORTED if outcome == "cancelled" else Status.FAILED
        self.client.update_job(self.job.id, status=status)
        self.status = status
        self._release_cem()
        return self._report(outcome, message)

    def _observed_change(self, change: _StatusChanged) -> RunReport:
        self.status = change.status
        if change.status not in (Status.ABORTED, Status.FAILED, Status.FINISHED):
            return self._report("error", str(change))
        try:
            self._release_cem()
        except AgentError as error:
            return self._report("error", str(error))
        return self._report("cancelled" if change.status == Status.ABORTED else "stopped", str(change))

    def _release_cem(self) -> None:
        if self.cem_started:
            self.client.cem_state(self.agent.cem, self.job.id, started=False)

    def _write_evidence(self, workload_outcome: str) -> None:
        evidence = {
            "schemaVersion": 1,
            "localExecutionId": self.execution_id,
            "jobId": self.job.id,
            "system": self.job.system,
            "environment": self.job.environment,
            "phaseExclusionMask": self.job.phases,
            "cdlSha256": sha256(self.job.cdl.encode("utf-8")).hexdigest(),
            "platform": {"os": platform.system(), "release": platform.release(),
                         "architecture": platform.machine(), "python": platform.python_version()},
            "startedAt": self.started,
            "observedAt": _utc_now(),
            "elapsedSeconds": time.monotonic() - self.start_time,
            "workloadOutcome": workload_outcome,
            "phases": self.records,
            "commands": self.context.commands if self.context else [],
        }
        (self.directory / "execution.json").write_text(json.dumps(evidence, indent=2) + "\n",
                                                      encoding="utf-8")

    def _archive(self) -> Path:
        archive = self.directory / "results.zip"
        staging = self.directory / "results.zip.part"
        assert self.context is not None
        root = self.context.artifacts
        if root.is_symlink() or not root.is_dir():
            raise ValueError("The artifacts directory must be a local directory.")
        stage = staging.open("xb")
        try:
            with stage, ZipFile(stage, "w", compression=ZIP_DEFLATED) as output:
                output.write(self.directory / "execution.json", "execution.json")
                for directory, dirs, files in os.walk(root, onerror=_raise):
                    for name in sorted(dirs + files):
                        path = Path(directory) / name
                        mode = path.lstat().st_mode
                        if stat.S_ISREG(mode):
                            output.write(path, path.relative_to(self.directory).as_posix())
                        elif not stat.S_ISDIR(mode):
                            raise ValueError("Artifacts must be regular files or directories.")
                    dirs.sort()
            staging.replace(archive)
        finally:
            staging.unlink(missing_ok=True)
        return archive

    def _diagnostic(self, message: str) -> None:
        with (self.directory / "agent.log").open("a", encoding="utf-8", errors="replace") as stream:
            stream.write(message + "\n")

    def _report(self, outcome: str, message: str = "") -> RunReport:
        if not (self.directory / "execution.json").exists():
            self._write_evidence(outcome)
        (self.directory / "outcome.json").write_text(json.dumps({
            "outcome": outcome,
            "lastKnownStatus": int(self.status),
            "message": message,
        }, indent=2) + "\n", encoding="utf-8")
        return RunReport(self.job.id, self.directory, outcome, message)


def _raise(error: OSError) -> None:
    raise error
