"""POSIX command hooks shared by library clients, the CLI and Bash."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import os
from pathlib import Path
import signal
import subprocess
import time

from .agent import Context, _utc_now


class CommandFailed(Exception):
    """A configured command could not start or exited unsuccessfully."""


@dataclass(frozen=True)
class Command:
    argv: tuple[str, ...]
    cwd: Path | None = None

    def __init__(self, argv: Sequence[str], *, cwd: Path | None = None):
        if (isinstance(argv, str) or not argv or not argv[0]
                or any(not isinstance(arg, str) or "\0" in arg for arg in argv)):
            raise ValueError("Command requires an executable and a sequence of string arguments.")
        object.__setattr__(self, "argv", tuple(argv))
        object.__setattr__(self, "cwd", cwd)

    def __call__(self, context: Context) -> None:
        if os.name != "posix":
            raise CommandFailed("Command hooks require POSIX process groups.")
        env = {
            **os.environ,
            "CHRONOS_JOB_ID": str(context.job.id),
            "CHRONOS_SYSTEM": context.job.system,
            "CHRONOS_ENVIRONMENT": context.job.environment,
            "CHRONOS_PHASE": context.phase.name.lower(),
            "CHRONOS_CDL": str(context.cdl_path),
            "CHRONOS_WORK_DIR": str(context.directory),
            "CHRONOS_ARTIFACTS": str(context.artifacts),
            "CHRONOS_RESULT_JSON": str(context.result_path),
        }
        context.check()
        started = time.monotonic()
        observed = {"phase": context.phase.name.lower(), "startedAt": _utc_now()}
        process = None
        try:
            with context.log_path.open("ab") as output:
                try:
                    process = subprocess.Popen(
                        self.argv, cwd=self.cwd or context.directory, env=env,
                        stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                except OSError as error:
                    raise CommandFailed(f"Unable to start {context.phase.name.lower()} command: {error}") from error
                try:
                    while process.poll() is None:
                        context.check()
                        try:
                            process.wait(timeout=0.05)
                        except subprocess.TimeoutExpired:
                            continue
                finally:
                    _stop_group(process)
            if process.returncode:
                raise CommandFailed(f"{context.phase.name.lower()} command exited with code {process.returncode}.")
        finally:
            observed.update(finishedAt=_utc_now(), durationSeconds=time.monotonic() - started,
                            returnCode=process.returncode if process else None)
            context.commands.append(observed)


def _signal_group(pid: int, sig: int) -> bool:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        return False
    return True


def _stop_group(process: subprocess.Popen) -> None:
    try:
        if not _signal_group(process.pid, signal.SIGTERM):
            return
        # Descendants can still be running after the direct child has exited.
        deadline = time.monotonic() + 0.3
        while time.monotonic() < deadline:
            process.poll()
            try:
                if not _signal_group(process.pid, 0):
                    return
            except PermissionError:
                # Darwin can report EPERM while the group leader is exiting.
                pass
            time.sleep(0.02)
        _signal_group(process.pid, signal.SIGKILL)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
