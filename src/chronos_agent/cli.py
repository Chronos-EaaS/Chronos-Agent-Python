"""Shell entry point for a single Chronos job or a polling worker."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import platform
import signal
import sys
import time

from .agent import Agent, Hooks, RunReport
from .attempt import AttemptAgent, AttemptReport
from .attempt_protocol import AttemptClient, PROTOCOL, SHA256_PATTERN
from .command import Command
from .protocol import AgentError, CemNode, Client


def _command_json(value: str) -> list[str]:
    try:
        command = json.loads(value)
        if not isinstance(command, list):
            raise ValueError("expected a JSON argument array")
        Command(command)
        return command
    except (ValueError, RecursionError) as error:
        raise argparse.ArgumentTypeError(f"Invalid command: {error}") from error


def _positive(value: str) -> float:
    try:
        number = float(value)
    except ValueError:
        number = 0
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("Expected a positive, finite number.")
    return number


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chronos-agent", allow_abbrev=False,
        description="Run a local command for jobs from the Chronos v1 agent API.",
        usage="%(prog)s [options] -- EXECUTABLE [ARG ...]\n"
              "       %(prog)s --protocol attempt-v1 --artifact-cache DIR [options]",
    )
    parser.add_argument("--url", default=os.getenv("CHRONOS_URL"), help="Chronos Control base URL.")
    parser.add_argument("--protocol", choices=("legacy", PROTOCOL), default="legacy",
                        help="Opt in to digest-verified chronos-command@1 attempts.")
    parser.add_argument("--artifact-cache", type=Path, help="Digest-named artifact cache for attempt-v1.")
    parser.add_argument("--worker", help="Stable worker ID for attempt-v1 (defaults to the hostname).")
    parser.add_argument("--attempt-job", type=int, metavar="ID",
                        help="Run a CEM-claimed repetition once using CHRONOS_ATTEMPT_TOKEN.")
    parser.add_argument("--environment", default="default", help="Environment name without its queue prefix.")
    parser.add_argument("--system", action="append", default=[], help="Supported system identifier. Repeat as needed.")
    parser.add_argument("--cem-node", help="Use the CEM queue with this stable node ID.")
    parser.add_argument("--cem-version", type=int, help="CEM protocol version configured by the operator.")
    parser.add_argument("--front-controller", action="store_true", help="Use the legacy index.php URL alias.")
    parser.add_argument("--upload-origin", help="Allow this additional HTTP(S) upload origin.")
    parser.add_argument("--ftp-host", help="Allow legacy cleartext FTP to this configured host.")
    parser.add_argument("--once", action="store_true", help="Poll and run at most one job.")
    parser.add_argument("--poll-interval", type=_positive, default=5, help="Seconds between empty queue polls.")
    parser.add_argument("--status-interval", type=_positive, default=1, help="Seconds between job status probes.")
    parser.add_argument("--timeout", type=_positive, default=3600, help="Workload deadline in seconds.")
    parser.add_argument("--cleanup-timeout", type=_positive, default=30, help="Separate teardown deadline in seconds.")
    parser.add_argument("--http-timeout", type=_positive, default=10, help="Socket timeout in seconds.")
    parser.add_argument("--read-retries", type=int, default=2,
                        help="Retries for reads and replay-safe attempt requests (0–5).")
    parser.add_argument("--work-dir", type=Path, default=Path(".chronos-agent"), help="Directory for local evidence.")
    parser.add_argument("--cwd", type=Path, help="Override the command working directory for all phases.")
    for name in ("setup", "warmup", "validate", "teardown"):
        parser.add_argument(f"--{name}-json", type=_command_json, metavar="ARGV",
                            help=f"Optional {name} command as a JSON array of arguments.")
    return parser


def _emit(report: RunReport | None) -> None:
    if report is None:
        print(json.dumps({"outcome": "empty"}), flush=True)
        return
    data = {"outcome": report.outcome, "jobId": report.job_id,
            "directory": str(report.directory), "message": report.message}
    if isinstance(report, AttemptReport):
        data["attemptId"] = report.attempt_id
    print(json.dumps(data), flush=True)
    if report.message:
        print(f"Chronos job {report.job_id}: {report.message}", file=sys.stderr, flush=True)


class _SignalStop:
    exit_code = 0

    def __call__(self, signum, frame):
        self.exit_code = 128 + signum

    def is_set(self) -> bool:
        return self.exit_code != 0

    def wait(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.is_set() and (remaining := deadline - time.monotonic()) > 0:
            time.sleep(min(0.1, remaining))


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    parser = _parser()
    delimiter = args.index("--") if "--" in args else len(args)
    options = parser.parse_args(args[:delimiter])
    command = args[delimiter + 1:]
    if not options.url:
        parser.error("--url or CHRONOS_URL is required.")
    if (options.cem_node is None) != (options.cem_version is None):
        parser.error("--cem-node and --cem-version must be supplied together.")
    stop = _SignalStop()
    try:
        if options.protocol == PROTOCOL:
            agent = _attempt_agent(options, delimiter < len(args), stop)
        else:
            agent = _legacy_agent(options, command, stop)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    signals = (signal.SIGINT, signal.SIGTERM)
    handlers = {sig: signal.signal(sig, stop) for sig in signals}
    try:
        return _worker(agent, options, stop)
    finally:
        for sig, previous in handlers.items():
            signal.signal(sig, previous)


def _attempt_agent(options, has_delimiter: bool, stop: _SignalStop) -> AttemptAgent:
    if options.artifact_cache is None:
        raise ValueError("--protocol attempt-v1 requires --artifact-cache.")
    if (has_delimiter or options.cwd is not None or options.cem_node is not None
            or options.upload_origin is not None or options.ftp_host is not None
            or any((options.setup_json, options.warmup_json, options.validate_json, options.teardown_json))):
        raise ValueError("attempt-v1 uses the frozen driver, staged artifacts and the Chronos upload endpoint.")
    handoff = None
    if options.attempt_job is not None:
        token = os.getenv("CHRONOS_ATTEMPT_TOKEN", "")
        if not options.once or not 1 <= options.attempt_job <= 2_147_483_647:
            raise ValueError("--attempt-job requires a positive repetition ID and --once.")
        if not SHA256_PATTERN.fullmatch(token):
            raise ValueError("--attempt-job requires the CEM claim credential in CHRONOS_ATTEMPT_TOKEN.")
        handoff = options.attempt_job, token
    client = AttemptClient(options.url, timeout=options.http_timeout, read_retries=options.read_retries,
                           front_controller=options.front_controller)
    return AttemptAgent(client, worker=options.worker or platform.node() or "python-worker",
                        systems=tuple(options.system), environment=options.environment,
                        cache=options.artifact_cache, directory=options.work_dir, stop=stop,
                        timeout=options.timeout, status_interval=options.status_interval, handoff=handoff)


def _legacy_agent(options, command: list[str], stop: _SignalStop) -> Agent:
    if any(option is not None for option in (options.artifact_cache, options.worker, options.attempt_job)):
        raise ValueError("--artifact-cache, --worker and --attempt-job require --protocol attempt-v1.")
    if not command:
        raise ValueError("A command after -- is required.")
    cwd = options.cwd.resolve(strict=True) if options.cwd else None
    if cwd is not None and not cwd.is_dir():
        raise ValueError("--cwd must be a directory.")
    cem = (CemNode(options.cem_node, options.cem_version, options.environment)
           if options.cem_node is not None else None)
    client = Client(options.url, timeout=options.http_timeout, read_retries=options.read_retries,
                    front_controller=options.front_controller, upload_origin=options.upload_origin,
                    ftp_host=options.ftp_host)
    hook_commands = {
        name: Command(values, cwd=cwd) if values is not None else None
        for name, values in (
            ("setup", options.setup_json), ("warmup", options.warmup_json),
            ("validate", options.validate_json), ("teardown", options.teardown_json),
        )
    }
    return Agent(client, Hooks(run=Command(command, cwd=cwd), **hook_commands),
                 directory=options.work_dir, systems=tuple(options.system),
                 environment=options.environment, cem=cem, stop=stop, timeout=options.timeout,
                 cleanup_timeout=options.cleanup_timeout, status_interval=options.status_interval)


def _worker(agent: Agent | AttemptAgent, options, stop: _SignalStop) -> int:
    exit_codes = {"finished": 0, "failed": 1, "error": 5, "timeout": 124,
                  "cancelled": 130, "stopped": 1}
    while not stop.is_set():
        try:
            report = agent.run_once()
        except ValueError as error:
            print(f"Chronos configuration: {error}", file=sys.stderr)
            return 2
        except (AgentError, OSError) as error:
            print(f"Chronos agent: {error}", file=sys.stderr)
            return 5
        if report is not None or (options.once and not stop.is_set()):
            _emit(report)
        if report and report.outcome == "error":
            return 5
        if stop.is_set():
            break
        if options.once:
            return exit_codes[report.outcome] if report else 3
        if report is None:
            stop.wait(options.poll_interval)
    return stop.exit_code
