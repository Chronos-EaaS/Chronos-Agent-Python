"""Shell entry point for a single Chronos job or a polling worker."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import signal
import sys
import time

from .agent import Agent, Hooks, RunReport
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
        usage="%(prog)s [options] -- EXECUTABLE [ARG ...]",
    )
    parser.add_argument("--url", default=os.getenv("CHRONOS_URL"), help="Chronos Control base URL.")
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
    parser.add_argument("--read-retries", type=int, default=2, help="Retries per read-only API request (0–5).")
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
    print(json.dumps({"outcome": report.outcome, "jobId": report.job_id,
                      "directory": str(report.directory), "message": report.message}), flush=True)
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
    if not options.url or not command:
        parser.error("--url or CHRONOS_URL and a command after -- are required.")
    if (options.cem_node is None) != (options.cem_version is None):
        parser.error("--cem-node and --cem-version must be supplied together.")
    stop = _SignalStop()
    try:
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
        agent = Agent(client, Hooks(run=Command(command, cwd=cwd), **hook_commands),
                      directory=options.work_dir, systems=tuple(options.system),
                      environment=options.environment, cem=cem, stop=stop, timeout=options.timeout,
                      cleanup_timeout=options.cleanup_timeout, status_interval=options.status_interval)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    signals = (signal.SIGINT, signal.SIGTERM)
    handlers = {sig: signal.signal(sig, stop) for sig in signals}
    try:
        return _worker(agent, options, stop)
    finally:
        for sig, previous in handlers.items():
            signal.signal(sig, previous)


def _worker(agent: Agent, options, stop: _SignalStop) -> int:
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
