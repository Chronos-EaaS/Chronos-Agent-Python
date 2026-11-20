# Chronos Agent for Python

[![Checks](https://github.com/Chronos-EaaS/Chronos-Agent-Python/actions/workflows/checks.yml/badge.svg?branch=main)](https://github.com/Chronos-EaaS/Chronos-Agent-Python/actions/workflows/checks.yml)

Run local benchmark adapters against the Chronos Control v1 job API. The Python SDK, `chronos-agent` command and Bash wrapper use the same lifecycle. The command runner targets Linux and macOS with Python 3.11 or later. The package has no runtime dependencies.

## Install and run

Clone the repository and install the package in a virtual environment.

```sh
git clone https://github.com/Chronos-EaaS/Chronos-Agent-Python.git
cd Chronos-Agent-Python
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

For a synthetic end-to-end run, start the in-memory fixture from the repository root in one terminal. It listens on a randomly selected loopback port and starts with one scheduled job.

```sh
python3 tests/mock_server.py
```

Set `CHRONOS_URL` to the printed URL in a second terminal, then run the worker using the activated environment. The example copies its arguments and a CDL parameter into a JSON result and uploads a small text artifact.

```sh
chronos-agent --url "$CHRONOS_URL" --system fixture-system --environment agent --once \
  -- "$(command -v python)" "$PWD/examples/workload.py" 'with spaces' '' '--literal'
```

Restart the fixture to reset its in-memory job before repeating the example via the Bash wrapper or Python SDK.

```sh
PYTHON="$(command -v python)" ./examples/agent.sh \
  --url "$CHRONOS_URL" --system fixture-system --environment agent --once \
  -- "$(command -v python)" "$PWD/examples/workload.py" 'A&B + Zürich'

python examples/library.py "$CHRONOS_URL"
```

The worker runs continuously by default and waits `--poll-interval` seconds when the queue is empty. Use repeated `--system` options for systems you explicitly support. For CEM, use `--cem-node NODE_ID --cem-version VERSION --environment NAME`. CEM selects systems configured on the server for that environment. Provision a stable node ID and a version accepted by the server. `--front-controller` enables the `/index.php?/api/v1/` route alias.

## Workload hooks

Configure commands on the worker. The server supplies CDL data and a phase exclusion mask. The agent executes the configured hooks in this order.

| Hook | Chronos phase | CLI |
| --- | --- | --- |
| Setup | 1 PREPARE | `--setup-json` |
| Warm-up | 2 WARMUP | `--warmup-json` |
| Run | 3 EXECUTE | Arguments after `--` |
| Validation | 4 ANALYZE | `--validate-json` |
| Teardown | 5 CLEAN | `--teardown-json` |

The optional hook flags accept JSON argument arrays such as `--validate-json '["/opt/bench/validate", "--strict"]'`. Use an absolute executable path or a command on `PATH`. Each command runs in its job directory unless you set `--cwd`. Commands inherit the worker environment and receive these additional variables.

| Variable | Value |
| --- | --- |
| `CHRONOS_JOB_ID` | Legacy job ID |
| `CHRONOS_SYSTEM`, `CHRONOS_ENVIRONMENT` | System identifier and full server environment |
| `CHRONOS_PHASE` | Active phase, in lowercase |
| `CHRONOS_WORK_DIR` | Private per-execution evidence directory |
| `CHRONOS_CDL` | Absolute path to the raw CDL |
| `CHRONOS_RESULT_JSON` | Path for an optional UTF-8 JSON object up to 1 MiB |
| `CHRONOS_ARTIFACTS` | Directory for files to include in the ZIP |

A nonzero command exit fails the job. The agent captures combined stdout and stderr locally and sends up to 1,048,576 characters per phase to Chronos at phase boundaries. Keep secrets out of workload logs, results and artifacts. A configured and enabled teardown runs after attempted execution, including local failure or cancellation, with a separate deadline. It sends progress at the phase boundary. Hooks omitted by the operator or excluded by the job mask are skipped. Provisioning logic lives in the setup hook.

Python hooks receive a `Context` with parsed `cdl.setup` and `cdl.evaluation` mappings, paths and the current phase. Use `ctx.progress(0..100)` to report progress and `ctx.log(text)` to publish diagnostic text at the boundary. Use either `ctx.result` or `ctx.result_path` for results. Call `ctx.check()` regularly inside long-running Python hooks to observe deadline and cancellation. Pass a `threading.Event` to `Agent(stop=...)` to request cancellation from another thread, or use the SDK's `Command` hook for process-level termination.

The CLI handles SIGINT and SIGTERM and terminates the command's POSIX process group (TERM, then KILL after a short grace period). Child processes should stay in that group. Status probes detect server-side aborts and other state changes. `--timeout` bounds cooperative workload execution, `--cleanup-timeout` bounds teardown and `--http-timeout` controls the HTTP socket timeout. In-flight network I/O and child termination may delay return.

## Results and exit codes

On success, the agent uploads a ZIP containing `execution.json` and the regular files from `artifacts/`, then sends the result object and finished status in one PATCH. Use `--upload-origin https://uploads.example` if the server advertises an upload origin different from the configured Control origin. Legacy cleartext FTP requires `--ftp-host HOST` with the exact host configured by the server.

The local directory also retains the raw CDL, phase logs, result file when supplied, and `outcome.json`. Execution metadata records the CDL SHA-256, local execution UUID, platform, UTC timestamps, monotonic durations and observed command exit statuses. The execution UUID identifies a local run. Reported durations include agent work around the hooks and serve as execution diagnostics.

Stdout emits a JSON line for each completed job and `{"outcome": "empty"}` for an empty `--once` poll. Diagnostics go to stderr. In continuous mode, per-job failures are reported and polling continues. An API or transport failure stops the worker.

| Exit | Meaning |
| --- | --- |
| 0 | Job finished |
| 1 | Workload failed or the server stopped it |
| 2 | Invalid CLI configuration |
| 3 | Empty queue with `--once` |
| 5 | API, transport or local I/O error requiring investigation |
| 124 | Workload or cleanup deadline exceeded |
| 130 | Job cancelled or SIGINT |
| 143 | SIGTERM |

## v1 compatibility and integration

Use one consumer per system queue partition. System polling is a read followed by a status PATCH, with no atomic claim. CEM reserves a candidate for 60 seconds and uses `jobStarted` and `jobTerminated` to track the node. Both paths still use mutable job-scoped state and evidence. A state probe narrows the race with operator changes.

Retries are bounded and limited to read-only requests. CEM selection can write a reservation and is sent once. Each log write, status change and upload is sent once. An unclear acknowledgement stops processing, keeps local evidence, and requires inspection of the existing job, upload and CEM node state before restarting. An upload can already be present while the job is still running. The v1 API has no idempotency key, attempt identity or atomic result/archive commit.

`Client` owns wire encoding and compatibility. Integrate a future core attempt protocol at the client and lifecycle boundaries after the server advertises the respective capability. Retain the present v1 path for legacy installations and require server-issued attempt IDs and documented replay semantics before enabling attempt-scoped writes.

The v1 contract is defined by `backend/src/Http/Api/V1`, `JobCdlAssembler` and `tests/api/agent-contracts.py` in Chronos Control. [Chronos Control source](https://github.com/Chronos-EaaS/Chronos-Control)

The public Java agent provides a reference for phase masks and upload target handling. [Chronos Agent reference](https://github.com/Chronos-EaaS/Chronos-Agent)

## Local verification

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The suite uses synthetic commands and ephemeral loopback HTTP servers. FTP client calls are exercised through a local test double. No database or existing Chronos instance is needed.

The Checks workflow runs package installation, CLI and Bash checks, and the test suite on Linux and macOS with Python 3.11 and 3.15.

## License

[MIT](LICENSE), with the same copyright notice as Chronos Control.
