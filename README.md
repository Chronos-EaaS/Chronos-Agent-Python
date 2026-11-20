# Chronos Agent for Python

[![Checks](https://github.com/Chronos-EaaS/Chronos-Agent-Python/actions/workflows/checks.yml/badge.svg?branch=main)](https://github.com/Chronos-EaaS/Chronos-Agent-Python/actions/workflows/checks.yml)

Connect a benchmark or evaluation script to Chronos Control. The agent picks up jobs, passes their configuration to your workload, and sends progress, logs, results, and artifacts back to Chronos. You can run an existing command from the CLI or add Chronos support to a Python application with the SDK. [Chronos Control](https://github.com/Chronos-EaaS/Chronos-Control) · [Java agent](https://github.com/Chronos-EaaS/Chronos-Agent-Java)

The package requires Python 3.11 or later and has no runtime dependencies. The command runner supports Linux and macOS.

## Getting started

Clone the repository and install the package in a virtual environment.

```sh
git clone https://github.com/Chronos-EaaS/Chronos-Agent-Python.git
cd Chronos-Agent-Python
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

### Run an existing command

With a Chronos Control instance running, schedule a job for your system and environment. Give the agent the system identifier and the command to run. Everything after `--` goes to your command as arguments.

```sh
chronos-agent --url https://chronos.example.org --system my-system --environment default \
  -- /path/to/benchmark --threads 4
```

The agent keeps polling for jobs. Add `--once` to poll once and run at most one job. Repeat `--system` to support several systems. Run `chronos-agent --help` for the full list of options.

### Try it locally

Start the mock server from the repository root. It runs on loopback and starts with one synthetic job.

```sh
python tests/mock_server.py
```

In a second terminal, activate the same virtual environment and set `CHRONOS_URL` to the address printed by the server. Run this from the repository root to see a job produce a JSON result and a text artifact.

```sh
chronos-agent --url "$CHRONOS_URL" --system fixture-system --environment agent --once \
  -- "$(command -v python)" "$PWD/examples/workload.py" "hello from Chronos"
```

The command prints a JSON summary with the run directory. You can use `./examples/agent.sh` in place of `chronos-agent` with the same arguments. The wrapper uses `python3` by default and accepts a Python executable through `PYTHON`. Restart the mock server and update `CHRONOS_URL` before trying another example. [Example workload](examples/workload.py) · [Bash wrapper](examples/agent.sh)

### Use the Python SDK

Pass your workload function to `Hooks(run=...)` and call `Agent.run_once()` to pick up a job. Each hook receives a `Context` with the parsed CDL (`ctx.cdl.setup` and `ctx.cdl.evaluation`), the run directory, and methods for logging and reporting progress. Set `ctx.result` to a dictionary or write a JSON object to `ctx.result_path`. Put files you want to upload in `ctx.artifacts`. Long-running Python hooks should call `ctx.check()` periodically to handle cancellation and timeouts. [Complete SDK example](examples/library.py)

With a fresh mock server, try the SDK example with `python examples/library.py "$CHRONOS_URL"`.

## Set up your workload

The run command is the only hook you have to supply. Add setup, warm-up, validation, or teardown with `--setup-json`, `--warmup-json`, `--validate-json`, and `--teardown-json`. These options take JSON arrays of arguments, for example `--validate-json '["/opt/bench/validate", "--strict"]'`. The agent respects the phases enabled for the job in Chronos. If you configure teardown and the job enables it, it also runs after a failed or cancelled workload.

By default, commands start in a fresh directory under `.chronos-agent/`. Use `--cwd` if your workload needs a specific working directory. The agent provides these environment variables to the command.

| Variable | What it contains |
| --- | --- |
| `CHRONOS_JOB_ID` | Job ID |
| `CHRONOS_SYSTEM`, `CHRONOS_ENVIRONMENT` | System identifier and server environment |
| `CHRONOS_PHASE` | Current phase in lowercase |
| `CHRONOS_WORK_DIR` | Absolute path to the run directory |
| `CHRONOS_CDL` | Path to the job configuration in CDL XML |
| `CHRONOS_RESULT_JSON` | Path where you can write a UTF-8 JSON object up to 1 MiB |
| `CHRONOS_ARTIFACTS` | Directory for any files you want included in the results ZIP |

The run directory keeps a copy of the CDL, phase logs, execution metadata, and the job outcome. Chronos receives log output at the end of each phase. A successful run uploads a ZIP with execution metadata and the files you placed in the artifacts directory, then reports the job as finished with the JSON result if you supplied one.

## Running an agent

For a CEM queue, use `--cem-node NODE_ID --cem-version VERSION --environment NAME` in place of the system selection. Use a stable node ID and a version accepted by your Chronos installation. CEM selects the systems configured for that environment.

Run one worker per system and environment combination. The v1 system queue reads a candidate and updates its status in separate requests, so two workers that cover the same queue can pick up the same job. If the server may have accepted a write but the agent lost the reply, the agent stops. Check the job, its uploaded files, and the node assignment if you use CEM before restarting.

The CLI writes a JSON summary for each reported job to stdout. Diagnostics go to stderr. With `--once`, the following exit codes let a script decide what to do next.

| Exit | Meaning |
| --- | --- |
| 0 | Job finished |
| 1 | Workload failed or the server stopped it |
| 2 | Invalid CLI configuration |
| 3 | Queue was empty |
| 5 | API, transport, or local I/O error |
| 124 | Workload or teardown timed out |
| 130 | Job cancelled or interrupted by SIGINT |
| 143 | Agent stopped by SIGTERM |

## Contributing

If you change the agent, run the tests from the repository root. The suite uses a local mock server and synthetic workloads.

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

## License

[MIT](LICENSE).
