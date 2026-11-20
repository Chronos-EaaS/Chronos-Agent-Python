#!/usr/bin/env python3
"""Run one synthetic job with Python lifecycle hooks."""

import sys
from pathlib import Path

from chronos_agent import Agent, Client, Context, Hooks


def setup(context: Context) -> None:
    context.log("Setting up synthetic workload.")


def run(context: Context) -> None:
    context.check()
    context.progress(50)
    (context.artifacts / "synthetic.txt").write_text("Synthetic example output\n", encoding="utf-8")
    context.result = {"label": context.cdl.evaluation.get("label", "")}


def validate(context: Context) -> None:
    if (context.artifacts / "synthetic.txt").read_text(encoding="utf-8") != "Synthetic example output\n":
        raise ValueError("Unexpected synthetic output.")


def teardown(context: Context) -> None:
    context.log("Synthetic teardown completed.")


def main(url: str) -> int:
    agent = Agent(Client(url), Hooks(run=run, setup=setup, validate=validate, teardown=teardown),
                  systems=("fixture-system",), environment="agent", directory=Path(".chronos-agent"))
    report = agent.run_once()
    print(report if report else "No job in queue.")
    return 0 if report and report.outcome == "finished" else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: library.py CHRONOS_URL")
    raise SystemExit(main(sys.argv[1]))
