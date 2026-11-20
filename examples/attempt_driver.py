#!/usr/bin/env python3
"""Synthetic command driver for the attempt-v1 example."""

import json
import sys


def main():
    request = json.load(sys.stdin)
    threads = request["specification"]["effectiveParameters"]["threads"]
    print(f"Repetition {request['repetitionId']}, attempt {request['attemptId']}", file=sys.stderr)
    print(json.dumps({"workers": threads}))


if __name__ == "__main__":
    main()
