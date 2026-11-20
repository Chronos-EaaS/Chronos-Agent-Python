#!/usr/bin/env python3
"""A synthetic workload for the CLI and Bash wrapper."""

import json
import os
from pathlib import Path
import sys

from chronos_agent import Cdl


def main() -> None:
    cdl = Cdl.parse(Path(os.environ["CHRONOS_CDL"]).read_text(encoding="utf-8"))
    result = {"label": cdl.evaluation.get("label", ""), "arguments": sys.argv[1:]}
    Path(os.environ["CHRONOS_RESULT_JSON"]).write_text(json.dumps(result) + "\n", encoding="utf-8")
    artifact = Path(os.environ["CHRONOS_ARTIFACTS"]) / "synthetic.txt"
    artifact.write_text("Synthetic example output\n", encoding="utf-8")
    print("Synthetic workload completed.")


if __name__ == "__main__":
    main()
