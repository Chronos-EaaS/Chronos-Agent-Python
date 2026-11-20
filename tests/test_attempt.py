from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from zipfile import ZipFile

from chronos_agent.attempt import AttemptAgent
from chronos_agent.attempt_protocol import AttemptClient
from mock_server import AttemptFixture, serving


ROOT = Path(__file__).resolve().parents[1]


class AttemptTests(unittest.TestCase):
    def setUp(self):
        temp = self.enterContext(TemporaryDirectory())
        self.cache = Path(temp) / "cache"
        self.evidence = Path(temp) / "evidence"

    def run_agent(self, fixture, *, timeout=4, stop=None):
        with serving(fixture) as url:
            return self.agent(url, timeout=timeout, stop=stop).run_once()

    def agent(self, url, *, timeout=4, stop=None):
        return AttemptAgent(
            AttemptClient(url, retry_delay=0), worker="python.fixture:1",
            systems=("fixture-system", "other-system"), environment="agent",
            cache=self.cache, directory=self.evidence, stop=stop or Event(),
            timeout=timeout, status_interval=1,
        )

    def test_replays_claim_and_archive_and_reconciles_finished_status(self):
        fixture = AttemptFixture(self.cache, driver=br"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
data = json.load(sys.stdin)
assert data["repetitionId"] == 90 and data["attemptId"] == 501
assert data["specification"]["setup"] == [] and data["specification"]["phases"] == 2
assert data["specification"]["artifacts"]["buildId"] == "fixture-build"
assert "CHRONOS_ATTEMPT_TOKEN" not in os.environ
assert Path(data["artifactPaths"]["input"]).read_bytes() == b"fixture data"
assert Path(data["artifactPaths"]["driver"]).parent == Path(data["artifactPaths"]["input"]).parent
print("driver log: A&B + Z\u00fcrich", file=sys.stderr)
print(json.dumps({"workers": data["specification"]["effectiveParameters"]["threads"]}))
""", assets={"input": b"fixture data"})
        fixture.faults[("GET", "job", "next")] = ["drop"]
        fixture.faults[("POST", "job", "upload")] = ["drop"]
        fixture.faults[("PATCH", "job", "90")] = [None, "drop"]
        report = self.run_agent(fixture)
        self.assertEqual(("finished", 90, 501), (report.outcome, report.job_id, report.attempt_id))
        self.assertEqual(1, len(fixture.attempts))
        self.assertEqual(1, len(set(fixture.tokens)))
        self.assertEqual(2, fixture.counts[("GET", "job", "next")])
        self.assertEqual(2, fixture.counts[("PATCH", "job", "90")])
        self.assertEqual(fixture.uploads, [fixture.attempts[0]["archive"]] * 2)
        self.assertEqual(fixture.requests[0], ("GET", "job", {
            "id": "next", "protocol": "attempt-v1", "environment": "agent", "adapter": "chronos-command@1",
            "worker": "python.fixture:1", "supports": "fixture-system,other-system",
        }, {}))
        self.assertEqual(fixture.specification, json.loads((report.directory / "specification.json").read_bytes()))
        self.assertEqual((report.directory / "result.zip").read_bytes(), fixture.attempts[0]["archive"])
        attempt = fixture.attempts[0]
        self.assertTrue(attempt["selected"])
        self.assertEqual({"workers": 2}, attempt["result"])
        self.assertEqual(fixture.artifacts, attempt["artifacts"])
        self.assertTrue(attempt["environment"]["architecture"])
        self.assertEqual("driver log: A&B + Zürich\n", attempt["log"])
        with ZipFile(BytesIO(attempt["archive"])) as archive:
            self.assertEqual({"workers": 2}, json.loads(archive.read("stdout.json")))
            self.assertEqual(attempt["log"], archive.read("stderr.log").decode())

    def test_corrupt_artifact_fails_and_reschedule_uses_new_credential(self):
        fixture = AttemptFixture(self.cache, assets={"input": b"abc"})
        data = self.cache / fixture.artifacts["input"]["sha256"]
        data.write_bytes(b"bad")
        failure = self.run_agent(fixture)
        self.assertEqual("failed", failure.outcome)
        self.assertIn("SHA-256 mismatch", failure.message)
        self.assertFalse((failure.directory / "stdout.json").exists())
        self.assertEqual({"driver": fixture.artifacts["driver"]}, fixture.attempts[0]["artifacts"])
        self.assertEqual(-2, fixture.status)
        self.assertFalse(fixture.attempts[0]["selected"])
        data.write_bytes(b"abc")
        fixture.status = 0  # Operator reschedules the same repetition.
        success = self.run_agent(fixture)
        self.assertEqual("finished", success.outcome)
        self.assertEqual(failure.job_id, success.job_id)
        self.assertNotEqual(failure.attempt_id, success.attempt_id)
        self.assertNotEqual(fixture.attempts[0]["token"], fixture.attempts[1]["token"])
        self.assertEqual([-2, 3], [attempt["status"] for attempt in fixture.attempts])

    def test_refuses_symlink_even_when_the_target_matches_the_manifest(self):
        fixture = AttemptFixture(self.cache)
        path = self.cache / fixture.artifacts["driver"]["sha256"]
        target = self.cache / "driver-source"
        path.rename(target)
        path.symlink_to(target)
        report = self.run_agent(fixture)
        self.assertEqual("failed", report.outcome)
        self.assertEqual({}, fixture.attempts[0]["artifacts"])
        self.assertEqual([-2], [int(form["status"]) for method, _, _, form in fixture.requests if method == "PATCH"])

    def test_uncertain_start_never_starts_driver_or_replays(self):
        fixture = AttemptFixture(self.cache)
        fixture.faults[("PATCH", "job", "90")] = ["drop"]
        report = self.run_agent(fixture)
        self.assertEqual("error", report.outcome)
        self.assertIn("acknowledgement lost", report.message)
        self.assertEqual(2, fixture.status)
        self.assertEqual(1, fixture.counts[("PATCH", "job", "90")])
        self.assertFalse((report.directory / "stdout.json").exists())

    def test_uncommitted_ambiguous_completion_stays_active_for_operator(self):
        fixture = AttemptFixture(self.cache)
        fixture.faults[("PATCH", "job", "90")] = [None, "503"]
        report = self.run_agent(fixture)
        self.assertEqual("error", report.outcome)
        self.assertEqual(2, fixture.status)
        self.assertFalse(fixture.attempts[0]["selected"])
        self.assertEqual(2, fixture.counts[("PATCH", "job", "90")])
        self.assertTrue((report.directory / "result.zip").is_file())

    def test_ambiguous_log_is_not_replayed_and_archive_is_retained(self):
        fixture = AttemptFixture(self.cache)
        fixture.faults[("POST", "job", "appendLog")] = ["drop"]
        report = self.run_agent(fixture)
        self.assertEqual("error", report.outcome)
        self.assertEqual(-2, fixture.status)
        self.assertEqual(1, fixture.counts[("POST", "job", "appendLog")])
        self.assertEqual("Repetition 90, attempt 501\n", fixture.attempts[0]["log"])
        self.assertIsNotNone(fixture.attempts[0]["archive"])

    def test_rejected_measurement_retains_unselected_result_and_log(self):
        driver = b"#!/bin/sh\nprintf '{\"workers\":\"wrong\"}\\n'\nprintf 'bad value\\n' >&2\n"
        fixture = AttemptFixture(self.cache, driver=driver)
        report = self.run_agent(fixture)
        self.assertEqual("failed", report.outcome)
        self.assertEqual(-2, fixture.status)
        attempt = fixture.attempts[0]
        self.assertFalse(attempt["selected"])
        self.assertEqual({"workers": "wrong"}, attempt["result"])
        self.assertEqual("bad value\n", attempt["log"])
        self.assertIsNotNone(attempt["archive"])

    def test_archives_nested_driver_evidence_on_success_and_failure(self):
        for exit_code, expected in ((0, "finished"), (9, "failed")):
            with self.subTest(exit_code=exit_code):
                driver = f"""#!/usr/bin/env python3
import sys
from pathlib import Path
Path("evidence/nested").mkdir(parents=True)
Path("evidence/nested/trace.bin").write_bytes(bytes(range(256)))
for number in range(98):
    Path(f"evidence/nested/sample-{{number:02}}.txt").write_text("sample")
Path("outside.txt").write_text("outside the evidence tree")
print('{{"workers":2}}')
print("driver diagnostic", file=sys.stderr)
sys.exit({exit_code})
""".encode()
                fixture = AttemptFixture(self.cache, driver=driver)
                report = self.run_agent(fixture)
                self.assertEqual(expected, report.outcome)
                attempt = fixture.attempts[0]
                self.assertEqual({"workers": 2}, attempt["result"])
                self.assertEqual("driver diagnostic\n", attempt["log"])
                with ZipFile(BytesIO(attempt["archive"])) as archive:
                    expected_names = {"stdout.json", "stderr.log", "evidence/nested/trace.bin"}
                    expected_names.update(f"evidence/nested/sample-{number:02}.txt" for number in range(98))
                    self.assertEqual(expected_names, set(archive.namelist()))
                    self.assertEqual(bytes(range(256)), archive.read("evidence/nested/trace.bin"))

    def test_rejects_unsafe_evidence_with_independent_logs_and_result(self):
        variants = {
            "root symlink": ('Path("evidence").symlink_to("private")', "evidence directory"),
            "nested symlink": ('Path("evidence/unsafe").symlink_to("../private/secret")', "regular file"),
            "hard link": ('os.link("private/secret", "evidence/unsafe")', "one link"),
            "FIFO": ('os.mkfifo("evidence/unsafe")', "regular file"),
            "unsafe path": ('Path("evidence/.. ").mkdir(); Path("evidence/.. /secret").touch()', "Invalid driver evidence path"),
        }
        for name, (operation, diagnostic) in variants.items():
            with self.subTest(kind=name):
                setup = "" if name == "root symlink" else (
                    'Path("evidence").mkdir()\nPath("evidence/safe.txt").write_text("retained")'
                )
                driver = f"""#!/usr/bin/env python3
import os, sys
from pathlib import Path
Path("private").mkdir()
Path("private/secret").write_text("outside the evidence tree")
{setup}
{operation}
print('{{"workers":2}}')
print("driver diagnostic", file=sys.stderr)
""".encode()
                fixture = AttemptFixture(self.cache, driver=driver)
                report = self.run_agent(fixture)
                self.assertEqual("failed", report.outcome)
                self.assertIn(diagnostic, report.message)
                attempt = fixture.attempts[0]
                self.assertFalse(attempt["selected"])
                self.assertEqual({"workers": 2}, attempt["result"])
                self.assertEqual("driver diagnostic\n", attempt["log"])
                with ZipFile(BytesIO(attempt["archive"])) as archive:
                    retained = {path for path in archive.namelist() if path.startswith("evidence/")}
                    self.assertEqual(set() if name == "root symlink" else {"evidence/safe.txt"}, retained)
                    self.assertIn(diagnostic, archive.read("evidence-error.txt").decode())
                    if name != "root symlink":
                        self.assertEqual(b"retained", archive.read("evidence/safe.txt"))

    def test_enforces_driver_evidence_collection_limits(self):
        variants = {
            "files": ('for i in range(257): Path(f"evidence/{i:03}.txt").touch()', "256 files", 256),
            "entries": ('for i in range(513): Path(f"evidence/{i:03}").mkdir()', "512 directory entries", 0),
            "bytes": ('''with open("evidence/a.bin", "wb") as output:
    output.truncate(16 * 1024 * 1024)
Path("evidence/b.txt").write_bytes(b"too large")''', "16 MiB total", 1),
        }
        for label, (operation, diagnostic, expected) in variants.items():
            with self.subTest(limit=label):
                driver = f"""#!/usr/bin/env python3
from pathlib import Path
Path("evidence").mkdir()
{operation}
print('{{"workers":2}}')
""".encode()
                fixture = AttemptFixture(self.cache, driver=driver)
                report = self.run_agent(fixture)
                self.assertEqual("failed", report.outcome)
                self.assertIn(diagnostic, report.message)
                with ZipFile(BytesIO(fixture.attempts[0]["archive"])) as archive:
                    evidence = [file for file in archive.infolist() if file.filename.startswith("evidence/")]
                    self.assertEqual(expected, len(evidence))
                    if label == "bytes":
                        self.assertEqual(16 * 1024 * 1024, evidence[0].file_size)
                    self.assertEqual({"workers": 2}, json.loads(archive.read("stdout.json")))

    def test_operator_abort_stops_driver_and_keeps_local_evidence(self):
        driver = b"#!/bin/sh\nprintf 'driver started\\n' >&2\nexec sleep 30\n"
        fixture = AttemptFixture(self.cache, driver=driver)
        fixture.abort_on_read = True
        report = self.run_agent(fixture)
        self.assertEqual("stopped", report.outcome)
        self.assertEqual(-1, fixture.status)
        self.assertEqual(1, fixture.counts[("PATCH", "job", "90")])
        self.assertEqual([], fixture.uploads)
        with ZipFile(report.directory / "result.zip") as bundle:
            self.assertEqual(b"driver started\n", bundle.read("stderr.log"))

    def test_driver_timeout_records_failure_with_evidence(self):
        driver = b"#!/bin/sh\nprintf 'before timeout\\n' >&2\nexec sleep 30\n"
        fixture = AttemptFixture(self.cache, driver=driver)
        report = self.run_agent(fixture, timeout=2)
        self.assertEqual("timeout", report.outcome)
        self.assertEqual(-2, fixture.status)
        self.assertEqual("before timeout\n", fixture.attempts[0]["log"])
        self.assertIsNotNone(fixture.attempts[0]["archive"])

    def test_local_stop_records_interrupted_attempt(self):
        driver = b"#!/bin/sh\nprintf 'before interruption\\n' >&2\nexec sleep 30\n"
        fixture = AttemptFixture(self.cache, driver=driver)
        evidence = self.evidence

        class StopAfterDriverStarts:
            def is_set(self):
                return any(path.stat().st_size for path in evidence.glob("*/stderr.log"))

        report = self.run_agent(fixture, stop=StopAfterDriverStarts())
        self.assertEqual("cancelled", report.outcome)
        self.assertEqual(-2, fixture.status)
        self.assertIn("interrupted", fixture.attempts[0]["reason"])
        self.assertEqual("before interruption\n", fixture.attempts[0]["log"])
        self.assertIsNotNone(fixture.attempts[0]["archive"])

    def test_cli_cem_handoff_runs_a_preclaimed_repetition_through_bash(self):
        fixture = AttemptFixture(self.cache)
        with serving(fixture) as url:
            attempt = AttemptClient(url).claim("fixture-cem", (), "agent")
            proc = subprocess.run([
                str(ROOT / "examples" / "agent.sh"), "--protocol", "attempt-v1", "--url", url,
                "--artifact-cache", str(self.cache), "--work-dir", str(self.evidence), "--once",
                "--attempt-job", "90",
            ], env={
                **os.environ, "PYTHON": sys.executable, "PYTHONPATH": str(ROOT / "src"),
                "CHRONOS_ATTEMPT_TOKEN": attempt.token,
            }, text=True, capture_output=True, timeout=10)
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual(501, json.loads(proc.stdout)["attemptId"])
        self.assertEqual(1, fixture.counts[("GET", "job", "next")])
        self.assertEqual(3, fixture.status)
        self.assertNotIn(attempt.token, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main()
