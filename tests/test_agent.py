from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Event
import time
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from chronos_agent import (
    Agent, ApiError, Cdl, CdlError, CemNode, Client, Command, Hooks, Phase, ProtocolError,
    Status, TransportError, UncertainWrite,
)
from mock_server import CDL, SYSTEM_CDL, Fixture, serving


ROOT = Path(__file__).resolve().parents[1]


class ProtocolTests(unittest.TestCase):
    def test_system_poll_and_legacy_alias(self):
        fixture = Fixture(environment="system-A/B + Zürich")
        with serving(fixture) as url:
            client = Client(url, front_controller=True)
            self.assertIsNone(client.poll(("fixture-system",), "empty-fixture"))
            job = client.poll(("fixture-system", "fixture-system"), "A/B + Zürich")
            self.assertEqual((90, "fixture-system", "system-A/B + Zürich", SYSTEM_CDL),
                             (job.id, job.system, job.environment, job.cdl))
            self.assertEqual(("GET", "job", {
                "id": "next", "supports": "fixture-system,fixture-system",
                "environment": "A/B + Zürich",
            }, {}), fixture.requests[-1])
            self.assertEqual(job, client.get_job(90))

    def test_envelope_error_codes(self):
        with serving(Fixture()) as url:
            client = Client(url)
            calls = [
                (600, "Unknown system", lambda: client.poll(("unknown",))),
                (602, "Job does not exist", lambda: client.get_job(999)),
                (602, "Job does not exist", lambda: client.update_job(999, status=Status.RUNNING)),
                (631, "Outdated version", lambda: client.poll_cem(CemNode("node", 1, "fixture"))),
                (632, "Unknown CEM environment", lambda: client.poll_cem(CemNode("node", 2, "unknown"))),
            ]
            for code, message, call in calls:
                with self.subTest(code=code), self.assertRaisesRegex(ApiError, message) as caught:
                    call()
                self.assertEqual(code, caught.exception.code)
            self.assertIsNone(client.poll_cem(CemNode("node", 2, "fixture")))

    def test_results_progress_and_log_are_form_encoded(self):
        fixture = Fixture()
        result = {"throughput": 123.5, "note": "A&B + Zürich", "zero": 0, "flag": False}
        with serving(fixture) as url:
            client = Client(url)
            client.update_job(90, status=Status.RUNNING, progress=47)
            client.update_job(90, phase=Phase.EXECUTE)
            self.assertEqual((2, 3, 0), tuple(fixture.job[k] for k in ("status", "currentPhase", "progress")))
            with self.assertRaisesRegex(ValueError, "separately"):
                client.update_job(90, phase=Phase.EXECUTE, progress=80)
            client.update_job(90, progress=80, result=result)
            client.append_log(90, "Agent: A&B + Zürich\n")
        self.assertEqual(result, fixture.result)
        self.assertEqual(80, fixture.job["progress"])
        self.assertEqual("Agent: A&B + Zürich\n", fixture.log)

    def test_read_retries_are_bounded(self):
        fixture = Fixture()
        queue = ("GET", "job", "next")
        fixture.faults[queue] = ["503", "drop"]
        with serving(fixture) as url:
            client = Client(url, read_retries=2, retry_delay=0)
            self.assertEqual(90, client.poll(("fixture-system",), "agent").id)
            self.assertEqual(3, fixture.counts[queue])
            fixture.faults[queue] = ["malformed"] * 5
            with self.assertRaisesRegex(TransportError, "invalid Chronos envelope"):
                client.poll(("fixture-system",), "agent")
            self.assertEqual(6, fixture.counts[queue])

    def test_cem_selection_can_reserve_and_is_never_retried(self):
        fixture = Fixture(environment="cem-fixture")
        queue = ("GET", "cem", "poll")
        fixture.faults[queue] = ["drop"]
        with serving(fixture) as url:
            with self.assertRaisesRegex(UncertainWrite, "acknowledgement lost"):
                Client(url).poll_cem(CemNode("fixture-node", 2, "fixture"))
        self.assertEqual(1, fixture.counts[queue])
        self.assertEqual(1, fixture.reservations)

    def test_mutating_redirect_is_not_followed(self):
        fixture = Fixture()
        fixture.faults[("PATCH", "job", "90")] = ["redirect"]
        with serving(fixture) as url:
            with self.assertRaises(UncertainWrite):
                Client(url).update_job(90, status=Status.SETUP)
        self.assertEqual(1, len(fixture.requests))

    def test_http_upload_origin_and_exact_bytes(self):
        fixture = Fixture()
        alternate = Fixture()
        archive_bytes = bytes.fromhex("504b0506000000000000000000000000000000000000")
        with TemporaryDirectory() as temp, serving(fixture) as url, serving(alternate) as upload_url:
            fixture.upload_target["hostname"] = upload_url
            path = Path(temp) / "empty archive.zip"
            path.write_bytes(archive_bytes)
            with self.assertRaisesRegex(ProtocolError, "upload origin"):
                Client(url).upload_archive(90, path)
            self.assertIsNone(alternate.archive)
            Client(url, upload_origin=upload_url).upload_archive(90, path)
            self.assertEqual(archive_bytes, alternate.archive)

    def test_ftp_uses_configured_legacy_target_once(self):
        fixture = Fixture()
        fixture.upload_target.update({
            "method": "ftp", "hostname": "ftp.fixture", "path": "/chronos/evaluation/",
            "port": "2121", "username": "fixture", "password": "synthetic", "filename": "90.zip",
        })
        with TemporaryDirectory() as temp, serving(fixture) as url:
            archive = Path(temp) / "archive.zip"
            archive.write_bytes(b"archive")
            with self.assertRaisesRegex(ProtocolError, "matching host"):
                Client(url).upload_archive(90, archive)
            with patch("chronos_agent.protocol.FTP") as ftp:
                connection = ftp.return_value.__enter__.return_value
                connection.storbinary.side_effect = OSError("simulated transfer disruption")
                with self.assertRaises(UncertainWrite):
                    Client(url, ftp_host="ftp.fixture").upload_archive(90, archive)
                connection.connect.assert_called_once_with("ftp.fixture", 2121)
                connection.cwd.assert_called_once_with("/chronos/evaluation/")
                self.assertEqual(1, connection.storbinary.call_count)
                self.assertEqual("STOR 90.zip", connection.storbinary.call_args.args[0])


class CdlTests(unittest.TestCase):
    def test_text_unicode_cr_and_empty_values(self):
        cdl = Cdl.parse(CDL)
        self.assertEqual("1", cdl.system_id)
        self.assertEqual("fixture-setup-value", cdl.setup["fixtureSetup"])
        self.assertEqual("Zürich & <value> &amp;\r\nsecond line 😀", cdl.evaluation["label"])
        self.assertEqual(("0", ""), (cdl.evaluation["zero"], cdl.evaluation["empty"]))

    def test_malformed_or_unsupported_xml(self):
        for xml in ("<chronos>", "<other/>", "<chronos><setup/><evaluation/></chronos>",
                    '<chronos><setup/><evaluation system="1"><x><y/></x></evaluation></chronos>',
                    '<chronos><setup/><evaluation system="1"><x/><x/></evaluation></chronos>',
                    '<!DOCTYPE chronos [<!ENTITY x "expanded">]><chronos/>'):
            with self.subTest(xml=xml), self.assertRaises(CdlError):
                Cdl.parse(xml)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)

    def run_fixture(self, fixture, hooks, *, cem=None, stop=None, timeout=5, cleanup_timeout=1):
        with serving(fixture) as url:
            agent = Agent(Client(url, retry_delay=0), hooks, directory=self.path,
                          systems=() if cem else ("fixture-system",), environment="agent",
                          cem=cem, stop=stop, timeout=timeout,
                          cleanup_timeout=cleanup_timeout, status_interval=0.05)
            return agent.run_once()

    def test_empty_queue_does_not_create_run_evidence(self):
        report = self.run_fixture(Fixture(status=3), Hooks(run=lambda _: self.fail("must not run")))
        self.assertIsNone(report)
        self.assertEqual([], list(self.path.iterdir()))

    def test_full_lifecycle_and_evidence(self):
        fixture = Fixture()
        order = []

        def hook(ctx):
            order.append(ctx.phase)
            ctx.log(f"{ctx.phase.name} A&B + Zürich")
            if ctx.phase == Phase.EXECUTE:
                ctx.progress(47)
                ctx.result = {"answer": 0, "passed": True}
                (ctx.artifacts / "evidence.txt").write_bytes(b"synthetic evidence")

        report = self.run_fixture(fixture, Hooks(run=hook, setup=hook, warmup=hook,
                                                validate=hook, teardown=hook))
        self.assertEqual(list(Phase), order)
        self.assertEqual("finished", report.outcome)
        self.assertEqual(3, fixture.job["status"])
        self.assertEqual(100, fixture.job["progress"])
        self.assertEqual({"answer": 0, "passed": True}, fixture.result)
        self.assertEqual(1, fixture.log.count("EXECUTE A&B + Zürich"))
        patches = [form for method, _, _, form in fixture.requests if method == "PATCH"]
        self.assertEqual({"status": "1"}, patches[0])
        self.assertEqual(["1", "2", "3", "4", "5"],
                         [form["currentPhase"] for form in patches if "currentPhase" in form])
        self.assertIn({"progress": "47"}, patches)
        self.assertTrue(all(len(form) == 1 for form in patches if "currentPhase" in form))
        self.assertEqual("3", patches[-1]["status"])
        self.assertEqual(SYSTEM_CDL, (report.directory / "job.xml").read_text(encoding="utf-8"))
        with ZipFile(BytesIO(fixture.archive)) as archive:
            self.assertEqual(b"synthetic evidence", archive.read("artifacts/evidence.txt"))
            evidence = json.loads(archive.read("execution.json"))
        self.assertEqual(sha256(SYSTEM_CDL.encode("utf-8")).hexdigest(), evidence["cdlSha256"])
        self.assertEqual("completed", evidence["workloadOutcome"])
        self.assertEqual(["prepare", "warmup", "execute", "analyze", "clean"],
                         [record["phase"] for record in evidence["phases"]])
        self.assertTrue(all(record["durationSeconds"] >= 0 for record in evidence["phases"]))
        self.assertNotIn("attemptId", evidence)

    def test_excluded_hooks_and_cem_lifecycle(self):
        fixture = Fixture(environment="cem-fixture", mask=2 | 8)
        seen = []

        def hook(ctx):
            seen.append(ctx.phase)
            self.assertEqual(fixture.setup, ctx.job.cem_setup)

        report = self.run_fixture(fixture, Hooks(run=hook, setup=hook, warmup=hook,
                                                validate=hook, teardown=hook),
                                  cem=CemNode("A/B + Zürich", 2, "fixture"))
        self.assertEqual("finished", report.outcome)
        self.assertEqual([Phase.PREPARE, Phase.EXECUTE, Phase.CLEAN], seen)
        self.assertIsNone(fixture.node_job)
        actions = [(params["action"], params["uniqueId"], fields)
                   for method, endpoint, params, fields in fixture.requests
                   if (method, endpoint) == ("POST", "cem")]
        self.assertEqual([("jobStarted", "A/B + Zürich", {"jobId": "90"}),
                          ("jobTerminated", "A/B + Zürich", {"jobId": "90"})], actions)

    def test_bad_cdl_fails_without_running_hooks(self):
        fixture = Fixture(cdl="<chronos>")
        report = self.run_fixture(fixture, Hooks(run=lambda _: self.fail("must not run")))
        self.assertEqual(("failed", -2), (report.outcome, fixture.job["status"]))
        self.assertIn("Malformed CDL", fixture.log)
        self.assertIsNone(fixture.archive)

    def test_command_failure_logs_and_teardown(self):
        fixture = Fixture()
        run = Command([sys.executable, "-c", "print('failure output'); raise SystemExit(29)"])
        teardown = Command([sys.executable, "-c", "print('cleanup executed')"])
        report = self.run_fixture(fixture, Hooks(run=run, teardown=teardown))
        self.assertEqual(("failed", -2), (report.outcome, fixture.job["status"]))
        self.assertIn("execute command exited with code 29", report.message)
        self.assertEqual(1, fixture.log.count("failure output"))
        self.assertEqual(1, fixture.log.count("cleanup executed"))
        self.assertIsNone(fixture.archive)
        evidence = json.loads((report.directory / "execution.json").read_text())
        self.assertEqual([29, 0], [entry["returnCode"] for entry in evidence["commands"]])

    def test_validation_failure_preserves_local_evidence_and_withholds_results(self):
        fixture = Fixture()

        def run(ctx):
            ctx.result = {"unvalidated": True}

        def validate(ctx):
            raise ValueError("Synthetic correctness check failed.")

        report = self.run_fixture(fixture, Hooks(run=run, validate=validate))
        self.assertEqual("failed", report.outcome)
        self.assertIn("correctness check failed", fixture.log)
        self.assertIsNone(fixture.result)
        self.assertIsNone(fixture.archive)

    def test_local_cancellation_runs_teardown_and_marks_aborted(self):
        fixture = Fixture()
        stop = Event()
        order = []

        def run(ctx):
            stop.set()
            ctx.check()

        report = self.run_fixture(fixture, Hooks(run=run, teardown=lambda _: order.append("teardown")),
                                  stop=stop)
        self.assertEqual(("cancelled", -1), (report.outcome, fixture.job["status"]))
        self.assertEqual(["teardown"], order)
        self.assertIsNone(fixture.archive)

    def test_remote_cancellation_preserves_server_status(self):
        fixture = Fixture(environment="cem-fixture")
        order = []

        def run(ctx):
            fixture.abort_on_read = True
            order.append("run")

        report = self.run_fixture(fixture,
                                  Hooks(run=run, teardown=lambda _: order.append("teardown")),
                                  cem=CemNode("fixture-node", 2, "fixture"))
        self.assertEqual(("cancelled", -1), (report.outcome, fixture.job["status"]))
        self.assertEqual(["run", "teardown"], order)
        self.assertIsNone(fixture.node_job)
        writes = [form["status"] for method, _, _, form in fixture.requests
                  if method == "PATCH" and "status" in form]
        self.assertEqual(["2"], writes)
        self.assertIsNone(fixture.archive)

    @unittest.skipUnless(os.name == "posix", "Requires POSIX process groups")
    def test_timeout_cleans_up_group_and_preserves_failure_output(self):
        fixture = Fixture()
        marker = self.path / "escaped-child"
        grandchild = "import time; from pathlib import Path; time.sleep(1.2); Path(%r).touch()" % str(marker)
        script = (
            "import subprocess, sys, time, signal;"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
            "subprocess.Popen([sys.executable, '-c', sys.argv[1]]);"
            "print('started group', flush=True); time.sleep(30)"
        )
        run = Command([sys.executable, "-c", script, grandchild])
        report = self.run_fixture(fixture, Hooks(run=run), timeout=0.6)
        self.assertEqual(("timeout", -2), (report.outcome, fixture.job["status"]), report.message)
        self.assertIn("started group", fixture.log)
        self.assertIn("timeout", fixture.log)
        evidence = json.loads((report.directory / "execution.json").read_text())
        self.assertEqual(-signal.SIGKILL, evidence["commands"][0]["returnCode"])
        time.sleep(0.7)
        self.assertFalse(marker.exists())

    def test_teardown_timeout_is_bounded(self):
        fixture = Fixture()
        cleanup = Command([sys.executable, "-c", "import time; time.sleep(30)"])
        report = self.run_fixture(fixture, Hooks(run=lambda _: None, teardown=cleanup),
                                  cleanup_timeout=0.15)
        self.assertEqual(("timeout", -2), (report.outcome, fixture.job["status"]), report.message)
        self.assertIn("teardown timeout", report.message)

    def test_ambiguous_claim_never_starts_hooks(self):
        fixture = Fixture()
        fixture.faults[("PATCH", "job", "90")] = ["drop"]
        with serving(fixture) as url:
            agent = Agent(Client(url), Hooks(run=lambda _: self.fail("ambiguous claim")),
                          directory=self.path, systems=("fixture-system",), environment="agent")
            report = agent.run_once()
            after_failure = len(fixture.requests)
            with self.assertRaisesRegex(ProtocolError, "Agent halted"):
                agent.run_once()
            self.assertEqual(after_failure, len(fixture.requests))
        self.assertEqual(("error", 1), (report.outcome, fixture.job["status"]))
        self.assertEqual(1, fixture.counts[("PATCH", "job", "90")])
        self.assertIn("acknowledgement lost", report.message)

    def test_lost_upload_ack_keeps_local_zip_without_retry_or_finish(self):
        fixture = Fixture()
        fixture.faults[("POST", "job", "upload")] = ["drop"]
        report = self.run_fixture(fixture, Hooks(run=lambda _: None))
        self.assertEqual(("error", 2), (report.outcome, fixture.job["status"]))
        self.assertEqual(1, fixture.counts[("POST", "job", "upload")])
        self.assertEqual(fixture.archive, (report.directory / "results.zip").read_bytes())
        self.assertIsNone(fixture.result)

    def test_lost_result_ack_never_replays_or_overwrites_terminal_status(self):
        fixture = Fixture()

        def run(ctx):
            ctx.result = {"synthetic": True}
            # The next PATCH completes execute progress; the following writes result and status.
            fixture.faults[("PATCH", "job", "90")] = [None, "drop"]

        report = self.run_fixture(fixture, Hooks(run=run))
        self.assertEqual(("error", 3), (report.outcome, fixture.job["status"]))
        reports = [form for method, _, _, form in fixture.requests if method == "PATCH" and "result" in form]
        self.assertEqual(1, len(reports))
        self.assertEqual({"synthetic": True}, fixture.result)
        self.assertEqual(fixture.result, json.loads((report.directory / "result.json").read_text()))

    def test_lost_log_ack_does_not_duplicate_log_and_runs_local_cleanup(self):
        fixture = Fixture()
        fixture.faults[("POST", "job", "appendLog")] = ["malformed"]
        order = []
        report = self.run_fixture(fixture, Hooks(run=lambda ctx: ctx.log("synthetic log"),
                                                teardown=lambda _: order.append("teardown")))
        self.assertEqual(("error", 2), (report.outcome, fixture.job["status"]))
        self.assertEqual(1, fixture.log.count("synthetic log"))
        self.assertEqual(["teardown"], order)
        self.assertEqual(1, fixture.counts[("POST", "job", "appendLog")])

    def test_symlink_artifact_is_rejected(self):
        fixture = Fixture()

        def run(ctx):
            (ctx.artifacts / "external").symlink_to(ctx.cdl_path)

        report = self.run_fixture(fixture, Hooks(run=run))
        self.assertEqual(("failed", -2), (report.outcome, fixture.job["status"]))
        self.assertIn("regular files or directories", report.message)
        self.assertIsNone(fixture.archive)
        self.assertEqual([], list(report.directory.glob("results.zip*")))


class CliTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)

    def arguments(self, url):
        return ["--url", url, "--system", "fixture-system", "--environment", "agent",
                "--work-dir", str(self.path), "--once"]

    def environment(self):
        return {**os.environ, "PYTHON": sys.executable, "PYTHONPATH": str(ROOT / "src")}

    def invoke(self, command, options):
        return subprocess.run([*command, *options], env=self.environment(), cwd=self.path,
                              capture_output=True, text=True, timeout=5)

    def test_cli_and_bash_preserve_arguments_without_shell_interpretation(self):
        commands = ([sys.executable, "-m", "chronos_agent"], [str(ROOT / "examples" / "agent.sh")])
        args = ["space and ü", "", '--', "$(touch injected)", "; touch injected", '"quote"']
        for command in commands:
            with self.subTest(command=command), serving(Fixture()) as url:
                result = self.invoke(command, [*self.arguments(url),
                                               "--setup-json", json.dumps([
                                                   sys.executable, "-c",
                                                   "import sys; assert sys.argv[1:] == ['--', '', 'a b']",
                                                   "--", "", "a b",
                                               ]),
                                               "--", sys.executable,
                                               str(ROOT / "examples" / "workload.py"), *args])
                self.assertEqual(0, result.returncode, result.stderr)
                record = json.loads(result.stdout)
                self.assertEqual("finished", record["outcome"])
                self.assertEqual(args, json.loads(Path(record["directory"], "result.json").read_text())["arguments"])
                self.assertFalse(Path(record["directory"], "injected").exists())

    def test_exit_codes_and_diagnostics(self):
        cli = [sys.executable, "-m", "chronos_agent"]
        with serving(Fixture(status=3)) as url:
            self.assertEqual(3, self.invoke(cli, [*self.arguments(url), "--", "ignored"]).returncode)
            bad = self.invoke(cli, [*self.arguments(url), "--setup-json", '"shell string"', "--", "ignored"])
            self.assertEqual(2, bad.returncode)
            self.assertIn("JSON argument array", bad.stderr)
        with serving(Fixture()) as url:
            failure = self.invoke(cli, [*self.arguments(url), "--", "/missing/chronos-synthetic-command"])
            self.assertEqual(1, failure.returncode)
            self.assertIn("Unable to start execute command", failure.stderr)
        fixture = Fixture()
        fixture.faults[("GET", "job", "next")] = ["503"]
        with serving(fixture) as url:
            unavailable = self.invoke(cli, [*self.arguments(url), "--read-retries", "0", "--", "ignored"])
            self.assertEqual(5, unavailable.returncode)
            self.assertIn("HTTP 503", unavailable.stderr)

    @unittest.skipUnless(os.name == "posix", "Requires POSIX signals")
    def test_sigterm_reaches_python_through_bash_and_cleans_up(self):
        fixture = Fixture()
        with serving(fixture) as url:
            args = [str(ROOT / "examples" / "agent.sh"), *self.arguments(url), "--", sys.executable,
                    "-c", "import time; print('ready', flush=True); time.sleep(30)"]
            with subprocess.Popen(args, env=self.environment(), cwd=self.path, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
                try:
                    deadline = time.monotonic() + 3
                    while time.monotonic() < deadline:
                        if any("ready" in path.read_text() for path in self.path.glob("*/execute.log")):
                            break
                        if process.poll() is not None:
                            self.fail(process.communicate())
                        time.sleep(0.02)
                    else:
                        self.fail("Synthetic command did not start.")
                    process.terminate()
                    stdout, stderr = process.communicate(timeout=4)
                    self.assertEqual(143, process.returncode, stderr)
                    self.assertEqual("cancelled", json.loads(stdout)["outcome"])
                    self.assertEqual(-1, fixture.job["status"])
                finally:
                    if process.poll() is None:
                        process.kill()


if __name__ == "__main__":
    unittest.main()
