"""An isolated in-memory HTTP fixture for the v1 handlers used by this agent."""

from collections import Counter
from contextlib import contextmanager
import argparse
from email import policy
from email.parser import BytesParser
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import socket
from threading import Event, Thread
from urllib.parse import parse_qsl, unquote_plus, urlsplit


CDL = """<?xml version="1.0" encoding="utf-8"?>
<chronos>
  <setup><fixtureSetup>fixture-setup-value</fixtureSetup></setup>
  <evaluation system="1">
    <label>Zürich &amp; &lt;value&gt; &amp;amp;&#13;
second line 😀</label>
    <zero>0</zero><empty/>
  </evaluation>
</chronos>"""


SYSTEM_CDL = CDL.replace("<setup><fixtureSetup>fixture-setup-value</fixtureSetup></setup>", "<setup/>")


class Fixture:
    def __init__(self, *, cdl=None, status=0, mask=0, environment="system-agent"):
        if cdl is None:
            cdl = CDL if environment.startswith("cem-") else SYSTEM_CDL
        self.job = {
            "id": 90, "user": 3, "username": "owner", "status": status, "progress": 0,
            "phases": mask, "currentPhase": 0, "system": "fixture-system",
            "environment": environment, "cdl": cdl,
        }
        self.setup = {"fixtureSetup": "fixture-setup-value"}
        self.requests = []
        self.log = ""
        self.archive = None
        self.result = None
        self.node_job = None
        self.reservations = 0
        self.faults = {}
        self.counts = Counter()
        self.upload_target = {"method": "http", "path": "/api/v1/job", "hostname": ""}
        self.abort_on_read = False

    def dispatch(self, method, endpoint, params, form, archive, token):
        job_id = params.get("id")
        action = params.get("action")
        if endpoint == "cem":
            if params.get("version") != "2":
                return 631, "Outdated version, please update.", None
            if params.get("environment") != "fixture":
                return 632, "Unknown CEM environment.", None
            if method == "GET":
                if self.job["status"] != 0 or self.job["environment"] != "cem-fixture":
                    return 601, "No job in queue!", None
                self.reservations += 1
                return 200, "success", {
                    "job": {"id": 90, "status": 0}, "system": {"id": 1},
                    "setup": self.setup, "cdl": self.job["cdl"],
                }
            if form.get("jobId") != "90":
                return 602, "Job does not exist!", None
            if action == "jobStarted":
                self.job["status"] = 1
                self.node_job = 90
            elif action == "jobTerminated":
                if form.get("reason") or self.job["status"] in (0, 1, 2):
                    self.job["status"] = -2
                self.node_job = None
            else:
                return 600, "Unsupported action", None
            return 200, "success", None
        if endpoint != "job":
            return 600, "Unavailable endpoint", None
        if method == "GET" and job_id == "next":
            supports = params.get("supports", "").split(",")
            if supports != ["demoall"] and any(system != "fixture-system" for system in supports):
                return 600, "Unknown system: " + ",".join(supports), None
            if self.job["status"] != 0 or self.job["environment"] != "system-" + params.get("environment", "default"):
                return 601, "No job in queue!", None
            return 200, "success", self.job.copy()
        if job_id != "90":
            return 602, "Job does not exist!", None
        if method == "GET":
            if self.abort_on_read and self.job["status"] == 2:
                self.job["status"] = -1
            return 200, "success", self.job.copy()
        if method == "PATCH":
            for key in ("status", "progress", "currentPhase"):
                if key in form:
                    self.job[key] = int(form[key])
            if "currentPhase" in form:
                self.job["progress"] = 0
            if "result" in form:
                self.result = json.loads(form["result"])
        elif action == "appendLog" and form.get("log") not in ("", "0", None):
            self.log += form["log"]
        elif action == "getUploadTarget":
            return 200, "success", self.upload_target
        elif action == "upload" and archive is not None:
            self.archive = archive
        else:
            return 600, "Unsupported action or invalid request data.", None
        return 200, "success", None


class AttemptFixture:
    def __init__(self, cache, *, driver=None, assets=None):
        if driver is None:
            driver = (Path(__file__).resolve().parents[1] / "examples" / "attempt_driver.py").read_bytes()
        self.artifacts = {}
        entries = []
        cache.mkdir(parents=True, exist_ok=True)
        for role, data in {"driver": driver, **(assets or {})}.items():
            digest = sha256(data).hexdigest()
            (cache / digest).write_bytes(data)
            self.artifacts[role] = {"sha256": digest, "sizeBytes": len(data)}
            entries.append({"role": role, "uri": f"staged:fixture-{role}", **self.artifacts[role]})
        spec = {
            "protocol": "attempt-v1", "adapter": "chronos-command@1", "schemaVersion": 1,
            "system": {"id": 1, "identifier": "fixture-system", "version": "test-1"},
            "benchmark": {"id": 1, "version": "1", "contract": {
                "parameters": {"workers": {"type": "integer"}}, "measurement": "Synthetic worker input.",
                "lifecycle": {"warmUp": "disabled"}, "results": {"workers": {"type": "integer"}},
            }},
            "binding": {"id": 1, "version": "1"},
            "environment": "system-agent", "phases": 2, "parameters": {"workers": 2},
            "effectiveParameters": {"threads": 2}, "setup": [], "cdl": SYSTEM_CDL,
            "artifacts": {"schemaVersion": 1, "artifacts": entries, "buildId": "fixture-build"},
        }
        spec["sha256"] = sha256(json.dumps(spec).encode()).hexdigest()
        self.specification = spec
        self.status = 0
        self.attempts = []
        self.tokens = []
        self.uploads = []
        self.requests = []
        self.faults = {}
        self.counts = Counter()
        self.abort_on_read = False

    def dispatch(self, method, endpoint, params, form, archive, token):
        if endpoint != "job" or params.get("protocol") != "attempt-v1":
            return 600, "Unsupported protocol", None
        if not token or not re.fullmatch(r"[a-f0-9]{64}", token):
            return 600, "Invalid attempt token", None
        job_id = params.get("id")
        self.tokens.append(token)
        if method == "GET" and job_id == "next":
            if (params.get("adapter") != "chronos-command@1" or not params.get("worker")
                    or "system-" + params.get("environment", "") != self.specification["environment"]):
                return 601, "No job in queue!", None
            supports = params.get("supports", "demoall")
            if supports != "demoall" and "fixture-system" not in supports.split(","):
                return 601, "No job in queue!", None
            previous = next((attempt for attempt in self.attempts if attempt["token"] == token), None)
            if previous is None:
                if self.status != 0:
                    return 601, "No job in queue!", None
                self.attempts.append({"id": 501 + len(self.attempts), "token": token, "worker": params["worker"],
                                      "status": 1, "log": "", "archive": None, "selected": False})
                self.status = 1
            elif self.status not in (1, 2) or params["worker"] != previous["worker"]:
                return 600, "This attempt no longer owns the repetition.", None
        elif job_id != "90":
            return 602, "Job does not exist!", None
        if not self.attempts or token != self.attempts[-1]["token"]:
            return 600, "This attempt no longer owns the repetition.", None
        attempt = self.attempts[-1]
        if method == "GET":
            if self.abort_on_read and self.status == 2:
                self.status = attempt["status"] = -1
            return 200, "success", {
                "id": 90, "repetitionId": 90, "attemptId": attempt["id"], "status": self.status,
                "protocol": "attempt-v1", "specification": self.specification,
            }
        if self.status not in (1, 2):
            return 600, "This attempt no longer owns the repetition.", None
        if method == "PATCH":
            target = int(form["status"])
            result = json.loads(form["result"]) if "result" in form else None
            if target == 2 and self.status != 1:
                return 600, "Invalid attempt status transition", None
            if target == 3 and (self.status != 2 or type((result or {}).get("workers")) is not int
                                or attempt.get("artifacts") != self.artifacts):
                return 600, "Please check the submitted values.", None
            self.status = attempt["status"] = target
            for field in ("environment", "artifacts", "result"):
                if field in form:
                    attempt[field] = json.loads(form[field])
            if "reason" in form:
                attempt["reason"] = form["reason"]
            attempt["selected"] = target == 3
        elif params.get("action") == "appendLog" and "log" in form:
            attempt["log"] += form["log"]
        elif params.get("action") == "upload" and archive is not None:
            if attempt["archive"] not in (None, archive):
                return 600, "This attempt already has a different archive.", None
            attempt["archive"] = archive
            self.uploads.append(archive)
        else:
            return 600, "Unsupported attempt action", None
        return 200, "success", None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        self._run()

    do_POST = do_PATCH = do_GET

    def _run(self):
        parts = urlsplit(self.path)
        path = parts.query if parts.path == "/index.php" else parts.path
        segments = path.removeprefix("/api/v1/").split("/")
        endpoint = segments.pop(0)
        params = {name: unquote_plus(value) for name, value in
                  (part.split("=", 1) for part in segments if "=" in part)}
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        form, archive = {}, None
        if self.headers.get_content_type() == "application/x-www-form-urlencoded":
            form = dict(parse_qsl(raw.decode("ascii"), keep_blank_values=True))
        if self.headers.get_content_type() == "multipart/form-data":
            header = f"Content-Type: {self.headers['Content-Type']}\r\n\r\n".encode()
            parts = BytesParser(policy=policy.default).parsebytes(header + raw)
            for part in parts.iter_parts():
                if part.get_param("name", header="content-disposition") == "result":
                    archive = part.get_payload(decode=True)
        fixture = self.server.fixture
        action = params.get("action", params.get("id", "poll"))
        key = self.command, endpoint, action
        fixture.requests.append((self.command, endpoint, params, form))
        fixture.counts[key] += 1
        faults = fixture.faults.get(key, [])
        fault = faults.pop(0) if faults else None
        if fault == "503":
            self.send_error(503)
            return
        if fault == "redirect":
            self.send_response(307)
            self.send_header("Location", "/api/v1/job/id=90/action=appendLog")
            self.end_headers()
            return
        code, message, payload = fixture.dispatch(
            self.command, endpoint, params, form, archive, self.headers.get("X-Chronos-Attempt-Token"),
        )
        if fault == "drop":
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        envelope = {"status": {"code": code, "message": message}}
        if payload is not None:
            envelope["response"] = payload
        response = b"not JSON" if fault == "malformed" else json.dumps(envelope).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


@contextmanager
def serving(fixture):
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.fixture = fixture
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempt-cache", type=Path, help="Serve an attempt and stage its driver in this cache.")
    options = parser.parse_args()
    fixture = AttemptFixture(options.attempt_cache) if options.attempt_cache else Fixture()
    with serving(fixture) as url:
        print(url, flush=True)
        try:
            Event().wait()
        except KeyboardInterrupt:
            pass
