from concurrent.futures import ThreadPoolExecutor
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from runledger.db import Conflict, Database
from runledger.evaluation import benchmark_fingerprint, compare, evaluate, validate
from runledger.web import ROOT, Server
from runledger.worker import run_one

def fixture(name="baseline"):
    return json.loads((ROOT / "examples" / f"{name}.json").read_text())

class Clock:
    def __init__(self): self.now = 1000.
    def __call__(self): return self.now
    def advance(self, seconds): self.now += seconds

class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.clock = Clock()
        self.path = Path(self.temp.name) / "jobs.sqlite"
        self.db = Database(self.path, self.clock, base_backoff=.01)
    def tearDown(self): self.temp.cleanup()
    def submit(self, key="test", attempts=3): return self.db.submit(key, fixture(), attempts)[0]["id"]

    def test_idempotent_concurrent_submission_and_conflict(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.db.submit("same", fixture()), range(24)))
        self.assertEqual(len({job["id"] for job, _ in results}), 1)
        self.assertEqual(sum(created for _, created in results), 1)
        changed = fixture(); changed["name"] = "different"
        with self.assertRaises(Conflict): self.db.submit("same", changed)
        with self.assertRaises(Conflict): self.db.submit("same", fixture(), 4)

    def test_atomic_claim_has_one_owner(self):
        job = self.submit()
        with ThreadPoolExecutor(max_workers=12) as pool:
            claims = list(pool.map(lambda i: self.db.claim(f"worker-{i}"), range(24)))
        self.assertEqual(sum(claim is not None for claim in claims), 1)
        self.assertEqual(len(self.db.get(job)["attempts"]), 1)

    def test_expired_worker_cannot_complete_heartbeat_or_fail(self):
        job = self.submit(); first = self.db.claim("first", 1.)
        self.clock.advance(1.1)
        self.assertFalse(self.db.complete(job, first["lease_token"], {"wrong": 1}))
        self.assertFalse(self.db.heartbeat(job, first["lease_token"], 2.))
        self.assertFalse(self.db.fail(job, first["lease_token"], "late"))
        self.assertIsNone(self.db.claim("second", 1.))  # expired attempt enters bounded backoff
        self.clock.advance(.1); second = self.db.claim("second", 1.)
        self.assertNotEqual(first["lease_token"], second["lease_token"])
        self.assertFalse(self.db.complete(job, first["lease_token"], {"wrong": 2}))
        self.assertTrue(self.db.complete(job, second["lease_token"], {"correct": True}))
        self.assertFalse(self.db.complete(job, second["lease_token"], {"overwrite": True}))
        self.assertEqual(self.db.get(job)["result"], {"correct": True})
        self.assertEqual(self.db.metrics()["committed_results"], 1)

    def test_heartbeat_extends_only_current_attempt(self):
        job = self.submit(); claim = self.db.claim("worker", 1.)
        self.clock.advance(.7); self.assertTrue(self.db.heartbeat(job, claim["lease_token"], 1.))
        self.clock.advance(.6); self.assertIsNone(self.db.claim("other"))
        self.assertTrue(self.db.complete(job, claim["lease_token"], {"ok": True}))

    def test_retry_budget_dead_letter_and_deterministic_backoff(self):
        job = self.submit(attempts=2); claim = self.db.claim("one")
        self.assertTrue(self.db.fail(job, claim["lease_token"], "temporary"))
        self.assertEqual(self.db.get(job)["state"], "retry_wait")
        self.assertIsNone(self.db.claim("two"))
        self.clock.advance(.1); claim = self.db.claim("two")
        self.db.fail(job, claim["lease_token"], "again")
        self.assertEqual(self.db.get(job)["state"], "dead")
        self.clock.advance(100); self.assertIsNone(self.db.claim("three"))
        self.assertEqual(self.db.backoff(job, 1), self.db.backoff(job, 1))
        self.assertLessEqual(self.db.backoff(job, 10), 75.)

    def test_expiry_on_last_attempt_becomes_dead(self):
        job = self.submit(attempts=1); self.db.claim("gone", 1.)
        self.clock.advance(2); self.db.claim("sweeper")
        self.assertEqual(self.db.get(job)["state"], "dead")
        self.assertEqual(self.db.get(job)["attempts"][0]["state"], "lease_expired")

    def test_reopen_preserves_committed_result_and_audit(self):
        job = self.submit(); claim = self.db.claim("worker")
        self.db.complete(job, claim["lease_token"], evaluate(fixture()))
        fresh = Database(self.path, self.clock)
        self.assertEqual(fresh.get(job)["result"], self.db.get(job)["result"])
        self.assertEqual([e["kind"] for e in fresh.get(job)["events"]], ["submitted", "claimed", "completed"])
        self.assertNotIn("lease_token", fresh.get(job))
        self.assertNotIn("token", fresh.get(job)["attempts"][0])

    def test_nonfinite_result_rolls_back_without_state_change(self):
        job = self.submit(); claim = self.db.claim("worker")
        with self.assertRaises(ValueError): self.db.complete(job, claim["lease_token"], {"nan": float("nan")})
        self.assertEqual(self.db.get(job)["state"], "running")
        self.assertEqual(self.db.metrics()["committed_results"], 0)

class EvaluationTests(unittest.TestCase):
    def test_hand_calculated_metrics(self):
        result = evaluate(fixture())
        self.assertEqual(result["metrics"]["hit_at_3"], 1.)
        self.assertAlmostEqual(result["metrics"]["mrr"], .875)
        self.assertEqual(result["metrics"]["invalid_citation_rate"], 0.)
        self.assertIsNone(result["metrics"]["mean_observed_model_latency_ms"])

    def test_regression_guard_same_benchmark_not_changed_reference(self):
        baseline, candidate = fixture(), fixture("regressed")
        self.assertEqual(benchmark_fingerprint(baseline), benchmark_fingerprint(candidate))
        comparison = compare(evaluate(baseline), evaluate(candidate))
        self.assertFalse(comparison["passed"])
        self.assertEqual(len(comparison["failures"]), 2)
        candidate["queries"][0]["question"] = "changed question"
        with self.assertRaises(ValueError): compare(evaluate(baseline), evaluate(candidate))
        self.assertTrue(compare(evaluate(baseline), evaluate(baseline))["passed"])

    def test_reference_order_does_not_change_benchmark(self):
        a, b = fixture(), fixture(); b["queries"].reverse(); b["corpus_ids"].reverse()
        self.assertEqual(benchmark_fingerprint(a), benchmark_fingerprint(b))

    def test_schema_bounds_and_nonfinite_inputs(self):
        mutators = [lambda p: p.update(type="code"), lambda p: p.update(work_ms=6000),
                    lambda p: p.update(provenance="real-unverified"),
                    lambda p: p["queries"][0].update(observed_latency_ms=float("inf")),
                    lambda p: p["queries"][0].update(relevant_ids=["missing"]),
                    lambda p: p["queries"][0].update(retrieved_ids=["a", "a"]),
                    lambda p: p.update(queries=[]), lambda p: p.update(queries=p["queries"] * 300)]
        for mutate in mutators:
            payload = fixture(); mutate(payload)
            with self.subTest(payload=str(payload)[:100]):
                with self.assertRaises(ValueError): validate(payload)

class IntegrationTests(unittest.TestCase):
    def test_actual_process_crash_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.sqlite"; db = Database(path)
            job = db.submit("crash-demo", fixture())[0]["id"]
            command = [sys.executable, "-m", "runledger", "--db", str(path), "worker"]
            killed = subprocess.run(command + ["--once", "--crash-after-claim", "--lease-seconds", ".15"], cwd=ROOT, timeout=10, capture_output=True)
            self.assertEqual(killed.returncode, 73)
            recovered = subprocess.run(command + ["--max-jobs", "1", "--lease-seconds", "1"], cwd=ROOT, timeout=10, capture_output=True)
            self.assertEqual(recovered.returncode, 0, recovered.stderr.decode())
            result = db.get(job)
            self.assertEqual(result["state"], "succeeded")
            self.assertEqual([a["state"] for a in result["attempts"]], ["lease_expired", "succeeded"])
            self.assertEqual(db.metrics()["committed_results"], 1)

    def test_heartbeat_during_work(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / "jobs.sqlite")
            payload = fixture(); payload["work_ms"] = 350
            job = db.submit("slow", payload)[0]["id"]
            self.assertTrue(run_one(db, lease_seconds=.2))
            result = db.get(job)
            self.assertEqual(result["state"], "succeeded")
            self.assertGreaterEqual(sum(e["kind"] == "heartbeat" for e in result["events"]), 2)

    def test_http_idempotency_conflict_oversize_origin_and_result(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory) / "jobs.sqlite"); server = Server(db, ("127.0.0.1", 0))
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            url = f"http://127.0.0.1:{server.server_port}"
            def post(payload, **headers):
                req = Request(url + "/api/jobs", data=json.dumps({"payload": payload}).encode(), headers={"Content-Type": "application/json", "Idempotency-Key": "same", **headers})
                with urlopen(req, timeout=3) as response: return response.status, json.load(response)
            try:
                self.assertEqual(urlopen(url, timeout=3).status, 200)
                status, result = post(fixture()); self.assertEqual(status, 201)
                self.assertEqual(post(fixture())[0], 200)
                job = result["job"]["id"]
                with self.assertRaises(HTTPError) as error: post(fixture("regressed"))
                self.assertEqual(error.exception.code, 409)
                with self.assertRaises(HTTPError) as error: post(fixture(), Origin="https://example.com")
                self.assertEqual(error.exception.code, 403)
                oversized = Request(url + "/api/jobs", data=b"x" * 1000001, headers={"Content-Type": "application/json", "Idempotency-Key": "large"})
                with self.assertRaises(HTTPError) as error: urlopen(oversized, timeout=3)
                self.assertEqual(error.exception.code, 413)
                run_one(db)
                with urlopen(url + "/api/jobs/" + job, timeout=3) as response:
                    self.assertEqual(json.load(response)["state"], "succeeded")
                with urlopen(url + f"/api/compare?baseline={job}&candidate={job}", timeout=3) as response:
                    self.assertTrue(json.load(response)["passed"])
            finally:
                server.shutdown(); server.server_close(); thread.join(timeout=3)

if __name__ == "__main__": unittest.main()
