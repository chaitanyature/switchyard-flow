import json
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from switchyard import Engine, LostLease
from switchyard.worker import HANDLERS, execute

ROOT = Path(__file__).resolve().parents[1]


def job(needs=(), **options):
    return {"kind": "test", "needs": list(needs), **options}


def claim_and_exit(path, pipe):
    lease = Engine(path).claim("doomed", ttl=0.25)
    pipe.send((lease.name, lease.token))
    pipe.close()
    os._exit(17)


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "engine.db"
        self.now = 10.0
        self.db = Engine(self.path, clock=lambda: self.now)

    def test_idempotent_concurrent_submission(self):
        spec = {"jobs": {"a": job()}}
        with ThreadPoolExecutor(max_workers=8) as pool:
            created = list(pool.map(lambda _: self.db.submit("x", spec), range(16)))
        self.assertEqual(sum(created), 1)
        with self.assertRaises(ValueError):
            self.db.submit("x", {"jobs": {"changed": job()}})
        self.assertEqual(len(self.db.events("x")), 1)

    def test_validation_rolls_back(self):
        for spec in [
            {"a": job(["b"])},
            {"a": job(["b"]), "b": job(["a"])},
            {"a": job(max_attempts=True)},
            {"a": job(["a", "a"])},
            {"a": job(missing=17)},
        ]:
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                self.db.submit("x", {"jobs": spec})
        self.assertEqual(self.db.events("x"), [])
        self.assertTrue(self.db.submit("x", {"jobs": {"a": job()}}))

    def test_workflow_join_and_reopen(self):
        self.db.submit("x", json.loads((ROOT / "examples/word-count.json").read_text()))
        self.db.set_capacity("readers", 2)
        leaves = [self.db.claim("w1"), self.db.claim("w2")]
        self.assertIsNone(self.db.claim("w3"))
        for lease in leaves:
            execute(self.db, lease, HANDLERS)
        reopened = Engine(self.path, clock=lambda: self.now)
        merge = reopened.claim("w3")
        self.assertEqual(set(merge.inputs), {"east", "west"})
        execute(reopened, merge, HANDLERS)
        execute(reopened, reopened.claim("w3"), HANDLERS)
        report = reopened.report("x")
        self.assertEqual(report["state"], "succeeded")
        self.assertEqual(
            report["jobs"]["top-two"]["result"], [["blue", 3], ["ferry", 3]]
        )
        events = reopened.events("x")
        self.assertEqual(
            [r["id"] for r in reopened.events("x", events[-2]["id"])],
            [events[-1]["id"]],
        )

    def test_expiry_fences_all_mutations_and_retries(self):
        self.db.submit("x", {"jobs": {"a": job()}})
        first = self.db.claim("slow", 5)
        self.now += 5
        for mutate in (
            lambda: self.db.finish(first, "old"),
            lambda: self.db.fail(first, "old"),
            lambda: self.db.heartbeat(first, 2),
        ):
            with self.assertRaises(LostLease):
                mutate()
        self.assertIsNone(self.db.claim("reaper"))
        self.now += 1
        replacement = self.db.claim("fast")
        self.assertEqual(replacement.attempt, 2)
        self.assertNotEqual(first.token, replacement.token)
        with self.assertRaises(LostLease):
            self.db.finish(first, "late")
        self.db.finish(replacement, "ok")
        self.assertEqual(self.db.report("x")["jobs"]["a"]["result"], "ok")

    def test_global_capacity_under_contention(self):
        for n in range(18):
            self.db.submit(str(n), {"jobs": {"a": job(pool="api")}})
        self.db.set_capacity("api", 3)
        with ThreadPoolExecutor(max_workers=12) as workers:
            leases = list(workers.map(lambda n: self.db.claim(str(n)), range(30)))
        live = [lease for lease in leases if lease]
        self.assertEqual(len({lease.run_id for lease in live}), 3)
        self.assertEqual(len(live), 3)
        self.db.set_capacity("api", 1)
        for lease in live[:2]:
            self.db.finish(lease, None)
        self.assertIsNone(self.db.claim("limited"))
        self.db.finish(live[2], None)
        self.assertIsNotNone(self.db.claim("next"))

    def test_failure_cascade_keeps_independent_branch(self):
        self.db.submit(
            "x",
            {
                "jobs": {
                    "a": job(max_attempts=1),
                    "b": job(["a"]),
                    "c": job(["a", "b"]),
                    "z": job(),
                }
            },
        )
        self.db.fail(self.db.claim("w"), "unrecoverable")
        other = self.db.claim("w")
        self.assertEqual(other.name, "z")
        self.db.finish(other, "survived")
        report = self.db.report("x")
        self.assertEqual(report["state"], "failed")
        self.assertEqual(
            [report["jobs"][k]["state"] for k in "abcz"],
            ["failed", "skipped", "skipped", "succeeded"],
        )

    def test_retry_budget_survives_restart(self):
        self.db.submit("x", {"jobs": {"a": job(max_attempts=2)}})
        execute(self.db, self.db.claim("w"), {})  # unknown handler fails the attempt
        self.now += 1
        other = Engine(self.path, clock=lambda: self.now)
        last = other.claim("w", ttl=1)
        self.assertEqual(last.attempt, 2)
        self.now += 1
        self.assertIsNone(other.claim("reaper"))
        self.assertEqual(other.report("x")["state"], "failed")

    def test_heartbeat_moves_deadline(self):
        self.db.submit("x", {"jobs": {"a": job()}})
        lease = self.db.claim("w", 3)
        self.now += 2
        self.db.heartbeat(lease, 5)
        self.now += 4
        self.assertIsNone(self.db.claim("other"))
        self.db.finish(lease, 0)
        self.assertEqual(self.db.report("x")["jobs"]["a"]["result"], 0)

    def test_abrupt_worker_exit(self):
        real = Engine(self.path)
        real.submit("kill", {"jobs": {"a": job()}})
        ctx = multiprocessing.get_context("spawn")
        recv, send = ctx.Pipe(duplex=False)
        worker = ctx.Process(target=claim_and_exit, args=(self.path, send))
        worker.start()
        send.close()
        try:
            self.assertTrue(recv.poll(10))
            name, token = recv.recv()
            self.assertEqual(name, "a")
            worker.join(10)
            self.assertEqual(worker.exitcode, 17)
            deadline = time.monotonic() + 8
            next_attempt = None
            while time.monotonic() < deadline and next_attempt is None:
                next_attempt = real.claim("replacement")
                if next_attempt is None:
                    time.sleep(0.05)
            self.assertIsNotNone(next_attempt)
            self.assertEqual(next_attempt.attempt, 2)
            self.assertNotEqual(next_attempt.token, token)
            real.finish(next_attempt, "recovered")
        finally:
            recv.close()
            if worker.is_alive():
                worker.kill()
                worker.join()

    def test_cli_end_to_end(self):
        def run(*args):
            return subprocess.check_output(
                [sys.executable, "-m", "switchyard", self.path, *args],
                cwd=ROOT,
                text=True,
            )

        run("submit", "demo", str(ROOT / "examples/word-count.json"))
        run("work", "local", "--until-idle")
        report = json.loads(run("status", "demo"))
        self.assertEqual(report["state"], "succeeded")
        self.assertEqual(len(json.loads(run("events", "demo"))), 9)
