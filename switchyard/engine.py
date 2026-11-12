import json
import math
import secrets
import sqlite3
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class LostLease(RuntimeError):
    """The attempt expired or another attempt owns the job."""


@dataclass(frozen=True)
class Lease:
    run_id: str
    name: str
    kind: str
    payload: object
    inputs: dict
    attempt: int
    token: str


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def name_ok(value):
    return isinstance(value, str) and 0 < len(value) <= 160


def check_spec(spec):
    if not isinstance(spec, dict) or set(spec) != {"jobs"}:
        raise ValueError("expected an object containing jobs")
    jobs = spec["jobs"]
    if not isinstance(jobs, dict) or not jobs:
        raise ValueError("jobs must be a nonempty object")
    children = {name: [] for name in jobs}
    remaining = {}
    for name, job in jobs.items():
        if not name_ok(name) or not isinstance(job, dict):
            raise ValueError("invalid job definition")
        if set(job) - {"kind", "needs", "pool", "max_attempts", "payload"}:
            raise ValueError(f"{name}: unknown job field")
        if not name_ok(job.get("kind")) or not name_ok(job.get("pool", "default")):
            raise ValueError(f"{name}: invalid kind or pool")
        budget = job.get("max_attempts", 3)
        if type(budget) is not int or not 1 <= budget <= 32:
            raise ValueError(f"{name}: max_attempts must be 1..32")
        needs = job.get("needs", [])
        if not isinstance(needs, list) or any(
            not name_ok(p) or p not in jobs for p in needs
        ):
            raise ValueError(f"{name}: unknown or invalid dependency")
        if len(needs) != len(set(needs)):
            raise ValueError(f"{name}: duplicate dependency")
        remaining[name] = len(needs)
        for parent in needs:
            children[parent].append(name)
    queue = deque(name for name, count in remaining.items() if count == 0)
    visited = 0
    while queue:
        visited += 1
        for child in children[queue.popleft()]:
            remaining[child] -= 1
            if remaining[child] == 0:
                queue.append(child)
    if visited != len(jobs):
        raise ValueError("workflow contains a dependency cycle")
    return jobs


def positive_duration(value):
    if not math.isfinite(value) or value <= 0:
        raise ValueError("lease duration must be finite and positive")


class Engine:
    def __init__(self, database, clock=time.time):
        self.path = str(database)
        self.clock = clock
        if self.path == ":memory:":
            raise ValueError("supply a file-backed database")
        with self.connection() as db:
            db.executescript(Path(__file__).with_name("schema.sql").read_text())

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys = ON")
            db.execute("PRAGMA synchronous = FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def set_capacity(self, pool, size):
        if not name_ok(pool) or type(size) is not int or size < 1:
            raise ValueError("invalid pool name or capacity")
        with self.transaction() as db:
            db.execute(
                """INSERT INTO pools VALUES (?,?) ON CONFLICT(name)
                          DO UPDATE SET capacity=excluded.capacity""",
                (pool, size),
            )

    @staticmethod
    def event(db, run, job, kind, now, detail=None):
        db.execute(
            "INSERT INTO events(run_id,job,kind,at,detail) VALUES (?,?,?,?,?)",
            (run, job, kind, now, encode(detail)),
        )

    def submit(self, run, spec):
        """The run ID is an idempotency key; a different definition is rejected."""
        if not name_ok(run):
            raise ValueError("invalid run ID")
        jobs = check_spec(spec)
        canonical = encode(spec)
        with self.transaction() as db:
            existing = db.execute("SELECT spec FROM runs WHERE id=?", (run,)).fetchone()
            if existing:
                if existing["spec"] != canonical:
                    raise ValueError("run ID already bound to a different workflow")
                return False
            db.execute("INSERT INTO runs VALUES (?,?)", (run, canonical))
            for name, job in jobs.items():
                pool = job.get("pool", "default")
                db.execute("INSERT OR IGNORE INTO pools VALUES (?,1)", (pool,))
                db.execute(
                    """INSERT INTO jobs(run_id,name,kind,payload,pool,max_attempts)
                              VALUES (?,?,?,?,?,?)""",
                    (
                        run,
                        name,
                        job["kind"],
                        encode(job.get("payload")),
                        pool,
                        job.get("max_attempts", 3),
                    ),
                )
            for name, job in jobs.items():
                db.executemany(
                    "INSERT INTO edges VALUES (?,?,?)",
                    [(run, parent, name) for parent in job.get("needs", [])],
                )
            self.event(db, run, None, "submitted", self.clock())
        return True

    def _retry_or_fail(self, db, row, now, reason):
        run, name = row["run_id"], row["name"]
        terminal = row["attempt"] >= row["max_attempts"]
        state = "failed" if terminal else "waiting"
        delay = 0 if terminal else min(60, 2 ** (row["attempt"] - 1))
        db.execute(
            """UPDATE jobs SET state=?,ready_at=?,error=?,owner=NULL,token=NULL,
                      expires=NULL WHERE run_id=? AND name=?""",
            (state, now + delay, reason, run, name),
        )
        self.event(
            db,
            run,
            name,
            "failed" if terminal else "retry",
            now,
            {"reason": reason, "attempt": row["attempt"], "delay": delay},
        )
        if terminal:
            blocked = db.execute(
                """WITH RECURSIVE downstream(name) AS (
                SELECT child FROM edges WHERE run_id=? AND parent=?
                UNION SELECT e.child FROM edges e JOIN downstream d
                ON d.name=e.parent WHERE e.run_id=?
            ) SELECT name FROM jobs WHERE run_id=? AND state='waiting'
              AND name IN (SELECT name FROM downstream) ORDER BY name""",
                (run, name, run, run),
            ).fetchall()
            for child in blocked:
                db.execute(
                    "UPDATE jobs SET state='skipped' WHERE run_id=? AND name=?",
                    (run, child["name"]),
                )
                self.event(
                    db, run, child["name"], "skipped", now, {"failed_ancestor": name}
                )

    def claim(self, owner, ttl=30.0):
        positive_duration(ttl)
        if not name_ok(owner):
            raise ValueError("invalid worker ID")
        with self.transaction() as db:
            now = self.clock()
            for expired in db.execute(
                """SELECT * FROM jobs WHERE state='running'
                                         AND expires<=? ORDER BY run_id,name""",
                (now,),
            ).fetchall():
                self._retry_or_fail(db, expired, now, "lease expired")
            row = db.execute(
                """SELECT j.* FROM jobs j JOIN pools p ON p.name=j.pool
                WHERE j.state='waiting' AND j.ready_at<=?
                AND (SELECT count(*) FROM jobs live WHERE live.state='running'
                     AND live.pool=j.pool) < p.capacity
                AND NOT EXISTS (
                    SELECT 1 FROM edges e JOIN jobs dep
                    ON dep.run_id=e.run_id AND dep.name=e.parent
                    WHERE e.run_id=j.run_id AND e.child=j.name AND dep.state!='succeeded')
                ORDER BY j.ready_at,j.rowid LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None
            run, name = row["run_id"], row["name"]
            token = secrets.token_hex(16)
            attempt = row["attempt"] + 1
            db.execute(
                """UPDATE jobs SET state='running',attempt=?,owner=?,token=?,expires=?
                          WHERE run_id=? AND name=?""",
                (attempt, owner, token, now + ttl, run, name),
            )
            inputs = {
                r["name"]: json.loads(r["result"])
                for r in db.execute(
                    """SELECT j.name,j.result FROM edges e JOIN jobs j ON j.run_id=e.run_id
                   AND j.name=e.parent WHERE e.run_id=? AND e.child=?""",
                    (run, name),
                )
            }
            self.event(
                db, run, name, "claimed", now, {"owner": owner, "attempt": attempt}
            )
            return Lease(
                run,
                name,
                row["kind"],
                json.loads(row["payload"]),
                inputs,
                attempt,
                token,
            )

    @staticmethod
    def _owned(db, lease, now):
        row = db.execute(
            """SELECT * FROM jobs WHERE run_id=? AND name=? AND state='running'
                            AND token=? AND expires>?""",
            (lease.run_id, lease.name, lease.token, now),
        ).fetchone()
        if row is None:
            raise LostLease(
                f"{lease.run_id}/{lease.name} attempt {lease.attempt} expired"
            )
        return row

    def heartbeat(self, lease, ttl=30.0):
        positive_duration(ttl)
        with self.transaction() as db:
            now = self.clock()
            self._owned(db, lease, now)
            db.execute(
                "UPDATE jobs SET expires=? WHERE run_id=? AND name=?",
                (now + ttl, lease.run_id, lease.name),
            )

    def finish(self, lease, result):
        value = encode(result)
        with self.transaction() as db:
            now = self.clock()
            self._owned(db, lease, now)
            db.execute(
                """UPDATE jobs SET state='succeeded',result=?,error=NULL,owner=NULL,
                          token=NULL,expires=NULL WHERE run_id=? AND name=?""",
                (value, lease.run_id, lease.name),
            )
            self.event(
                db,
                lease.run_id,
                lease.name,
                "succeeded",
                now,
                {"attempt": lease.attempt},
            )

    def fail(self, lease, reason):
        with self.transaction() as db:
            now = self.clock()
            self._retry_or_fail(db, self._owned(db, lease, now), now, str(reason))

    def report(self, run):
        with self.connection() as db:
            db.execute("BEGIN")
            rows = db.execute(
                """SELECT name,state,attempt,result,error FROM jobs
                                 WHERE run_id=? ORDER BY name""",
                (run,),
            ).fetchall()
            if not rows:
                raise KeyError(run)
            jobs = {
                r["name"]: {
                    **dict(r),
                    "result": json.loads(r["result"]) if r["result"] else None,
                }
                for r in rows
            }
            states = {r["state"] for r in rows}
            active = bool(states & {"waiting", "running"})
            status = (
                "active"
                if active
                else ("failed" if "failed" in states else "succeeded")
            )
            return {"run": run, "state": status, "jobs": jobs}

    def events(self, run, after=0):
        with self.connection() as db:
            return [
                {**dict(row), "detail": json.loads(row["detail"])}
                for row in db.execute(
                    "SELECT * FROM events WHERE run_id=? AND id>? ORDER BY id",
                    (run, after),
                )
            ]
