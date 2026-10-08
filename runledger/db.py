"""SQLite transitions use one write transaction; fenced leases protect committed results."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import uuid
from .evaluation import canonical, digest, validate

class Conflict(ValueError): pass

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL, payload TEXT NOT NULL,
 payload_sha256 TEXT NOT NULL, state TEXT NOT NULL,
 attempt INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
 lease_owner TEXT, lease_token TEXT, lease_until REAL,
 next_run REAL NOT NULL, created REAL NOT NULL, updated REAL NOT NULL, last_error TEXT
);
CREATE INDEX IF NOT EXISTS ready_jobs ON jobs(state, next_run, created);
CREATE TABLE IF NOT EXISTS attempts (
 id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id), attempt INTEGER NOT NULL,
 owner TEXT NOT NULL, token TEXT NOT NULL, started REAL NOT NULL, finished REAL,
 state TEXT NOT NULL, error TEXT, UNIQUE(job_id,attempt)
);
CREATE TABLE IF NOT EXISTS results (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id), result TEXT NOT NULL,
 sha256 TEXT NOT NULL, created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
 kind TEXT NOT NULL, timestamp REAL NOT NULL, detail TEXT NOT NULL
);
"""

class Database:
    def __init__(self, path, clock=time.time, base_backoff=.2):
        self.path = str(Path(path).resolve()); self.clock = clock; self.base_backoff = base_backoff
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript(SCHEMA)

    @contextmanager
    def connection(self):
        con = sqlite3.connect(self.path, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON"); con.execute("PRAGMA synchronous=FULL")
        try: yield con
        finally: con.close()

    @contextmanager
    def transaction(self):
        with self.connection() as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                yield con
                con.commit()
            except BaseException:
                con.rollback(); raise

    def event(self, con, job, kind, now, detail=None):
        con.execute("INSERT INTO events(job_id,kind,timestamp,detail) VALUES(?,?,?,?)",
                    (job, kind, now, canonical(detail or {})))

    def backoff(self, job, attempt):
        jitter = int(hashlib.sha256(f"{job}:{attempt}".encode()).hexdigest()[:8], 16) / 0xffffffff
        return min(60., self.base_backoff * 2**(attempt - 1) * (1 + .25 * jitter))

    def public(self, row):
        obj = dict(row)
        for key in ("lease_token", "payload"): obj.pop(key, None)
        return obj

    def submit(self, key, payload, max_attempts=3):
        if not isinstance(key, str) or not key or len(key) > 128: raise ValueError("Idempotency-Key must have 1-128 characters")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 10: raise ValueError("max_attempts must be 1-10")
        validate(payload)
        encoded = canonical(payload)
        if len(encoded.encode()) > 1_000_000: raise ValueError("payload exceeds 1MB")
        payload_hash = digest({"payload": payload, "max_attempts": max_attempts})
        with self.transaction() as con:
            old = con.execute("SELECT * FROM jobs WHERE idempotency_key=?", (key,)).fetchone()
            if old:
                if old["payload_sha256"] != payload_hash: raise Conflict("same idempotency key used with a different payload or attempt limit")
                return self.public(old), False
            now = self.clock(); job = uuid.uuid4().hex
            con.execute("INSERT INTO jobs(id,idempotency_key,payload,payload_sha256,state,max_attempts,next_run,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                        (job, key, encoded, payload_hash, "queued", max_attempts, now, now, now))
            self.event(con, job, "submitted", now)
            return self.public(con.execute("SELECT * FROM jobs WHERE id=?", (job,)).fetchone()), True

    def reap(self, con, now):
        expired = con.execute("SELECT * FROM jobs WHERE state='running' AND lease_until<=?", (now,)).fetchall()
        for row in expired:
            state = "dead" if row["attempt"] >= row["max_attempts"] else "retry_wait"
            next_run = now + self.backoff(row["id"], row["attempt"])
            con.execute("UPDATE attempts SET state='lease_expired',finished=?,error='lease expired' WHERE job_id=? AND attempt=?",
                        (now, row["id"], row["attempt"]))
            con.execute("UPDATE jobs SET state=?,next_run=?,updated=?,last_error='lease expired',lease_owner=NULL,lease_token=NULL,lease_until=NULL WHERE id=?",
                        (state, next_run, now, row["id"]))
            self.event(con, row["id"], "lease_expired", now, {"attempt": row["attempt"], "next_state": state})

    def claim(self, owner, lease_seconds=10.):
        if not isinstance(owner, str) or not owner or len(owner) > 128: raise ValueError("invalid owner")
        if not 0 < lease_seconds <= 3600: raise ValueError("lease_seconds must be in (0,3600]")
        with self.transaction() as con:
            now = self.clock(); self.reap(con, now)
            row = con.execute("SELECT * FROM jobs WHERE state IN ('queued','retry_wait') AND next_run<=? ORDER BY next_run,created,id LIMIT 1", (now,)).fetchone()
            if not row: return None
            token = uuid.uuid4().hex; attempt = row["attempt"] + 1
            con.execute("UPDATE jobs SET state='running',attempt=?,lease_owner=?,lease_token=?,lease_until=?,updated=? WHERE id=?",
                        (attempt, owner, token, now + lease_seconds, now, row["id"]))
            con.execute("INSERT INTO attempts(job_id,attempt,owner,token,started,state) VALUES(?,?,?,?,?,'running')",
                        (row["id"], attempt, owner, token, now))
            self.event(con, row["id"], "claimed", now, {"attempt": attempt, "owner": owner})
            return {**dict(con.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()), "payload": json.loads(row["payload"])}

    def authorized(self, con, job, token, now):
        return con.execute("SELECT * FROM jobs WHERE id=? AND state='running' AND lease_token=? AND lease_until>?",
                           (job, token, now)).fetchone()

    def heartbeat(self, job, token, lease_seconds):
        if not 0 < lease_seconds <= 3600: raise ValueError("invalid lease duration")
        with self.transaction() as con:
            now = self.clock()
            if not self.authorized(con, job, token, now): return False
            con.execute("UPDATE jobs SET lease_until=?,updated=? WHERE id=?", (now + lease_seconds, now, job))
            self.event(con, job, "heartbeat", now)
            return True

    def complete(self, job, token, result):
        encoded = canonical(result)
        with self.transaction() as con:
            now = self.clock(); row = self.authorized(con, job, token, now)
            if not row: return False
            # Result and successful transition share the same atomic commit.
            con.execute("INSERT INTO results(job_id,result,sha256,created) VALUES(?,?,?,?)", (job, encoded, digest(result), now))
            con.execute("UPDATE jobs SET state='succeeded',updated=?,lease_owner=NULL,lease_token=NULL,lease_until=NULL,last_error=NULL WHERE id=?", (now, job))
            con.execute("UPDATE attempts SET state='succeeded',finished=? WHERE job_id=? AND attempt=?", (now, job, row["attempt"]))
            self.event(con, job, "completed", now, {"attempt": row["attempt"]})
            return True

    def fail(self, job, token, error):
        error = str(error)[:2000]
        with self.transaction() as con:
            now = self.clock(); row = self.authorized(con, job, token, now)
            if not row: return False
            state = "dead" if row["attempt"] >= row["max_attempts"] else "retry_wait"
            con.execute("UPDATE jobs SET state=?,updated=?,next_run=?,last_error=?,lease_owner=NULL,lease_token=NULL,lease_until=NULL WHERE id=?",
                        (state, now, now + self.backoff(job, row["attempt"]), error, job))
            con.execute("UPDATE attempts SET state='failed',finished=?,error=? WHERE job_id=? AND attempt=?", (now, error, job, row["attempt"]))
            self.event(con, job, "failed", now, {"attempt": row["attempt"], "next_state": state, "error": error})
            return True

    def get(self, job):
        with self.connection() as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job,)).fetchone()
            if not row: raise KeyError(job)
            result = con.execute("SELECT result,sha256 FROM results WHERE job_id=?", (job,)).fetchone()
            return {**self.public(row), "result": json.loads(result["result"]) if result else None,
                    "result_sha256": result["sha256"] if result else None,
                    "attempts": [{k: v for k, v in dict(a).items() if k != "token"} for a in con.execute("SELECT * FROM attempts WHERE job_id=? ORDER BY attempt", (job,))],
                    "events": [{**dict(e), "detail": json.loads(e["detail"])} for e in con.execute("SELECT * FROM events WHERE job_id=? ORDER BY id", (job,))]}

    def list(self):
        with self.connection() as con:
            return [self.public(row) for row in con.execute("SELECT * FROM jobs ORDER BY created DESC LIMIT 100")]

    def metrics(self):
        with self.connection() as con:
            return {"states": {row[0]: row[1] for row in con.execute("SELECT state,COUNT(*) FROM jobs GROUP BY state")},
                    "attempts": con.execute("SELECT COUNT(*) FROM attempts").fetchone()[0],
                    "committed_results": con.execute("SELECT COUNT(*) FROM results").fetchone()[0],
                    "lease_expirations": con.execute("SELECT COUNT(*) FROM events WHERE kind='lease_expired'").fetchone()[0]}
