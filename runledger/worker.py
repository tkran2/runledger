import json
import os
import signal
import threading
import time
import uuid
from .evaluation import evaluate

def run_one(db, owner=None, lease_seconds=10., crash_after_claim=False):
    owner = owner or f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    claim = db.claim(owner, lease_seconds)
    if not claim: return False
    if crash_after_claim: os._exit(73)  # deliberate crash demo: no cleanup, no graceful release
    stop = threading.Event()
    def renew():
        while not stop.wait(max(.01, lease_seconds / 3)):
            if not db.heartbeat(claim["id"], claim["lease_token"], lease_seconds): break
    heartbeat = threading.Thread(target=renew, daemon=True); heartbeat.start()
    began = time.monotonic()
    try:
        time.sleep(claim["payload"].get("work_ms", 0) / 1000)
        result = evaluate(claim["payload"])
        result["worker_elapsed_ms"] = (time.monotonic() - began) * 1000
        committed = db.complete(claim["id"], claim["lease_token"], result)
        print(json.dumps({"job": claim["id"], "attempt": claim["attempt"], "committed": committed}), flush=True)
    except Exception as error:
        db.fail(claim["id"], claim["lease_token"], f"{type(error).__name__}: {error}")
    finally:
        stop.set(); heartbeat.join(timeout=2)
    return True

def loop(db, lease_seconds=10., once=False, crash_after_claim=False, max_jobs=None):
    stopped = threading.Event(); processed = 0
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    owner = f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    while not stopped.is_set():
        handled = run_one(db, owner, lease_seconds, crash_after_claim)
        processed += int(handled)
        if once or (max_jobs is not None and processed >= max_jobs): break
        if not handled: stopped.wait(.1)
    return processed
