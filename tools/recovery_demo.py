"""Actual subprocess crash + four workers. Writes a reproducible outcome report."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runledger.db import Database

def run(output, count=40):
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    path = (output / "recovery.sqlite").resolve()
    if path.exists(): raise ValueError("choose a fresh output directory to avoid mixing runs")
    db = Database(path)
    fixture = json.loads((ROOT / "examples" / "baseline.json").read_text())
    fixture["work_ms"] = 50
    jobs = [db.submit(f"load-{i}", {**fixture, "name": f"Replay {i}"})[0]["id"] for i in range(count)]
    command = [sys.executable, "-m", "runledger", "--db", str(path), "worker"]
    crashed = subprocess.run(command + ["--once", "--crash-after-claim", "--lease-seconds", ".15"], cwd=ROOT, timeout=10, capture_output=True)
    if crashed.returncode != 73: raise RuntimeError("crash injection did not occur")
    per_worker = [count // 4 + (i < count % 4) for i in range(4)]
    started = time.monotonic()
    workers = [subprocess.Popen(command + ["--max-jobs", str(n), "--lease-seconds", "1"], cwd=ROOT,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE) for n in per_worker if n]
    try:
        for worker in workers:
            _, error = worker.communicate(timeout=30)
            if worker.returncode: raise RuntimeError(error.decode())
    finally:
        for worker in workers:
            if worker.poll() is None: worker.kill(); worker.wait()
    elapsed = time.monotonic() - started
    records = [db.get(job) for job in jobs]
    metrics = db.metrics()
    if metrics["committed_results"] != count or metrics["states"] != {"succeeded": count}:
        raise RuntimeError("jobs were lost or not completed")
    report = {"scenario": "one os._exit(73) crash after claim, then four local worker processes",
              "jobs": count, "workers": len(workers), "work_ms_per_attempt": 50,
              "crash_exit_code": crashed.returncode, "recovery_elapsed_seconds": elapsed,
              "metrics": metrics, "retry_attempts": [record for record in records if record["attempt"] > 1],
              "python": platform.python_version(), "platform": platform.system(),
              "guarantee": "one committed result per job; task execution is at least once",
              "scope": "single-host SQLite recovery demo, not a distributed load or model-quality benchmark"}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k:v for k,v in report.items() if k != "retry_attempts"}, indent=2))

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--output", default="reports/recovery-demo"); p.add_argument("--jobs", type=int, default=40); a = p.parse_args()
    if not 4 <= a.jobs <= 1000: p.error("jobs must be 4-1000")
    run(a.output, a.jobs)
