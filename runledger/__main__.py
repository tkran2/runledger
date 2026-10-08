import argparse
import json
from pathlib import Path
import subprocess
import sys
from .db import Database
from .evaluation import compare, digest
from .web import ROOT, Server
from .worker import loop

def main():
    p = argparse.ArgumentParser(description="Durable background jobs and evaluation replay")
    p.add_argument("--db", default="data/runledger.sqlite")
    sub = p.add_subparsers(dest="command", required=True)
    for name in ("serve", "demo"):
        s = sub.add_parser(name); s.add_argument("--port", type=int, default=8765)
    worker = sub.add_parser("worker"); worker.add_argument("--once", action="store_true"); worker.add_argument("--crash-after-claim", action="store_true"); worker.add_argument("--lease-seconds", type=float, default=10); worker.add_argument("--max-jobs", type=int)
    submit = sub.add_parser("submit"); submit.add_argument("file"); submit.add_argument("--key", required=True)
    result = sub.add_parser("result"); result.add_argument("job")
    args = p.parse_args(); db = Database(args.db)
    if args.command == "submit":
        print(json.dumps(db.submit(args.key, json.loads(Path(args.file).read_text()))[0], indent=2)); return 0
    if args.command == "result": print(json.dumps(db.get(args.job), indent=2)); return 0
    if args.command == "worker": loop(db, args.lease_seconds, args.once, args.crash_after_claim, args.max_jobs); return 0
    server = Server(db, ("127.0.0.1", args.port)); children = []
    if args.command == "demo":
        for name in ("baseline", "regressed"):
            payload = json.loads((ROOT / "examples" / f"{name}.json").read_text())
            db.submit(f"demo-{name}-{digest(payload)[:12]}", payload)
        for _ in range(2):
            children.append(subprocess.Popen([sys.executable, "-m", "runledger", "--db", db.path, "worker"], cwd=ROOT))
    print(f"RunLedger: http://127.0.0.1:{server.server_port} · Ctrl-C to stop", flush=True)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally:
        server.server_close()
        for child in children: child.terminate()
        for child in children:
            try: child.wait(timeout=3)
            except subprocess.TimeoutExpired: child.kill(); child.wait()
    return 0

if __name__ == "__main__": sys.exit(main())
