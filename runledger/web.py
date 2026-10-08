from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from .db import Conflict
from .evaluation import compare

ROOT = Path(__file__).resolve().parents[1]

class Server(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, db, address=("127.0.0.1", 8765)):
        self.db = db
        super().__init__(address, Handler)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def send(self, status, value, content_type="application/json"):
        body = (json.dumps(value, allow_nan=False) if content_type == "application/json" else value).encode()
        self.send_response(status); self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff"); self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path)
        try:
            if path.path == "/": return self.send(200, (ROOT / "static" / "index.html").read_text(), "text/html; charset=utf-8")
            if path.path == "/api/jobs": return self.send(200, {"jobs": self.server.db.list()})
            if path.path == "/api/metrics": return self.send(200, self.server.db.metrics())
            if path.path.startswith("/api/jobs/"): return self.send(200, self.server.db.get(path.path.removeprefix("/api/jobs/")))
            if path.path == "/api/compare":
                query = parse_qs(path.query)
                before = self.server.db.get(query["baseline"][0])["result"]
                after = self.server.db.get(query["candidate"][0])["result"]
                if before is None or after is None: raise ValueError("both runs must have completed")
                return self.send(200, compare(before, after))
            if path.path in ("/api/examples/baseline", "/api/examples/regressed"):
                return self.send(200, json.loads((ROOT / "examples" / (path.path.rsplit("/", 1)[1] + ".json")).read_text()))
            return self.send(404, {"error": "route not found"})
        except KeyError: return self.send(404, {"error": "job or query parameter not found"})
        except ValueError as error: return self.send(400, {"error": str(error)})

    def do_POST(self):
        if self.path != "/api/jobs": return self.send(404, {"error": "route not found"})
        # Local demo only. JSON plus same-origin checking prevents drive-by browser form submissions.
        host = urlparse("http://" + self.headers.get("Host", "")).hostname
        origin = self.headers.get("Origin")
        if host not in ("localhost", "127.0.0.1") or (origin and urlparse(origin).netloc != self.headers.get("Host")):
            return self.send(403, {"error": "local same-origin request required"})
        if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
            return self.send(415, {"error": "application/json required"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 1_000_000: return self.send(413, {"error": "body must be 1-1000000 bytes"})
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict) or set(body) - {"payload", "max_attempts"}: raise ValueError("expected payload and optional max_attempts")
            job, created = self.server.db.submit(self.headers.get("Idempotency-Key"), body.get("payload"), body.get("max_attempts", 3))
            return self.send(201 if created else 200, {"job": job, "created": created})
        except Conflict as error: return self.send(409, {"error": str(error)})
        except (ValueError, UnicodeError) as error: return self.send(400, {"error": str(error)})
