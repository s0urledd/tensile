#!/usr/bin/env python3
"""
fake-http: answer every request with one status code and one body. A test
double for the observer API in deploy/test/selftest.sh, and, with --record,
for an alert webhook: every POST body is appended to the file as one line
before the answer goes out, so a caller that got its answer can count them.

  fake-http.py --port P --code 503 --body file.json [--record posts.log [--record-path]]

--record-path puts each post's path before its body on the line (a bot API
takes its token in the path).
"""
import argparse
import http.server
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--code", type=int, default=200)
    ap.add_argument("--body", default=None)
    ap.add_argument("--record", default=None)
    ap.add_argument("--record-path", action="store_true")
    a = ap.parse_args()
    body = open(a.body, "rb").read() if a.body else b"{}"

    class H(http.server.BaseHTTPRequestHandler):
        def answer(self):
            self.send_response(a.code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.answer()

        def do_POST(self):
            n = int(self.headers.get("content-length") or 0)
            got = self.rfile.read(n) if n else b""
            if a.record:
                with open(a.record, "ab") as f:
                    path = (self.path + " ").encode() if a.record_path else b""
                    f.write(path + got.replace(b"\n", b" ") + b"\n")
            self.answer()

        def log_message(self, *args):
            pass

    http.server.ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


if __name__ == "__main__":
    sys.exit(main())
