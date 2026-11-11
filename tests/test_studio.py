import http.server
import json
import threading

from imx519_edge.studio import Studio


def test_upload_streams_pieces_with_length():
    got = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            got["length"] = self.headers.get("Content-Length")
            got["chunked"] = self.headers.get("Transfer-Encoding")
            if got["length"]:
                got["body"] = self.rfile.read(int(got["length"]))
            else:  # chunked
                body = b""
                while size := int(self.rfile.readline(), 16):
                    body += self.rfile.read(size)
                    self.rfile.readline()
                self.rfile.readline()
                got["body"] = body
            out = json.dumps({"upload_id": "u20261018-141500-abc123"}).encode()
            self.send_response(201)
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        studio = Studio(f"http://127.0.0.1:{server.server_port}")
        assert studio.upload(iter([b"ab", b"cd"]), "a.DNG", 4)["upload_id"]
        assert got == {"length": "4", "chunked": None, "body": b"abcd"}
        studio.upload(iter([b"ab", b"cde"]), "a.DNG")
        assert got["chunked"] == "chunked" and got["body"] == b"abcde"
    finally:
        server.shutdown()
