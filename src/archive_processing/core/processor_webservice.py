import json
import os
import signal
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs


class ProcessorHTTPServer(HTTPServer):
    def __init__(self, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)
        self.context = None


class ProcessorHTTPGetHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # This is the path (e.g. /status) but with no parameters
        parsed_path = urlparse(self.path.lower()).path

        # Check for ending /'s and remove them all
        while parsed_path[-1] == "/":
            parsed_path = parsed_path[:-1]

        # This is a dictionary of key value pairs of all parameters
        parameter_list = parse_qs(urlparse(self.path.lower()).query)

        try:
            if parsed_path == "/status":
                data = json.dumps(self.server.context.get_status())

                self.send_response(200)
                self.end_headers()
                self.wfile.write(data.encode())

            elif parsed_path == "/refresh_obs_list":
                self.server.context.refresh_obs_list()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK, refreshing")
            elif parsed_path == "/stop":
                os.kill(os.getpid(), signal.SIGINT)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK, stopping")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(
                    f"Unknown command {parsed_path}".encode("utf-8")
                )

        except Exception as e:
            self.server.context.logger.error(f"GET: Error {str(e)}")
            self.send_response(400)
            self.end_headers()
