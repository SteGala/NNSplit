from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
from acbba.utils import *


class ACBBAServer:
    class MyRequestHandler(BaseHTTPRequestHandler):
        def __init__(self, request, client_address, server, queue):
            self.queue = queue
            super().__init__(request, client_address, server)

        def _set_response(self, status_code=200, content_type='application/json'):
            self.send_response(status_code)
            self.send_header('Content-type', content_type)
            self.end_headers()

        def do_POST(self):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(post_data)

            data["timestamp"] = string_list_to_datetime_list(data["timestamp"])

            self.queue.put(data)

            self._set_response()
            response = {'message': 'Data received successfully'}
            self.wfile.write(json.dumps(response).encode('utf-8'))

    def __init__(self, queue, port) -> None:
        self.port = port
        self.queue = queue

    def run_server(self, server_class=HTTPServer, handler_class=MyRequestHandler):
        server_address = ('', self.port)
        httpd = server_class(server_address, lambda *args, **
                             kwargs: handler_class(*args, **kwargs, queue=self.queue))
        threading.Thread(target=httpd.serve_forever).start()
