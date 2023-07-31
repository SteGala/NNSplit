import http.server
import socket
import threading
import socketserver


class ConnectionHandlerCallback(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            #my_hostname = socket.gethostname()
            #my_ip_address = socket.gethostbyname(my_hostname)
            message = f'Ping\n'
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(bytes(message, "utf8"))
        except socket.gaierror:
            self.send_error(500, 'Failed to resolve hostname\n')

    def log_message(self, format, *args):
        return


class ConnectionHandlerServer():
    def __init__(self, port) -> None:
        httpd = socketserver.TCPServer(("", port), ConnectionHandlerCallback)

        print(f"Serving on port {port}")
        httpd_thread = threading.Thread(target=httpd.serve_forever)
        httpd_thread.start()
