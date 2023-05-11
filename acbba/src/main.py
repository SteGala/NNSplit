import http.server
import socketserver
import socket
import threading 
import time
import requests
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            my_hostname = socket.gethostname()
            my_ip_address = socket.gethostbyname(my_hostname)
            message = f'My Hostname: {my_hostname}\nMy IP Address: {my_ip_address}\n'
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(bytes(message, "utf8"))
        except socket.gaierror:
            self.send_error(500, 'Failed to resolve hostname\n')

if __name__ == '__main__':
    PORT = 8080
    httpd = socketserver.TCPServer(("", PORT), Handler)

    print(f"Serving on port {PORT}")
    httpd_thread = threading.Thread(target=httpd.serve_forever)
    httpd_thread.start()

    ips = []
    success = False

    while not success:
        try:
        # Look up the IP addresses of all pods running the headless service
            _, _, ips = socket.gethostbyname_ex('acbba')
            success = True
        except:
            logging.info("There is no service acbba. Retry after 10 seconds")
            time.sleep(10)
            pass

    # Connect to each pod using its IP address
    for ip in ips:
        # Connect to the pod at IP address 'ip'
        # For example, send an HTTP request to the pod:
        response = requests.get(f"http://{ip}:" + str(PORT) + "/")
        logging.info(f"Pod at IP address {ip} responded with status code {response.status_code}")

    time.sleep(1000)
