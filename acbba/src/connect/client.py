import socket
import time
import logging
import requests
import threading


class ConnectionHandlerClient:
    def __init__(self, port, hostname, frequency) -> None:
        self.__ips = []
        self.__ips_lock = threading.Lock()
        self.__container_ip = self.__get_container_ip()
        logging.info(f"Container IP: {self.__container_ip}")

        self.__client_thread = threading.Thread(
            target=self.__scan_for_hosts, args=[port, hostname, frequency])
        self.__client_thread.start()

    def __scan_for_hosts(self, port, hostname, frequency):
        while True:
            success = False

            while not success:
                try:
                    # Look up the IP addresses of all pods running the headless service
                    _, _, ips = socket.gethostbyname_ex(hostname)
                    success = True
                except:
                    logging.info(
                        f"There is no service {hostname}. Retry after 10 seconds")
                    time.sleep(10)
                    pass

            with self.__ips_lock:
                # Clean the list of ips
                self.__ips = []

                # Connect to each pod using its IP address
                for ip in ips:
                    if ip == self.__container_ip:
                        continue

                    # Connect to the pod at IP address 'ip'
                    # For example, send an HTTP request to the pod:
                    response = requests.get(f"http://{ip}:" + str(port) + "/")

                    if response.status_code == 200:
                        self.__ips.append(ip)

                logging.info(
                    f"Found {len(self.__ips)} available hosts: {self.__ips}")
            time.sleep(frequency)

    def __get_container_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0)
        try:
            # doesn't even have to be reachable
            s.connect(('10.254.254.254', 1))
            IP = s.getsockname()[0]
        except Exception:
            IP = '127.0.0.1'
        finally:
            s.close()
        return IP
