import socket
import time
import logging
import requests
from plebiscito.utils import get_container_ip


class ConnectionHandlerClient:
    def __init__(self, port, hostname, frequency) -> None:
        self.__ips = []
        self.__container_ip = get_container_ip()
        #logging.info(f"Container IP: {self.__container_ip}")

        self.__scan_for_hosts(port, hostname, frequency)

    def __scan_for_hosts(self, port, hostname, frequency):
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
        # Clean the list of ips
        self.__ips = []

        logging.info(f"Detected {ips} under service {hostname}.")
        # Connect to each pod using its IP address
        for ip in ips:
            #if ip == self.__container_ip:
            #    continue
            # Connect to the pod at IP address 'ip'
            # For example, send an HTTP request to the pod:
            response = requests.get(f"http://{ip}:" + str(port) + "/")
            if response.status_code == 200:
                self.__ips.append(ip)

    def get_ips(self):
        return self.__ips
