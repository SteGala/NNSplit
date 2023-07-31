from kubernetes import client, config, watch
from plebiscito.node import *
from connect.server import ConnectionHandlerServer
from connect.client import ConnectionHandlerClient
import threading
from plebiscito.topology import topology
from prometheus_api_client import PrometheusConnect


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

acbba_port = 5555


class KubernetesHandler:
    def __init__(self, custom_resource_name, custom_resource_group, custom_api_version, discovery_port, service_name, discovery_time, alpha_value, node_bw, num_clients, utility, prometheus_url, prometheus_port, network_threshold) -> None:
        # Load Kubernetes configuration
        config.load_incluster_config()

        self.__node_name = os.environ['HOSTNAME']

        # Get the namespace in which the code is executed
        self.__namespace = open(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()

        # Create a Kubernetes API client
        self.__api = client.CustomObjectsApi()

        # Define the custom resource name to watch
        self.__custom_resource_name = custom_resource_name
        self.__custom_resource_group = custom_resource_group
        self.__custom_api_version = custom_api_version
        self.__discovery_port = discovery_port
        self.__service_name = service_name
        self.__discovery_time = discovery_time
        self.__node_bw = node_bw
        self.__prometheus_url = prometheus_url
        self.__prometheus_port = prometheus_port
        self.__network_threshold = network_threshold

        check_prometheus_status(self.__prometheus_url, self.__prometheus_port)

        self.__prometheus_connect = PrometheusConnect(f"http://{self.__prometheus_url}:{self.__prometheus_port}")

        _ = ConnectionHandlerServer(self.__discovery_port)

        self.__node = node(acbba_port, alpha_value, utility, self.__node_name)

        threading.Thread(target=self.__node.work, daemon=True).start()
        logging.info(f"Ready to receive nnsplit requests")

    def WatchForEvents(self):
        # Watch for changes to the custom resource
        resource_version = ''
        while True:
            stream = watch.Watch().stream(self.__api.list_namespaced_custom_object,
                                          self.__custom_resource_group,
                                          self.__custom_api_version,
                                          self.__namespace,
                                          self.__custom_resource_name,
                                          resource_version=resource_version)
            for event in stream:
                self.__handle_event(event)
                resource_version = event['object']['metadata']['resourceVersion']

    # Define a function to handle changes to the custom resource
    def __handle_event(self, event):
        logging.info(
            f"Received event: {event['type']} {event['object']['metadata']['name']}")

        if event["type"] == "ADDED":
            custom_resource = event["object"]
            # Extract the desired fields from the custom resource
            job_id = custom_resource["spec"]["job_id"]
            user = custom_resource["spec"]["user"]
            num_gpu = custom_resource["spec"]["num_gpu"]
            num_cpu = custom_resource["spec"]["num_cpu"]
            duration = custom_resource["spec"]["duration"]
            ram = custom_resource["spec"]["num_ram"]
            job_name = custom_resource["spec"]["job_name"]
            submit_time = custom_resource["spec"]["submit_time"]
            gpu_type = custom_resource["spec"]["gpu_type"]
            num_inst = custom_resource["spec"]["num_inst"]
            size = custom_resource["spec"]["size"]
            read_count = custom_resource["spec"]["read_count"]

            conn_client = ConnectionHandlerClient(
            self.__discovery_port, self.__service_name, self.__discovery_time)

            rx, tx = self.get_used_bandwidth()
            top = topology(func_name='complete_graph', max_bandwidth=self.__node_bw-max(tx, rx),
                                 min_bandwidth=self.__node_bw-max(tx, rx)-1, ip_edges=conn_client.get_ips())
        
            if self.need_to_disable_node_bidding(max(rx, tx)):
                self.__node.disable_bidding()
            else:
                self.__node.enable_bidding()
                self.__node.append_data(message_data(job_id, user, num_gpu, num_cpu,
                                    duration, read_count, conn_client.get_ips(), top, ram))

    def get_used_bandwidth(self):
        return 0, 0
        # Define the PromQL query
        query = "rate(node_network_receive_bytes_total{job=\"node-exporter\", instance=\"" + self.__node_name + "\", device!=\"lo\"}[1m]) * 8"  # Replace with your desired PromQL query
        result_receive = self.__prometheus_connect.custom_query(query)

        query = "rate(node_network_transmit_bytes_total{job=\"node-exporter\", instance=\"" + self.__node_name + "\", device!=\"lo\"}[1m]) * 8"  # Replace with your desired PromQL query
        result_transmit = self.__prometheus_connect.custom_query(query)

        return int(float(result_receive[0]["value"][1])), int(float(result_transmit[0]["value"][1]))

    def need_to_disable_node_bidding(self, max_used_bw):
        if max_used_bw/self.__node_bw > self.__network_threshold:
            return True
        return False

def check_prometheus_status(url, port):
    full_url = f"http://{url}:{port}"
    response = requests.get(full_url)
            
    if response.status_code != 200:
        sys.exit(f"Failed to connect to Prometheus at {full_url}. Exiting...")