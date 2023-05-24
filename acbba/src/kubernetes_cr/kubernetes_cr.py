import time
from kubernetes import client, config, watch
import logging
from acbba.node import *
from connect.server import ConnectionHandlerServer
from connect.client import ConnectionHandlerClient
import threading
from acbba.topology import topology

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

acbba_port = 5555


class KubernetesHandler:
    def __init__(self, custom_resource_name, custom_resource_group, custom_api_version, discovery_port, service_name, discovery_time, alpha_value, node_bw, num_clients, utility) -> None:
        # Load Kubernetes configuration
        config.load_incluster_config()

        node_name = os.environ['HOSTNAME']

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

        _ = ConnectionHandlerServer(self.__discovery_port)

        self.__node = node(acbba_port, alpha_value, utility, node_name)

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
            job_name = custom_resource["spec"]["job_name"]
            submit_time = custom_resource["spec"]["submit_time"]
            gpu_type = custom_resource["spec"]["gpu_type"]
            num_inst = custom_resource["spec"]["num_inst"]
            size = custom_resource["spec"]["size"]
            read_count = custom_resource["spec"]["read_count"]

            conn_client = ConnectionHandlerClient(
            self.__discovery_port, self.__service_name, self.__discovery_time)
            top = topology(func_name='complete_graph', max_bandwidth=self.__node_bw,
                                 min_bandwidth=self.__node_bw/2, ip_edges=conn_client.get_ips())

            self.__node.append_data(message_data(job_id, user, num_gpu, num_cpu,
                                    duration, job_name, submit_time, gpu_type, num_inst, size, read_count, top, conn_client.get_ips()))
