import logging
import time
from kubernetes_cr.kubernetes_cr import KubernetesHandler
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# start with default values
service_name = "acbba"
discovery_port = 8080
discovery_time = 30  # seconds
custom_resource_name = "nnsplitrequests"
custom_resource_group = "my-group.example.com"
custom_api_version = "v1"
alpha_value = 0.8
node_bandwidth = 1000000000
num_clients = 50


def ReadEnvVariables():
    env_var = os.environ.get('service_name')
    global service_name
    if env_var is not None:
        service_name = env_var
    logging.info(f"Discovery process performed on service {service_name}")

    env_var = os.environ.get('discovery_port')
    global discovery_port
    if env_var is not None:
        discovery_port = int(env_var)
    logging.info(f"Discovery process performed on port {discovery_port}")

    env_var = os.environ.get('discovery_time')
    global discovery_time
    if env_var is not None:
        discovery_time = int(env_var)
    logging.info(f"Discovery process performed every {discovery_time}s")

    env_var = os.environ.get('custom_resource_name')
    global custom_resource_name
    if env_var is not None:
        custom_resource_name = env_var

    env_var = os.environ.get('custom_resource_group')
    global custom_resource_group
    if env_var is not None:
        custom_resource_group = env_var

    env_var = os.environ.get('custom_api_version')
    global custom_api_version
    if env_var is not None:
        custom_api_version = env_var
    logging.info(
        f"Watching for custom resources {custom_resource_name} of group {custom_resource_group} version {custom_api_version}")

    env_var = os.environ.get('alpha_value')
    global alpha_value
    if env_var is not None:
        alpha_value = float(env_var)
    logging.info(f"Alpha value = {alpha_value}")

    env_var = os.environ.get('node_bandwidth')
    global node_bandwidth
    if env_var is not None:
        node_bandwidth = float(env_var)
    logging.info(f"Node bandwidth {node_bandwidth}")

    env_var = os.environ.get('num_clients')
    global num_clients
    if env_var is not None:
        num_clients = float(env_var)
    logging.info(f"Number of simulated clients {num_clients}")


if __name__ == '__main__':
    ReadEnvVariables()

    k8s = KubernetesHandler(custom_resource_name,
                            custom_resource_group, custom_api_version, discovery_port, service_name, discovery_time, alpha_value, node_bandwidth, num_clients)

    k8s.WatchForEvents()
