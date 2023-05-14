import logging
from connect.server import ConnectionHandlerServer
from connect.client import ConnectionHandlerClient
from kubernetes_cr.kubernetes_cr import KubernetesHandler
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# start with default values
service_name = "acbba"
discovery_port = 8080
discovery_time = 30 # seconds
custom_resource_name = "NNSplitRequest"
custom_resource_group = "my-group.example.com"
custom_api_version = "v1"

def ReadEnvVariables():
    env_var = os.environ.get('service_name')
    if env_var is not None:
        service_name = env_var
    logging.info(f"Discovery process performed on service {service_name}")

    env_var = os.environ.get('discovery_port')
    if env_var is not None:
        discovery_port = int(env_var)
    logging.info(f"Discovery process performed on port {discovery_port}")

    env_var = os.environ.get('discovery_time')
    if env_var is not None:
        discovery_time = int(env_var)
    logging.info(f"Discovery process performed every {discovery_time}s")

    env_var = os.environ.get('custom_resource_name')
    if env_var is not None:
        custom_resource_name = env_var
    
    env_var = os.environ.get('custom_resource_group')
    if env_var is not None:
        custom_resource_group = env_var

    env_var = os.environ.get('custom_api_version')
    if env_var is not None:
        custom_api_version = env_var
    logging.info(f"Watching for custom resources {custom_resource_name} of group {custom_resource_group} version {custom_api_version}")

if __name__ == '__main__':
    ReadEnvVariables()

    conn_server = ConnectionHandlerServer(discovery_port)
    conn_client = ConnectionHandlerClient(discovery_port, service_name, discovery_time)

    k8s = KubernetesHandler(custom_resource_name, custom_resource_group, custom_api_version)
    k8s.WatchForEvents()
    

