from kubernetes import client, config, watch
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

class KubernetesHandler:
    def __init__(self, custom_resource_name, custom_resource_group, custom_api_version) -> None:
        # Load Kubernetes configuration
        config.load_incluster_config()

        # Create a Kubernetes API client
        api = client.CoreV1Api()

        # List pods in the default namespace
        pods = api.list_namespaced_pod(namespace="default")
        for pod in pods.items:
            print(pod.metadata.name)

        # Get the namespace in which the code is executed
        self.__namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()

        # Create a Kubernetes API client
        self.__api = client.CustomObjectsApi()

        # Define the custom resource name to watch
        self.__custom_resource_name = custom_resource_name
        self.__custom_resource_group = custom_resource_group
        self.__custom_api_version = custom_api_version

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
                handle_event(event)
                resource_version = event['object']['metadata']['resourceVersion']


# Define a function to handle changes to the custom resource
def handle_event(event):
    logging.info(f"Received event: {event['type']} {event['object']['metadata']['name']}")

