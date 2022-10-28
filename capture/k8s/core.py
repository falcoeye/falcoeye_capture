import json
import logging
import os

import yaml
from kubernetes import client, config, utils
import kubernetes

logger = logging.getLogger(__name__)


class FalcoServingKube:
    ARTIFACT_REGISTRY = None
    def __init__(
        self,
        name,
        namespace="default"
    ):
        self.name = name
        self.service_name = self.name+"-svc"
        self.base_name = name.split("/")[-1]
        self.namespace = namespace
        try:
            config.load_kube_config()
        except:
            config.load_incluster_config()


    def deployment_exists(self):
        v1 = client.AppsV1Api()
        resp = v1.list_namespaced_deployment(namespace=self.namespace)
        for i in resp.items:
            if i.metadata.name == self.name:
                return True
        return False

    def service_exists(self):
        v1 = client.CoreV1Api()
        resp = v1.list_namespaced_service(namespace=self.namespace)
        for i in resp.items:
            if i.metadata.name == self.name:
                return True
        return False

    def is_running(self):
        if self.deployment_exists() and self.service_exists():
            return True
        return False

    def get_service_address(self, external=False, hostname=False):
        if not self.is_running():
            logger.error(f"No running deployment found for {self.name}.")
            return None

        v1 = client.CoreV1Api()
        service = v1.read_namespaced_service(namespace=self.namespace, name=self.name)
        [port] = [port.port for port in service.spec.ports]
        
        if external:
            try:
                service = v1.read_namespaced_service(namespace=self.namespace, name=self.service_name)
            except Exception :
                # trying without -svc
                service = v1.read_namespaced_service(namespace=self.namespace, name=self.service_name[:-4])
            if hostname:
                host = service.status.load_balancer.ingress[0].hostname
            else:
                host = service.status.load_balancer.ingress[0].ip
        else:
            host = service.spec.cluster_ip

        return f"{host}:{port}"
