from typing import Any

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client
from retry import retry


class CustomKubernetesHook(KubernetesHook):
    """
    KubernetesHook with retries to avoid some yandex cloud strange behavior.
    Sometimes yc can not load k8s creds the first time.
    In case of airflow on k8s there's a chance it can be solved without any efforts
    because airflow tries to load k8s cluster settings/context
    """

    @retry(tries=3, delay=2)
    def get_conn(self) -> Any:
        return super().get_conn()

    @retry(tries=3, delay=2)
    def get_custom_object(self, *args, **kwargs):
        return super().get_custom_object(*args, **kwargs)

    @property
    def api_client(self) -> client.ApiClient:
        return self.get_conn()
