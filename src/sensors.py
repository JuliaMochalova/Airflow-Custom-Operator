from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

from dagify.hooks import CustomKubernetesHook


class CustomSparkKubernetesSensor(SparkKubernetesSensor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = CustomKubernetesHook(conn_id=self.kubernetes_conn_id)
