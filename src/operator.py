from typing import Any, Dict, Optional, Sequence
import time

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.exceptions import AirflowException

from dagify.configs import SparkJobConf, K8sConf, get_k8s_yaml_with_spark
from dagify.constants import \
    K8S_DEFAULT_CONF, \
    params,\
    FAILURE_STATES, \
    SUCCESS_STATES, \
    PENDING_STATES


class SparkKubernetesOperator(BaseOperator):
    """SparkKubernetesOperator and CustomSparkKubernetesSensor are combined into one TaskGroup.
    It was made to enable retries for the whole group and avoid boilerplate operator/sensor definitions"""
    template_fields: Sequence[str] = ("args", "envs", "name")
    def __init__(
            self,
            spark_yaml: Dict[str, Any] = None,
            spark_conf: SparkJobConf = None,
            k8s_conf: K8sConf = K8sConf(),
            kubernetes_conn_id: Optional[str] = 'kubernetes_default',
            task_id: str = None,
            **kwargs
    ) -> None:

        self.spark_conf = spark_conf
        self.k8s_conf = k8s_conf
        self.spark_yaml = self.application_file = spark_yaml
        self.namespace = K8S_DEFAULT_CONF['namespace']
        self.hook = KubernetesHook(conn_id=kubernetes_conn_id)

        if self.spark_yaml:
            self.name = self.spark_yaml["metadata"]["name"]
        else:
            self.name = spark_conf.name
            self.envs = spark_conf.envs
            self.args = spark_conf.args

        self.name = "-".join(
            [self.name, params.env_type]
        )
        if self.spark_yaml:
            self.spark_yaml["metadata"]["name"] = self.name
            self.envs=spark_yaml.get('spec').get('env')
            self.args=spark_yaml.get('spec').get('args')
        if task_id is None:
            task_id = self.name
        super().__init__(**{**kwargs, **{'task_id': task_id}})


    def execute(self, context: Context):


        if (self.spark_yaml is None and self.spark_conf is None) \
                or (self.spark_yaml and self.spark_conf):
            raise ValueError("You must fill one of two attribute options: (spark_yaml) or (spark_conf and k8s_conf)")
        if self.spark_yaml:
            self.namespace = self.spark_yaml["metadata"]["namespace"]
        else:
            self.spark_conf.envs = self.envs
            self.spark_conf.args = self.args
            self.spark_conf.name = self.name

            self.namespace = self.k8s_conf.namespace if self.k8s_conf else self.namespace
            spark_job_config = get_k8s_yaml_with_spark(self.spark_conf, self.k8s_conf, context)

            self.application_file = spark_job_config

        self.log.info("Creating sparkApplication")
        response = self.hook.create_custom_object(
            group=K8S_DEFAULT_CONF['api_group'],
            version=K8S_DEFAULT_CONF['api_version'],
            plural=K8S_DEFAULT_CONF['plural'],
            body=self.application_file,
            namespace=self.namespace
        )
        if response:
            return self.check_application_status()

        # TODO Переделать, взависимости от содержания возвращаемого запроса
        else:
            raise AirflowException(f"Spark application failed: {response.text}")

    def check_application_status(self, **kwargs):
        app_name = self.name
        while True:
            if self.poke():
                driver_complete = True
                break
            else:
                time.sleep(1)
        if not (driver_complete):
            raise AirflowException(f'{app_name} failed!')
        return True

    def poke(self) -> bool:
        self.log.info("Poking: %s", self.name)
        response = self.hook.get_custom_object(
            group=K8S_DEFAULT_CONF['api_group'],
            version=K8S_DEFAULT_CONF['api_version'],
            plural=K8S_DEFAULT_CONF['plural'],
            name=self.name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["applicationState"]["state"]
        except KeyError:
            return False
        if application_state in FAILURE_STATES:
            raise AirflowException(f"Spark application failed with state: {application_state}")
        elif application_state in PENDING_STATES:
            self.log.info("Spark application still is in state: PENDING")
            return False
        elif application_state in SUCCESS_STATES:
            self.log.info("Spark application ended successfully")
            return True
        else:
            self.log.info("Spark application still is in state: %s", application_state)
            self.log_driver(response)
            return False

    def log_driver(self, response: dict) -> None:
        status_info = response["status"]
        if "driverInfo" not in status_info:
            return
        driver_info = status_info["driverInfo"]
        if "podName" not in driver_info:
            return
        driver_pod_name = driver_info["podName"]
        namespace = response["metadata"]["namespace"]
        log = self.hook.get_pod_log_stream(pod_name=driver_pod_name,
                                           container=K8S_DEFAULT_CONF['container_name'],
                                           namespace=namespace)
        for event in log[-1]:
            self.log.info(f"{event}")
