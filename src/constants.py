from datetime import timedelta

from pydantic import BaseSettings, Field

AIRFLOW_DEFAULT_CONFIG = {
    "catchup": False,
    "default_args": {"retries": 1, "retry_delay": timedelta(minutes=2)},
    "default_view": "tree",
}


class AppParams(BaseSettings):
    env_type: str = Field("dev", env="MODE")


params = AppParams()

SLA_EMOJI_CODE = "\U0001F422"
FAILURE_EMOJI_CODE = "\U0001F534"
DEV_FAILURE_EMOJI_CODE = "\U0001F535"

SPARK_DEFAULT_CONF = dict(
        driver_memory='600m',
        driver_cores=1,
        num_executors=1,
        executor_memory='600m',
        executor_cores=1,
        label_version='3.3.1',
        volume_mounts=[{'name': '\"test-volume\"', 'mountPath': '\"/tmp\"'}],
        service_account='spark'
    )

K8S_DEFAULT_CONF = dict(
        mode="cluster",
        namespace='spark',
        type='Python',
        kind='SparkApplication',
        api_group='sparkoperator.k8s.io',
        api_version='v1beta2',
        image_pull_policy='Always',
        time_to_live_seconds=60,
        version="3.3.1",
        restart_policy="Never",
        plural='sparkapplications',
        container_name='spark-kubernetes-driver'
    )

value_error_msg = "The incorrect value {}. {}"

FAILURE_STATES = ("FAILED", "UNKNOWN")
RUNNING_STATES = ("RUNNING",)
PENDING_STATES = ("PENDING", "SUBMITTED")
SUCCESS_STATES = ("COMPLETED",)