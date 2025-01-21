from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models import Variable

from dagify.old_operators import KubernetesOperatorWithSensor
from dagify.utils import SparkJobConfig
from dagify.callbacks import TelegramCallback


class BaseDagCreator(ABC):
    """Base class for every dag factory"""

    def __init__(
        self,
        dag_id: str,
        start_date: str,
        namespace: str = None,
        base_spark_job_config: SparkJobConfig = None,
        additional_dag_params: Optional[Dict[str, Any]] = None,
        responsible: List[str] = None,
        token: str = Variable.get("telegram_tkn"),
        chat_ids: List[str] = ["-695531404"],
        **kwargs
    ):
        self.dag_id = dag_id
        self.start_date = start_date
        self.namespace = namespace

        notificator = TelegramCallback(responsible=responsible, token=token, chat_ids=chat_ids)
        self.additional_dag_params = {
            "on_failure_callback": notificator.on_failure_callback,
            "sla_miss_callback": notificator.sla_callback
        }
        self.additional_dag_params = (
            self.additional_dag_params if additional_dag_params is None else {**self.additional_dag_params,**additional_dag_params}
        )
        self.config = base_spark_job_config

    @abstractmethod
    def create_dag(self, *args, **kwargs):
        pass

    def get_dag(self) -> DAG:
        return DAG(
            dag_id=self.dag_id,
            start_date=datetime.strptime(self.start_date, "%Y-%m-%dT%H:%M:%S"),
            **(self.additional_dag_params if self.additional_dag_params else {}),
        )


class SequentialDag(BaseDagCreator):
    """
    Dag with sequential dependencies.
    It's mandatory to point out `spark_job_configs_filepaths` in the right order (from the first to the last)
    """

    def __init__(
        self, *args, spark_job_configs_filepaths: List[Union[str, Path]], **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.spark_job_configs_filepaths = spark_job_configs_filepaths

    def create_dag(self) -> DAG:
        with super().get_dag() as dag:

            operator = KubernetesOperatorWithSensor(dag=dag, namespace=self.namespace)
            chain(
                *[
                    operator(
                        spark_job_config=self.config.merge(_spark_job_config_filepath)
                    )
                    for _spark_job_config_filepath in self.spark_job_configs_filepaths
                ]
            )
        return dag
