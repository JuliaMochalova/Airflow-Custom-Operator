from time import sleep
from typing import List

from airflow import DAG
from airflow.models import taskinstance
from airflow.utils.db import provide_session


@provide_session
def clear_tasks(
    tis: List[taskinstance.TaskInstance],
    dag: DAG,
    session=None,
    activate_dag_runs: bool = False,
) -> None:
    taskinstance.clear_task_instances(
        tis=tis,
        session=session,
        activate_dag_runs=activate_dag_runs,
        dag=dag,
    )


def clear_upstream_task(context) -> None:
    sleep(120)  # wait for spark application garbage collector
    tasks_to_clear = context["params"].get("tasks_to_clear", [])
    all_tasks = context["dag_run"].get_task_instances()
    tasks_to_clear = [ti for ti in all_tasks if ti.task_id in tasks_to_clear]
    clear_tasks(tasks_to_clear, dag=context["dag"])
