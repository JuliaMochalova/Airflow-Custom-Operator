Dagify
==============================

A wrapper over airflow spark operator. Can be extended for other operators and use cases.

Project Organization
------------

    ├── README.md               <- The top-level README for developers using this project.
    │
    ├── dagify                  <- Source code for use in this project.
    │   │
    │   ├── __init__.py         <- Makes src a Python module
    │   │
    │   ├── constants.py        <- Some constants to use in different parts of the library
    │   │
    │   ├── dag_factory.py      <- Base classes for dag creation
    │   │
    │   ├── dag_utils.py        <- Some utils for airflow dag
    │   │
    │   ├── hooks.py            <- Custom airflow hooks
    │   │
    │   ├── operators.py        <- Custom airflow operators
    │   │
    │   ├── sensors.py          <- Custom airflow sensors
    │   │
    │   ├── utils.py            <- Bunch of utils functions
    │   │
    ├── tests                   <- Unit tests
    │
    ├──.gitignore
    │
    ├──.pre-commit-config.yaml  <- Pre-commit hooks and their settings
    │
    ├──.pyproject.toml          <- poetry, linters, formatters settings
    │
    └── tox.ini                 <- tox file with settings for running tox; see tox.

------------


How to create the own DAG with custom dependencies?
------------

Let's see how this's done in SequentialDag

1) SequentialDag is inherited from BaseDagCreator
   ```python
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
    ```
2) BaseDagCreator requires to override `create_dag` method. \
Create a context manager that initializes a dag and defines tasks and their dependencies inside the context manager
   ```python
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
    ```
3) Create dags folder in your repo and put a dag definition file there
    ```python
       spark_job_configs_filepaths = (
            TEST_CONFIGS_PATH / "raw_features.yaml",
            TEST_CONFIGS_PATH / "catchments.yaml",
        )

        base_spark_job_config = SparkJobConfig(
            bucket="mle-airflow",
            key="base_spark_job_configs/template.yaml",
            endpoint_url="https://storage.yandexcloud.net",
        )

        dag = SequentialDag(
            dag_id="geo",
            start_date="2021-12-10T12:00:00",
            base_spark_job_config=base_spark_job_config,
            additional_dag_params=AIRFLOW_DEFAULT_CONFIG,
            spark_job_configs_filepaths=spark_job_configs_filepaths,
            namespace="spark",
        ).create_dag()
    ```
4) Add telegram callbacks to your DAG. Airflow has 2 types of callbacks on failure and on success.
```
from dagify import TelegramCallback

on_failure_callback = TelegramCallback(chat_ids=chat_ids, message="critical")
on_success_callback = TelegramCallback(chat_ids=chat_ids, message="have a nice day")

dag = SequentialDag(
    dag_id="geo",
    start_date="2021-12-10T12:00:00",
    base_spark_job_config=base_spark_job_config,
    additional_dag_params=AIRFLOW_DEFAULT_CONFIG,
    spark_job_configs_filepaths=spark_job_configs_filepaths,
    namespace="spark",
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
).create_dag()

```
5) Ask MLE team to set up the git sync between your repo and airflow
