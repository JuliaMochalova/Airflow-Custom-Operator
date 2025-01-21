
A wrapper over airflow spark operator. Can be extended for other operators and use cases.

Project Organization
------------

    ├── README.md               <- The top-level README for developers using this project.
    │
    ├── src                  <- Source code for use in this project.
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
    ├──.pyproject.toml          <- poetry, linters, formatters settings
