from pathlib import Path
from typing import Any, Dict, Union

from airflow.models import Variable
import boto3
from omegaconf import DictConfig, ListConfig, OmegaConf


def load_config(
    S3_ENDPOINT: str, bucket: str, key: str, ACCESS_KEY: str, SECRET_KEY: str
) -> Union[DictConfig, ListConfig]:
    s3 = boto3.client("s3", 
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      endpoint_url=S3_ENDPOINT)

    response = s3.get_object(Bucket=bucket, Key=key)
    return OmegaConf.load(response["Body"])


class SparkJobConfig:
    def __init__(
            self,
            endpoint_url: str = "",
            bucket: str = "",
            key: str = "",
            s3_conf: bool = True,
    ):

        if s3_conf:
            S3_ENDPOINT = Variable.get('S3_ENDPOINT')
            ACCESS_KEY = Variable.get('aws_access_key_id')
            SECRET_KEY = Variable.get('aws_secret_access_key')
            self.base_config = load_config(
                S3_ENDPOINT=S3_ENDPOINT, bucket=bucket, key=key, ACCESS_KEY=ACCESS_KEY, SECRET_KEY=SECRET_KEY
            )
        else:
            self.base_config = OmegaConf.load(key)

    def merge(self, job_config_filepath: Union[str, Path]) -> Dict[str, Any]:
        user_spark_job_config = OmegaConf.load(job_config_filepath)
        user_spark_job_config = OmegaConf.merge(self.base_config, user_spark_job_config)
        OmegaConf.resolve(user_spark_job_config)
        return OmegaConf.to_container(user_spark_job_config)  # type: ignore
