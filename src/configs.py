from typing import List, Optional, Dict, Union
from io import StringIO
from collections import defaultdict
import yaml

from airflow.models import Variable
from airflow.utils.context import Context

from dagify.nexus_path_extractor import get_nexus_path
from dagify.constants import SPARK_DEFAULT_CONF, \
                            K8S_DEFAULT_CONF, \
                            value_error_msg


class K8sConf:
    def __init__(self,
                 repositories: Optional[List[str]] = None,
                 restart_policy: Optional[str] = None,
                 namespace: Optional[str] = None,
                 image_pull_policy: Optional[str] = None):

        self.repositories: Optional[List[str]] = repositories
        """List of repositories."""
        self.restart_policy: Optional[str] = restart_policy
        """RestartPolicy"""
        self.namespace: Optional[str] = namespace
        """Namespace"""
        self.image_pull_policy: Optional[str]= image_pull_policy
        """Always/Never/IfNotPresent"""

    def __checking_parameters(self):

        if self.repositories:
            if type(self.repositories) is not list:
                raise TypeError(value_error_msg.format('repositories', "The necessary format is a list, example: ['repository1', 'repository2', ..]"))

        if self.restart_policy:
            if type(self.restart_policy) is not str:
                raise TypeError(value_error_msg.format('restart_policy', "The necessary format is the value in the string format"))

    def __generate_base_config(self):
        self.base_config = {}

    def generate_config(self) -> Dict:
        self.__checking_parameters()
        self.__generate_base_config()

        yaml_dict = defaultdict(dict)
        yaml_dict['kind'] = K8S_DEFAULT_CONF['kind']
        yaml_dict['metadata']['namespace'] = self.namespace if self.namespace else K8S_DEFAULT_CONF['namespace']
        yaml_dict['apiVersion'] = K8S_DEFAULT_CONF['api_group'] + "/" + K8S_DEFAULT_CONF['api_version']
        spec = defaultdict(dict)
        spec['type'] = K8S_DEFAULT_CONF['type']
        spec['mode'] = K8S_DEFAULT_CONF['mode']
        spec['deps']['repositories'] = self.repositories
        spec['imagePullPolicy'] = self.image_pull_policy if self.image_pull_policy else K8S_DEFAULT_CONF['image_pull_policy']
        spec['timeToLiveSeconds'] = K8S_DEFAULT_CONF['time_to_live_seconds']
        spec['sparkVersion'] = K8S_DEFAULT_CONF['api_group'] + "/" + K8S_DEFAULT_CONF['api_version']
        spec['restartPolicy']['type'] = self.restart_policy if self.restart_policy else K8S_DEFAULT_CONF['restart_policy']
        yaml_dict['spec'] = dict(spec)
        return yaml_dict

class SparkJobConf:

    def __init__(self,
                 name: str,
                 image: str,
                 main_application_file: str,
                 node_selector: str,
                 executor_core_limit: Optional[str] = None,
                 driver_core_limit: Optional[str] = None,
                 driver_memory: Optional[str] = None,
                 driver_cores: Optional[str] = None,
                 num_executors: Optional[int] = None,
                 max_executors: Optional[int] = None,
                 executor_memory: Optional[str] = None,
                 executor_cores: Optional[int] = None,
                 envs: Optional[Dict] = None,
                 env_from: Optional[str] = None,
                 spark_conf_overrides: Optional[List[str]] = None,
                 driver_java_options: Optional[str] = None,
                 executor_java_options: Optional[str] = None,
                 packages: Optional[List[str]] = None,
                 args: Optional[Union[List,Dict]] = None
                 ):

        self.name: str = name
        """The name of the application that will be displayed in the kubernetes"""
        self.image: str = image
        """image"""
        self.node_selector: str = node_selector
        """Node selector name in spark namespace"""
        self.driver_memory: Optional[str] = driver_memory
        """The amount of memory of the driver, for example 1g, 500m, etc."""
        self.driver_cores: Optional[str] = driver_cores
        """The number of driver cores"""
        self.driver_core_limit: Optional[str] = driver_core_limit
        """The number of cores is served"""
        self.num_executors: Optional[int] = num_executors
        """The number of executors """
        self.max_executors: Optional[int] = max_executors
        """The maximum number of executioners that can be highlighted
            application if the option spark.dynamicAllocation.enabled=true"""
        self.executor_memory: Optional[str] = executor_memory
        """The amount of memory of the executor, for example 1g, 500m, etc."""
        self.executor_cores: Optional[int] = executor_cores
        """Количество ядер экзекутора"""
        self.executor_core_limit: Optional[str] = executor_core_limit
        """The number of executor cores"""
        self.envs: Optional[Dict] = envs
        """Environment variables"""
        self.env_from: Optional[str] = env_from
        """Name of secret_ref in kubernetes"""
        self.spark_conf_overrides: Optional[List[str]] = spark_conf_overrides
        """Overlooking the spark config parameters, a list of configs in format ['key1=value1', 'key2=value2']"""
        self.driver_java_options: Optional[str] = driver_java_options
        """JVM Driver settings"""
        self.executor_java_options: Optional[str] = executor_java_options
        """JVM Executor settings"""
        self.packages:  Optional[List[str]] = packages
        """Packages that need to be additionally loaded to executioners. In the format of the list"""
        self.main_application_file: str = main_application_file
        """The path to executable python module"""
        self.args: Optional[Union[List,Dict]] = args
        """jobs arguments"""

    def __checking_parameters(self):
        if self.driver_memory:
            if type(self.driver_memory) is str:
                if self.driver_memory[-1].isdigit():
                    raise ValueError(
                        value_error_msg.format('driver_memory', "The necessary format is '600m', '1g', etc."))
            else:
                raise TypeError(value_error_msg.format('driver_memory', "The necessary format is '600m', '1g', etc."))
        if self.driver_cores:
            if type(self.driver_cores) is not int:
                raise TypeError(value_error_msg.format('driver_cores', "Enter the value of type int"))
        if self.driver_core_limit:
            if type(self.driver_core_limit) is not str:
                raise TypeError(value_error_msg.format('driver_core_limit', "Enter the value of type str"))
        if self.num_executors:
            if type(self.num_executors) is not int:
                raise TypeError(value_error_msg.format('num_executors', "Enter the value of type int"))
        if self.executor_memory:
            if type(self.executor_memory) is str:
                if self.executor_memory[-1].isdigit():
                    raise ValueError(
                        value_error_msg.format('executor_memory', "The necessary format is '600m', '1g', etc."))
            else:
                raise TypeError(
                    value_error_msg.format('executor_memory', "The necessary format is '600m', '1g', etc."))
        if self.executor_cores:
            if type(self.executor_cores) is not int:
                raise TypeError(value_error_msg.format('executor_cores', "Enter the value of type int"))
        if self.executor_core_limit:
            if type(self.executor_core_limit) is not str:
                raise TypeError(value_error_msg.format('executor_core_limit', "Enter the value of type str"))
        if self.envs:
            if type(self.envs) is not dict:
                raise TypeError(value_error_msg.format('envs', "Enter values in the dictionary format"))
        if self.spark_conf_overrides:
            if type(self.spark_conf_overrides) is not list:
                raise TypeError(value_error_msg.format('spark_conf_overrides', "Enter values in the format of the list of lines 'key=value'"))
        if self.packages:
            if type(self.packages) is not list:
                raise TypeError(value_error_msg.format('packages', "Enter packages in the list format"))
        if self.args:
            if type(self.args) not in [list, dict]:
                raise TypeError(value_error_msg.format('args', "Enter the values in the format of the dictionary or list"))


    @staticmethod
    def parse_env(env: Dict):
        env_list = []
        for key in env.keys():
            env_list.append({'name': key, 'value': f'{env[key]}'})
        return env_list

    @staticmethod
    def get_vars_dagrun_conf(context: Dict, key: str):
        dag_run_params = context.get("dag_run")
        if dag_run_params is not None:
            dagrun_vars = dag_run_params.conf.get(key)
            return dagrun_vars

    def get_spark_conf(self):
        extra_spark_conf = dict()
        extra_spark_conf["spark.jars.packages"] = "org.postgresql:postgresql:42.5.0,com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.3.1"
        extra_spark_conf["spark.jars.ivy"] = "/tmp/ivy"

        extra_spark_conf["spark.hadoop.fs.s3a.endpoint"] = Variable.get("S3_ENDPOINT")
        extra_spark_conf["spark.hadoop.fs.s3a.access.key"] = Variable.get("IT_AWS_ACCESS_KEY_ID")
        extra_spark_conf["spark.hadoop.fs.s3a.secret.key"] = Variable.get("IT_AWS_SECRET_ACCESS_KEY")
        extra_spark_conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"
        extra_spark_conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        extra_spark_conf["spark.hadoop.fs.s3a.path.style.access"] = "true"
        extra_spark_conf["spark.sql.catalogImplementation"] = "hive"
        extra_spark_conf["spark.hadoop.javax.jdo.option.ConnectionURL"] = Variable.get("HIVE_METASTORE_URL")
        extra_spark_conf["spark.hadoop.javax.jdo.option.ConnectionPassword"] = Variable.get("HIVE_METASTORE_PASSWORD")
        extra_spark_conf["spark.hadoop.javax.jdo.option.ConnectionUserName"] = Variable.get("HIVE_METASTORE_USER")
        extra_spark_conf["spark.hadoop.javax.jdo.option.ConnectionDriverName"] = "org.postgresql.Driver"

        extra_spark_conf["spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.claimName"] = "OnDemand"
        extra_spark_conf["spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.storageClass"] = "yc-network-ssd"
        extra_spark_conf["spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.sizeLimit"] = "50Gi"
        extra_spark_conf["spark.kubernetes.executor.volumes.persistentVolumeClaim.data.mount.path"] = "/data"
        extra_spark_conf["spark.kubernetes.submission.connectionTimeout"] = "60000"
        extra_spark_conf["spark.kubernetes.submission.requestTimeout"] = "60000"
        extra_spark_conf["spark.kubernetes.driver.connectionTimeout"] = "60000"
        extra_spark_conf["spark.kubernetes.driver.requestTimeout"] = "60000"

        if self.spark_conf_overrides:
            for i in self.spark_conf_overrides:
                split_config = i.split("=")
                extra_spark_conf[split_config[0]] = split_config[1]
        return extra_spark_conf


    def __set_envs(self, context: Context):
        envs = {}

        if self.envs:
            envs = self.envs

        envs_from_dagrun_conf = self.get_vars_dagrun_conf(context, "envs")
        if envs_from_dagrun_conf:
            envs.update(envs_from_dagrun_conf)
        return self.parse_env(envs)

    def __get_packages(self):

        base_packages = ["org.postgresql:postgresql:42.5.0"]
        if self.packages:
            return base_packages + self.packages
        return base_packages

    def __set_args(self, context: Context):
        args = []
        args_from_dagrun_conf = self.get_vars_dagrun_conf(context, "args")
        if self.args:
            if type(self.args) == dict:
                args.extend(["--" + str(k) + "=" + str(v) for k, v in self.args.items()])
            else:
                args.extend(self.args)
        if args_from_dagrun_conf is not None:
            if type(args_from_dagrun_conf) == dict:
                args.extend(["--" + str(k) + "=" + str(v) for k, v in args_from_dagrun_conf.items()])
            else:
                args.extend(args_from_dagrun_conf)
        return args

    @staticmethod
    def  __set_core_limits(cores, limits):
        return limits if limits else f'{cores * 1000}m'

    def generate_config(self, context: Context):
        self.__checking_parameters()
        self.envs = self.__set_envs(context)

        yaml_dict = defaultdict(dict)
        yaml_dict['metadata']['name'] = self.name
        spec = defaultdict(dict)
        spec['arguments'] = self.__set_args(context)
        spec['sparkConf'] = self.spark_conf_overrides
        spec['deps']['packages'] = self.__get_packages()
        spec['image'] = get_nexus_path(self.image)
        spec['mainApplicationFile'] = self.main_application_file
        spec['volumes'] = [{'name': 'test-volume', 'hostPath': {'path': '/tmp', 'type': "Directory"}}]
        spec_driver = defaultdict(dict)
        spec_driver['memory'] = self.driver_memory if self.driver_memory else SPARK_DEFAULT_CONF['driver_memory']
        spec_driver['cores'] = self.driver_cores if self.driver_cores else SPARK_DEFAULT_CONF['driver_cores']
        spec_driver['coreLimit'] = self.__set_core_limits(spec_driver['cores'], self.driver_core_limit)
        spec_driver['labels']['version'] = SPARK_DEFAULT_CONF['label_version']
        spec_driver['volumeMounts'] = SPARK_DEFAULT_CONF['volume_mounts']
        spec_driver['serviceAccount'] = SPARK_DEFAULT_CONF['service_account']
        spec_driver['env'] = self.envs
        if self.env_from:
            spec_driver['envFrom'] = [{'secretRef': {'name': self.env_from}}]
        spec_driver['nodeSelector']['spark'] = self.node_selector
        spec['driver'] = dict(spec_driver)
        spec_executor = defaultdict(dict)
        spec_executor["cores"] = self.executor_cores if self.executor_cores else SPARK_DEFAULT_CONF['executor_cores']
        spec_executor["coreLimit"] = self.__set_core_limits(spec_executor['cores'], self.executor_core_limit)
        spec_executor['memory'] = self.executor_memory if self.executor_memory else SPARK_DEFAULT_CONF['executor_memory']
        spec_executor['labels']['version'] = SPARK_DEFAULT_CONF['label_version']
        spec_executor['env'] = self.envs
        if self.env_from:
            spec_executor['envFrom'] = [{'secretRef':{'name':self.env_from}}]
        spec_executor['instances'] = self.num_executors if self.num_executors else SPARK_DEFAULT_CONF['num_executors']
        spec_executor['nodeSelector']['spark'] = self.node_selector
        spec['executor'] = dict(spec_executor)
        spec['sparkConf'] = self.get_spark_conf()
        yaml_dict['spec'] = dict(spec)

        return yaml_dict


def get_k8s_yaml_with_spark(spark_conf: SparkJobConf, k8s_conf: K8sConf, context: Context) -> str:
    spark_conf_yaml = spark_conf.generate_config(context)
    k8s_conf_yaml = k8s_conf.generate_config()
    for key, value in spark_conf_yaml.items():
        for subkey, subvalue in value.items():
            k8s_conf_yaml[key][subkey] = subvalue

    yaml_output = StringIO()
    yaml_output.write(yaml.dump(dict(k8s_conf_yaml), default_flow_style=False, sort_keys=False))
    application_file = yaml_output.getvalue().replace("\"\'", "\"").replace("\'\"", "\"")
    return application_file
