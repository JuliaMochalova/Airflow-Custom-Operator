import requests
from packaging.version import Version
from airflow.models import Variable

from dagify.constants import params

def get_latest_tag(items: str):
    versions = [item['version'] if item['version'][0].isdigit() else '0.0' for item in items]
    versions.sort(key=Version)
    return versions[-1]


def get_nexus_path(image):

    """
    Used to get a path to IMAGE in Nexus.
    """

    if ':' not in image:
        service_name = image.split('/')[1]

        if params.env_type == 'dev':
            version = 'develop'
        else:
            request_params = {
                "repository": "docker-private",
                "name": service_name,
                "sort": "version"
            }

            response = requests.get("",
                                    params=request_params,
                                    timeout=60,
                                    verify=Variable.get("ROOT_SERT_PATH"))

            version = get_latest_tag(response.json()['items'])
            if version == '0.0':
                raise ValueError(
                    'There is no tag in the repository. Make sure that the image deploy with the tag is made correctly.')
        image = f'{image.split("/")[0]}/{service_name}:{version}'
    return image
