from typing import List

import requests
from loguru import logger
from omegaconf import OmegaConf
from airflow.models import Variable

from dagify.constants import SLA_EMOJI_CODE, FAILURE_EMOJI_CODE, DEV_FAILURE_EMOJI_CODE


class TelegramCallback:
    """
    TelegramCallback
    Args:
        chat_ids (List[str]): Telegram chat id's
        responsible (List[str]): List of telegram nicknames
        token (str): telegram token
    """

    def __init__(
        self,
        chat_ids: List[str],
        token: str = None,
        message: str = None,  # for backward compatibility
        responsible: List[str] = None, # default value for backward compatibility
    ) -> None:
        self.chat_ids = chat_ids
        self.responsible = responsible
        self.host = Variable.get('self-host')
        self.mode = Variable.get('MODE')
        self.sla_emoji_code = SLA_EMOJI_CODE
        self.failure_emoji_code = FAILURE_EMOJI_CODE if self.mode != 'dev' else DEV_FAILURE_EMOJI_CODE
        self.token = token


    def __send_message(self, message: str, chat_id: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        if self.responsible is not None:
            text = " @" + ", @".join(self.responsible) + "\n" + message
        else:
            text = message

        payload = {
            "text": text,
            "parse_mode": "None",
            "disable_web_page_preview": False,
            "disable_notification": False,
            "reply_to_message_id": None,
            "chat_id": chat_id,
        }
        
        headers = {"accept": "application/json", "content-type": "application/json"}
        response = requests.post(url, json=payload, headers=headers)
        logger.info(f"status code:{response.status_code}, cause: {response.text}")

    def sla_callback(self, dag, **kwargs):
        """
        Method creates message for sla dags

        :param dag: Dag information dict
        """
    
        dag_id = dag.dag_id
        tags = ""
        if 'tags' in dag.__dict__:
            tags = " #" + " #".join(dag.tags) if dag.tags not in (None, '', []) else ""
        date_time = dag.last_loaded
        log_url = f"{self.host}/tree?dag_id={dag_id}"

        msg = f"{self.sla_emoji_code} WARN: Dag {dag_id} at {date_time} works longer than specified in sla." + "\n" + \
                f"Dag by {log_url}\n" + tags

        [self.__send_message(msg, chat_id) for chat_id in self.chat_ids]

    def on_failure_callback(self, context_dict, **kwargs):
        """
        Method creates message for failed dags

        :param context_dict: Dag context
        """

        dag_id = context_dict['dag'].dag_id
        tags = ""
        if 'tags' in context_dict['dag'].__dict__:
            tags = " #" + " #".join(context_dict['dag'].tags) if context_dict['dag'].tags not in (None, '', []) else ""
        failed_date = context_dict['dag'].last_loaded
        log_url = f"{self.host}/tree?dag_id={dag_id}"

        msg = f"{self.failure_emoji_code} ERROR: DAG \"{dag_id}\" failed at {failed_date}. \n" + \
                  f"Logs by {log_url}\n" + tags

        [self.__send_message(msg, chat_id) for chat_id in self.chat_ids]

