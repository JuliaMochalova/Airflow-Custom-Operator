from typing import List

from airflow import DAG as AIRFLOW_DAG
from airflow.models import Variable

from dagify.callbacks import TelegramCallback


class DAG(AIRFLOW_DAG):
    def __init__(
            self,
            responsible: List[str] = None,
            token: str = Variable.get('telegram_tkn'),
            chat_ids: List[str] = ["-695531404"],
            **kwargs):
        if responsible:
            notificator = TelegramCallback(responsible=responsible, token=token, chat_ids=chat_ids)
            if 'default_args' in kwargs:
                kwargs['default_args']['on_failure_callback'] = notificator.on_failure_callback,
                kwargs['default_args']['sla_miss_callback'] = notificator.sla_callback
            else:
                kwargs['default_args'] = {'on_failure_callback': notificator.on_failure_callback,
                                          'sla_miss_callback': notificator.sla_callback}
        super().__init__(**kwargs)


