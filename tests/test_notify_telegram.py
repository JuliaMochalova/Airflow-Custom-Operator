from dagify.callbacks import TelegramCallback


def test_notify_telegram() -> None:
    class Dag:
        def __init__(self, dag_id, last_loaded, tags):
            self.dag_id = dag_id
            self.last_loaded = last_loaded
            self.tags = tags
    context = {'dag': Dag(dag_id=1, last_loaded='2024-02-13', tags=['test'])}
    tg_cb = TelegramCallback(message="test", token_path="tg_token.yaml", responsible=['yuliamochalova'])
    tg_cb.on_failure_callback(context)
