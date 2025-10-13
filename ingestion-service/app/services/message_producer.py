from typing import Optional

from confluent_kafka import Producer

from app.configs.settings import settings


class MessageProducer:
    _KAFKA_BASE_CONFIG = {"bootstrap.servers": settings.KAFKA_BROKER}

    def __init__(self, client_id: str, topic: str) -> None:
        self._client_id = client_id
        self._topic = topic

        self._producer = Producer(self._get_producer_config())

    def produce(self, value: bytes, key: Optional[str] = None):
        self._producer.produce(topic=self._topic, key=key, value=value)

    def flush(self):
        self._producer.flush()

    def _get_producer_config(self) -> dict:
        conf = self._KAFKA_BASE_CONFIG.copy()
        conf["client.id"] = self._client_id

        return conf
