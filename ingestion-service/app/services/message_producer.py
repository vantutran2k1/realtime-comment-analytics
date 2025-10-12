import json
from abc import ABC
from typing import Optional, override

from app.configs.settings import settings
from confluent_kafka import Producer


class MessageProducer(ABC):
    def produce(self, value: bytes, key: Optional[str] = None):
        raise NotImplementedError()


class KafkaMessageProducer(MessageProducer):
    _KAFKA_BASE_CONFIG = {"bootstrap.servers": settings.KAFKA_BROKER}

    def __init__(self, client_id: str, topic: str) -> None:
        self._client_id = client_id
        self._topic = topic

        self._producer = Producer(self._get_producer_config())

    @override
    def produce(self, value: bytes, key: Optional[str] = None):
        self._producer.produce(topic=self._topic, key=key, value=value)

    def _get_producer_config(self):
        conf = self._KAFKA_BASE_CONFIG.copy()
        conf["client.id"] = self._client_id

        return conf
