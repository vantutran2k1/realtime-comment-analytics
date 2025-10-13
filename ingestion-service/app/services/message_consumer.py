from typing import Any, Callable

from confluent_kafka import Consumer

from app.configs.settings import settings
from app.utils.json_utils import deserialize_json


class MessageConsumer:
    _KAFKA_BASE_CONFIG = {"bootstrap.servers": settings.KAFKA_BROKER}

    def __init__(self, group_id: str, topic: str) -> None:
        self._group_id = group_id
        self._topic = topic

        self._consumer = Consumer(self._get_consumer_config())
        self._consumer.subscribe([self._topic])

    def consume(
        self,
        on_message_callback: Callable[..., Any],
        on_close_callback: Callable[..., Any],
    ):
        try:
            while True:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                raw_data = deserialize_json(msg.value())
                on_message_callback(raw_data)
                on_close_callback()
        except KeyboardInterrupt:
            print("[INFO] Stopping consumer app.")
        finally:
            self._consumer.close()

    def _get_consumer_config(self) -> dict:
        conf = self._KAFKA_BASE_CONFIG.copy()
        conf.update(
            {
                "group.id": self._group_id,
                "auto.offset.reset": "earliest",  # Start reading from the beginning if no offset is found
                "enable.auto.commit": True,
            }
        )

        return conf
