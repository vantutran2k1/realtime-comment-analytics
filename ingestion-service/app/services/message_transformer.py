import re
from abc import ABC
from typing import override

from app.configs.settings import settings
from app.services.message_consumer import MessageConsumer
from app.services.message_producer import MessageProducer
from app.utils.json_utils import serialize_json


class MessageTransformer(ABC):
    _KAFKA_BASE_CONFIG = {"bootstrap.servers": settings.KAFKA_BROKER}

    def __init__(
        self, client_id: str, group_id: str, source_topic: str, destination_topic: str
    ):
        self._producer = MessageProducer(client_id, destination_topic)
        self._consumer = MessageConsumer(group_id, source_topic)

    def run(self):
        self._consumer.consume(self.on_message, self.on_close)

    def on_message(self, message: dict):
        raise NotImplementedError()

    def on_close(self):
        self._producer.flush()


class MessageTransformerImpl(MessageTransformer):
    def __init__(
        self, client_id: str, group_id: str, source_topic: str, destination_topic: str
    ):
        super().__init__(client_id, group_id, source_topic, destination_topic)

    @override
    def on_message(self, message: dict):
        message["message"] = self._clean_text_for_emojis(message["message"])
        self._producer.produce(value=serialize_json(message))

    @staticmethod
    def _clean_text_for_emojis(text: str) -> str:
        if not text:
            return ""

        cleaned_text = EMOJI_PATTERN.sub(r"", text)
        return cleaned_text.strip()


EMOJI_PATTERN = re.compile(
    "["
    "\U0001f600-\U0001f64f"  # Emoticons
    "\U0001f300-\U0001f5ff"  # Misc Symbols and Pictographs
    "\U0001f680-\U0001f6ff"  # Transport and Map Symbols
    "\U0001f700-\U0001f77f"  # Alchemical Symbols
    "\U0001f780-\U0001f7ff"  # Geometric Shapes Extended
    "\U0001f800-\U0001f8ff"  # Supplemental Arrows-C
    "\U0001f900-\U0001f9ff"  # Supplemental Symbols and Pictographs
    "\U0001fa00-\U0001fa6f"  # Chess Symbols
    "\U0001fa70-\U0001faff"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027b0"  # Dingbats
    "\U000024c2-\U0001f251"
    "]+",
    flags=re.UNICODE,
)
