import json

from app.services.message_producer import KafkaMessageProducer, MessageProducer

PRODUCERS: dict[str, MessageProducer] = {
    "RAW_MESSAGE_PRODUCER": KafkaMessageProducer(
        "raw-message-producer-client", "events.raw_messages"
    )
}


class MessageHandler:
    @classmethod
    def on_receive_from_source(cls, message: dict):
        producer = PRODUCERS.get("RAW_MESSAGE_PRODUCER")
        producer.produce(value=json.dumps(message).encode("utf-8"))
