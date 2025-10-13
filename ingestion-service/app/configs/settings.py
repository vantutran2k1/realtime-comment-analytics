import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    GOOGLE_CLOUD_API_KEY: str = os.environ.get("GOOGLE_CLOUD_API_KEY", "")

    YOUTUBE_API_SERVICE_NAME: str = os.environ.get("YOUTUBE_API_SERVICE_NAME", "")
    YOUTUBE_API_VERSION: str = os.environ.get("YOUTUBE_API_VERSION", "")

    KAFKA_BROKER: str = os.environ.get("KAFKA_BROKER", "localhost:9092")
    KAFKA_RAW_MESSAGES_TOPIC: str = "events.raw_messages"
    KAFKA_TRANSFORMED_MESSAGES_TOPIC: str = "events.transformed_messages"
    RAW_MESSAGE_PRODUCER_CLIENT: str = "raw-message-producer-client-id"
    TRANSFORMER_PRODUCER_CLIENT: str = "transformer-producer-client-id"
    TRANSFORMER_CONSUMER_GROUP: str = "transformer-consumer-group-id"


settings = Settings()
