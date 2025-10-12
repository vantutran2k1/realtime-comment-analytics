import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    GOOGLE_CLOUD_API_KEY: str = os.environ.get("GOOGLE_CLOUD_API_KEY", "")

    YOUTUBE_API_SERVICE_NAME: str = os.environ.get("YOUTUBE_API_SERVICE_NAME", "")
    YOUTUBE_API_VERSION: str = os.environ.get("YOUTUBE_API_VERSION", "")

    KAFKA_BROKER: str = os.environ.get("KAFKA_BROKER", "localhost:9092")


settings = Settings()
