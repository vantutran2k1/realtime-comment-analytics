import threading
import time

from app.configs.settings import settings
from app.parser import create_parser
from app.services.chat_poller import ChatPoller, YouTubeChatPoller
from app.services.message_transformer import MessageTransformer, MessageTransformerImpl


def run_poller(poller_instance: YouTubeChatPoller):
    print("\n--- Starting YouTube Poller Thread ---")
    poller_instance.start_polling()


def run_transformer(transformer_instance: MessageTransformer):
    print("\n--- Starting Kafka Transformer Thread ---")
    transformer_instance.run()


def main():
    parser = create_parser()
    args = parser.parse_args()

    poller: ChatPoller = YouTubeChatPoller(
        api_key=settings.GOOGLE_CLOUD_API_KEY, video_id=args.video_id
    )
    transformer: MessageTransformer = MessageTransformerImpl(
        client_id=settings.TRANSFORMER_PRODUCER_CLIENT,
        group_id=settings.TRANSFORMER_CONSUMER_GROUP,
        source_topic=settings.KAFKA_RAW_MESSAGES_TOPIC,
        destination_topic=settings.KAFKA_TRANSFORMED_MESSAGES_TOPIC,
    )

    poller_thread = threading.Thread(
        target=run_poller, args=(poller,), daemon=True, name="YouTube-Poller"
    )
    transformer_thread = threading.Thread(
        target=run_transformer,
        args=(transformer,),
        daemon=True,
        name="Kafka-Transformer",
    )

    poller_thread.start()
    transformer_thread.start()

    try:
        while True:
            time.sleep(1)
            if not poller_thread.is_alive() or not transformer_thread.is_alive():
                print("\n[CRITICAL] One thread stopped unexpectedly. Shutting down...")
                break
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Keyboard interrupt received. Stopping threads...")
    finally:
        pass


if __name__ == "__main__":
    main()
