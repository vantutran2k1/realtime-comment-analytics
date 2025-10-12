from app.configs.settings import settings
from app.parser import create_parser
from app.services.chat_poller import ChatPoller, YouTubeChatPoller
from app.services.message_handlers import MessageHandler


def main():
    parser = create_parser()
    args = parser.parse_args()

    poller: ChatPoller = YouTubeChatPoller(
        api_key=settings.GOOGLE_CLOUD_API_KEY, video_id=args.video_id
    )
    poller.start_polling(MessageHandler.on_receive_from_source)


if __name__ == "__main__":
    main()
