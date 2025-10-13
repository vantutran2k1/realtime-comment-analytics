import time
from abc import ABC
from typing import override

from googleapiclient.discovery import build

from app.configs.settings import settings
from app.services.message_producer import MessageProducer
from app.utils.json_utils import serialize_json


class ChatPoller(ABC):
    def start_polling(self):
        raise NotImplementedError()


class YouTubeChatPoller(ChatPoller):
    def __init__(self, api_key: str, video_id: str):
        self._api_key = api_key
        self._video_id = video_id
        self._youtube = build(
            settings.YOUTUBE_API_SERVICE_NAME,
            settings.YOUTUBE_API_VERSION,
            developerKey=self._api_key,
        )

        self._live_chat_id = None
        self._is_live = False
        self._next_page_token = None
        self._polling_interval_ms = 5000  # Default to 5 seconds

        self._producer = MessageProducer(
            client_id=settings.RAW_MESSAGE_PRODUCER_CLIENT,
            topic=settings.KAFKA_RAW_MESSAGES_TOPIC,
        )

    @override
    def start_polling(self):
        self._fetch_live_chat_id()
        if not self._live_chat_id:
            return

        try:
            while self._is_live:
                messages = self._fetch_new_messages()

                if messages:
                    print(f"[INFO] Received {len(messages)} new messages.")
                    for msg in messages:
                        self._producer.produce(value=serialize_json(msg))
                        self._producer.flush()

                wait_time = self._polling_interval_ms / 1000.0
                print(f"[INFO] Waiting for {wait_time:.2f} seconds...")
                time.sleep(wait_time)
        except KeyboardInterrupt:
            print(f"[INFO] Shutting down poller application.")
        finally:
            self._is_live = False

    def _fetch_live_chat_id(self):
        try:
            request = self._youtube.videos().list(
                part="liveStreamingDetails,snippet", id=self._video_id
            )
            response = request.execute()

            if not response.get("items"):
                return None

            video_item = response["items"][0]

            if (
                video_item["snippet"]["liveBroadcastContent"] == "none"
                or "liveStreamingDetails" not in video_item
            ):
                return None

            details = video_item["liveStreamingDetails"]
            self._live_chat_id = details.get("activeLiveChatId")
            if not self._live_chat_id:
                return None

            self._is_live = True
            return None

        except Exception as e:
            print(f"[ERROR] Failed to fetch live chat ID: {e}")
            return False

    def _fetch_new_messages(self):
        if not self._live_chat_id:
            return []

        try:
            request = self._youtube.liveChatMessages().list(
                liveChatId=self._live_chat_id,
                part="id,snippet,authorDetails",
                pageToken=self._next_page_token,
                maxResults=2000,  # Max supported result set
            )
            response = request.execute()

            self._next_page_token = response.get("nextPageToken")

            self._polling_interval_ms = response.get("pollingIntervalMillis", 5000)

            if "offlineAt" in response and response["offlineAt"] is not None:
                self._is_live = False
                return []

            new_messages = []
            for item in response.get("items", []):
                snippet = item["snippet"]
                author = item["authorDetails"]

                if snippet["type"] == "textMessageEvent":
                    message_data = {
                        "timestamp": snippet["publishedAt"],
                        "author_name": author["displayName"],
                        "author_channel_id": author["channelId"],
                        "message": snippet["textMessageDetails"]["messageText"],
                        "is_member": author.get("isChatOwner", False)
                        or author.get("isChatModerator", False)
                        or author.get("isChatSponsor", False),
                    }
                    new_messages.append(message_data)
                elif snippet["type"] == "superChatEvent":
                    new_messages.append(
                        {
                            "timestamp": snippet["publishedAt"],
                            "author_name": author["displayName"],
                            "message": f"SUPER CHAT: {snippet['superChatDetails']['displayString']}",
                            "amount": snippet["superChatDetails"][
                                "amountDisplayString"
                            ],
                        }
                    )

            return new_messages
        except Exception as e:
            print(f"[ERROR] Failed to fetch messages: {e}")
            self._is_live = False
            return []
