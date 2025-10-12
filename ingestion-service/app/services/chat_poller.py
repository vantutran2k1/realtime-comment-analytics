import time
from abc import ABC
from typing import Any, Callable, override

from app.configs.settings import settings
from googleapiclient.discovery import build


class ChatPoller(ABC):
    def start_polling(self, on_message_callback: Callable[..., Any] = None):
        raise NotImplementedError()


class YouTubeChatPoller(ChatPoller):
    def __init__(self, api_key: str, video_id: str):
        """
        Initializes the poller with the API key and video ID.

        :param api_key: Your Google YouTube Data API v3 key.
        :param video_id: The 11-character YouTube video ID.
        """
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

    @override
    def start_polling(self, on_message_callback: Callable[..., Any] = None):
        """
        Starts the real-time polling loop.

        :param on_message_callback: A function to call with each new message.
        """
        self._fetch_live_chat_id()
        if not self._live_chat_id:
            return

        print(
            f"[+] Starting live chat polling. Initial interval: {self._polling_interval_ms / 1000}s"
        )

        while self._is_live:
            messages = self._fetch_new_messages()

            if messages:
                print(f"[INFO] Received {len(messages)} new messages.")
                if on_message_callback:
                    for msg in messages:
                        on_message_callback(msg)
                else:
                    # Default printing behavior
                    for msg in messages:
                        print(
                            f"[{msg.get('timestamp').split('T')[1][:-1]}] <{msg.get('author_name')}>: {msg.get('message')}"
                        )

            wait_time = self._polling_interval_ms / 1000.0
            print(f"[*] Waiting for {wait_time:.2f} seconds...")
            time.sleep(wait_time)

    def _fetch_live_chat_id(self):
        """
        Finds the liveChatId associated with the video.
        """
        print(f"[*] Fetching live chat ID for video: {self._video_id}")
        try:
            # Use videos.list with 'liveStreamingDetails' part
            request = self._youtube.videos().list(
                part="liveStreamingDetails,snippet", id=self._video_id
            )
            response = request.execute()

            if not response.get("items"):
                print(f"[!] Video ID {self._video_id} not found.")
                return

            video_item = response["items"][0]

            # Check if the video is live
            if video_item["snippet"]["liveBroadcastContent"] == "none":
                print(
                    f"[!] Video ID {self._video_id} is not a live stream or is no longer live."
                )
                return

            if "liveStreamingDetails" not in video_item:
                print(
                    f"[!] Video ID {self._video_id} has no live streaming details (e.g., chat is disabled or it's a Stream Now event)."
                )
                return

            details = video_item["liveStreamingDetails"]

            self._live_chat_id = details.get("activeLiveChatId")

            if not self._live_chat_id:
                print(
                    "[!] Could not find an activeLiveChatId. Chat may be over or disabled."
                )
                return

            print(f"[+] Found Live Chat ID: {self._live_chat_id}")
            self._is_live = True
            return

        except Exception as e:
            print(f"[CRITICAL ERROR] Failed to fetch live chat ID: {e}")
            return False

    def _fetch_new_messages(self):
        """
        Fetches the next batch of live chat messages.

        :return: A list of new chat messages or an empty list.
        """
        if not self._live_chat_id:
            return []

        print(f"[*] Polling for new messages (Token: {self._next_page_token})")

        try:
            # Use liveChatMessages.list to get messages
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
                print("[!] Live chat has ended.")
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
