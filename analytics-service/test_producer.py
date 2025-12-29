# pip install kafka-python
from kafka import KafkaProducer
import json
import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], # Lưu ý: localhost dùng cổng 29092
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

data = {
    "timestamp": datetime.datetime.now().isoformat(),
    "video_id": "N528G5LKgmU",
    "author_name": "Soicodon932",
    "author_channel_id": "UCmyaueI5XnwbZkhQeV17zvg",
    "message": "đánh như cc mà cũng live",
    "is_member": False
}

producer.send('events.transformed_messages', value=data)
print("Đã gửi tin nhắn test!")
producer.flush()