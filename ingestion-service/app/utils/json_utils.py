import json


def serialize_json(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def deserialize_json(message_value: bytes) -> dict:
    return json.loads(message_value.decode("utf-8"))
