import redis
import json

class RedisMemor:
    def __init__(self, host="localhost", port=6379, db=0):
        self.client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    def add_message(self, session_id, message):
        key = f"session:{session_id}:messages"
        self.client.rpush(key, json.dumps(message))

    def get_messages(self, session_id):
        key = f"session:{session_id}:messages"
        raw_messages = self.client.lrange(key, 0, -1)
        return [json.loads(m) for m in raw_messages]

    def trim_messages(self, session_id, limit=10):
        key = f"session:{session_id}:messages"
        self.client.ltrim(key, -limit, -1)

    def delete_session(self, session_id):
        self.client.delete(f"session:{session_id}:messages")
