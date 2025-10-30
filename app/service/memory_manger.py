class MemoryManager:
    def __init__(self, redis_mem, sql_mem):
        self.redis_mem = redis_mem
        self.sql_mem = sql_mem

    def load_session(self, user_id, session_id):
        """Load from SQL → Redis when session starts"""
        messages = self.sql_mem.get_messages(user_id, session_id)
        if not messages:
            return []
        for msg in messages:
            self.redis_mem.add_message(session_id, msg)
        self.redis_mem.trim_messages(session_id)
        return messages

    def add_message(self, user_id, session_id, message):
        """Add message to Redis"""
        self.redis_mem.add_message(session_id, message)
        self.redis_mem.trim_messages(session_id, limit=20)

    def backup_session(self, user_id, session_id):
        """Save Redis messages → SQL"""
        messages = self.redis_mem.get_messages(session_id)
        self.sql_mem.add_messages(user_id, session_id, messages)
        self.redis_mem.delete_session(session_id)
