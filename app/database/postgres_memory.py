import json

from langchain_core.messages import message_to_dict, messages_from_dict, BaseMessage
from psycopg import sql

class PostgresMemory:
    def __init__(self, conn, table_name="chat_history"):
        self.conn = conn
        self.table_name = table_name
        self.ensure_table()

    def ensure_table(self):

        ddl =  sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT,
                    session_id TEXT,
                    message JSONB
                );
            """).format(table=sql.Identifier(self.table_name))
        with self.conn.cursor() as cur:
            cur.execute(ddl
             )
            self.conn.commit()

    def get_messages(self, user_id, session_id):
        with self.conn.cursor() as cur:

            cur.execute(
                sql.SQL("""
                    SELECT message FROM {table}
                    WHERE user_id = %s AND session_id = %s
                    ORDER BY id
                """).format(table=sql.Identifier(self.table_name)),
                (user_id, session_id)
            )
            rows = cur.fetchall()
        messages_json = [r[0] for r in rows]
        return messages_from_dict(messages_json)

    def add_messages(self, user_id, session_id, messages):
        print(messages)
        with self.conn.cursor() as cur:
            for msg in messages:
                if isinstance(msg, BaseMessage):
                    msg_json = json.dumps(message_to_dict(msg))
                elif isinstance(msg, dict):
                    msg_json = json.dumps(msg)
                elif isinstance(msg, str):
                    msg_json = msg  # already JSON string
                else:
                    raise TypeError(f"Invalid message type: {type(msg)}")
                cur.execute(
                    sql.SQL("""
                        INSERT INTO {table} (user_id, session_id, message)
                        VALUES (%s, %s, %s)
                    """).format(table=sql.Identifier(self.table_name)),
                    (user_id, session_id, json.dumps(msg_json))
                )
        self.conn.commit()
