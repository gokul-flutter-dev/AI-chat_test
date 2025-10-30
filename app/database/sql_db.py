import json
from typing import Sequence, List, Optional
from psycopg import sql
from psycopg import AsyncConnection, Connection
from langchain_core.messages import BaseMessage, messages_to_dict, messages_from_dict, message_to_dict
from langchain_postgres.chat_message_histories import PostgresChatMessageHistory


class CustomPostgresChatMessageHistory(PostgresChatMessageHistory):
    """Extended PostgresChatMessageHistory that includes user_id."""

    def __init__(
        self,
        table_name: str,
        user_id: str,
        session_id: str,
        *,
        sync_connection: Optional[Connection] = None,
        async_connection: Optional[AsyncConnection] = None
    ) -> None:
        # Initialize parent (to keep interface consistent)
        super().__init__(
             table_name,
            session_id,
            sync_connection=sync_connection,
            async_connection=async_connection,
        )
        self.user_id = user_id
        self.session_id = session_id
        self.table_name = table_name
        self.sync_connection = sync_connection
        self.async_connection = async_connection
        self._ensure_table_with_user_id()

    # -------------------------------------------------------------------------
    # 🔧 Override schema to include user_id
    # -------------------------------------------------------------------------
    def _ensure_table_with_user_id(self):
        """Ensure our custom table (with user_id) exists."""
        ddl =sql.SQL( """
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            message JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS {table_name}
            ON {table_name} (user_id, session_id);
        """).format(
            table_name=sql.Identifier(self.table_name),
        )
        if self.sync_connection:
            with self.sync_connection.cursor() as cur:
                cur.execute(ddl)
                self.sync_connection.commit()
        elif self.async_connection:
            # async table creation if needed
            async def _async_create():
                async with self.async_connection.cursor() as cur:
                    await cur.execute(ddl)
                    await self.async_connection.commit()
            import asyncio
            asyncio.get_event_loop().run_until_complete(_async_create())

    # -------------------------------------------------------------------------
    # 💾 Store messages
    # -------------------------------------------------------------------------
    def add_messages(self, messages: Sequence[BaseMessage]) -> None:
        if not self.sync_connection:
            raise RuntimeError("Sync connection required for add_messages")

        with self.sync_connection.cursor() as cur:
            for msg in messages:
                msg_dict = message_to_dict(msg)
                cur.execute(
                    sql.SQL("""
                    INSERT INTO {table_name} (user_id, session_id, message)
                    VALUES (%s, %s, %s)
                    """).format(
                        table_name=sql.Identifier(self.table_name),
                    )
                    ,
                    (self.user_id, self.session_id, json.dumps(msg_dict)),
                )
        self.sync_connection.commit()

    # -------------------------------------------------------------------------
    # 📖 Retrieve messages
    # -------------------------------------------------------------------------
    def get_messages(self) -> List[BaseMessage]:
        if not self.sync_connection:
            raise RuntimeError("Sync connection required for get_messages")

        with self.sync_connection.cursor() as cur:
            cur.execute(
                sql.SQL("""
                SELECT message
                FROM {table_name}
                WHERE user_id = %s AND session_id = %s
                ORDER BY id ASC
                """)
                .format(
                    table_name=sql.Identifier(self.table_name),
                ),
                (self.user_id, self.session_id),
            )
            rows = cur.fetchall()
            print(rows)

        messages_json = [r[0] for r in rows]
        return messages_from_dict(messages_json)

    # -------------------------------------------------------------------------
    # 🧹 Clear messages
    # -------------------------------------------------------------------------
    def clear(self) -> None:
        if not self.sync_connection:
            raise RuntimeError("Sync connection required for clear")

        with self.sync_connection.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {self.table_name} WHERE user_id = %s AND session_id = %s"),
                (self.user_id, self.session_id),
            )
        self.sync_connection.commit()
