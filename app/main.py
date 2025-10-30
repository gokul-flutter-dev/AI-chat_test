import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import psycopg
import asyncio
import json
from langchain_core.messages import HumanMessage, AIMessage
from langchain_mistralai import ChatMistralAI
from langchain_postgres import PostgresChatMessageHistory
from pydantic import SecretStr
from langchain_redis import RedisChatMessageHistory

from app.database.redis_client import RedisMemory
from app.database.sql_db import CustomPostgresChatMessageHistory
from app.service.memory_manger import MemoryManager

# 👈 your class here

# ------------------------------------------------------------------
# 🔧 Database + Model Setup
# ------------------------------------------------------------------
DB_URL = "postgresql://postgres:gokul123@localhost:5432/your_db"
MODEL_NAME = "mistral-large-latest"
MISTRAL_API_KEY = "uGDeFhRsVqijNfGWTxXOR9J0fWmHQEuQ"

apikey = SecretStr(MISTRAL_API_KEY)
# Connect to Postgres
conn = psycopg.connect(DB_URL)
redis_mem = RedisMemory()

manager = MemoryManager(redis_mem, conn)
# FastAPI app
app = FastAPI()

# Initialize Mistral client
llm = ChatMistralAI(model_name="mistral-small",api_key=apikey,temperature=0.5)

# ------------------------------------------------------------------
# 🚀 POST /chat - Stream AI response + store messages
# ------------------------------------------------------------------


@app.post("/chat")
async def chat(request: Request):
    body = await request.json()
    user_id = body.get("user_id")
    session_id = body.get("session_id")
    user_message = body.get("message")

    if not all([user_id, session_id, user_message]):
        return JSONResponse({"error": "Missing user_id, session_id, or message"}, status_code=400)

    # Initialize chat history
    history = CustomPostgresChatMessageHistory(
        table_name="chat_history_custom",
        user_id=user_id,
        session_id=session_id,
        sync_connection=conn,
    )

    # Add user's message
    history.add_messages([HumanMessage(content=user_message)])

    # Get past messages
    messages = history.get_messages()

    # Ask Mistral (streaming)
    async def response_stream():
        buffer = ""
        async for chunk in llm.astream(messages + [HumanMessage(content=user_message)]):
            delta = chunk.content or ""
            buffer += delta
            yield delta
        # Save final AI message to DB
        history.add_messages([AIMessage(content=buffer)])

    return StreamingResponse(response_stream(), media_type="text/plain")

# ------------------------------------------------------------------
# 📜 GET /history/{user_id}/{session_id}
# ------------------------------------------------------------------
@app.get("/history/{user_id}/{session_id}")
async def get_history(user_id: str, session_id: str):
    history = CustomPostgresChatMessageHistory(
        table_name="chat_history_custom",
        user_id=user_id,
        session_id=session_id,
        sync_connection=conn,
    )
    messages = history.get_messages()
    return JSONResponse([{"type": m.type, "content": m.content} for m in messages])
