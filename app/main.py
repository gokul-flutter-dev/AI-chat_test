import os

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, Request,WebSocket
from fastapi.responses import StreamingResponse, JSONResponse
from langchain_core.messages import HumanMessage, AIMessage, messages_from_dict, messages_to_dict
from langchain_mistralai import ChatMistralAI
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect

from app.database.postgres_memory import PostgresMemory
from app.database.redis_client import RedisMemor
from app.database.sql_db import CustomPostgresChatMessageHistory
from app.service.memory_manger import MemoryManager

# 👈 your class here

# ------------------------------------------------------------------
# 🔧 Database + Model Setup
# ------------------------------------------------------------------
load_dotenv()
DB_URL = "postgresql://postgres:gokul123@localhost:5432/your_db"
MODEL_NAME = "mistral-large-latest"
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
print(MISTRAL_API_KEY)
apikey = SecretStr(MISTRAL_API_KEY)
# Connect to Postgres
conn = psycopg.connect(DB_URL)
redis_mem = RedisMemor()
sql_mem = PostgresMemory(conn)
manager = MemoryManager(redis_mem, sql_mem)
# FastAPI app
app = FastAPI()

# Initialize Mistral client
llm = ChatMistralAI(model_name="mistral-small-latest",api_key=apikey,temperature=0.5)

# ------------------------------------------------------------------
# 🚀 POST /chat - Stream AI response + store messages
# ------------------------------------------------------------------

@app.post("/chat/stream")
async def chat_stream(request: Request):
    body = await request.json()
    user_id = body.get("user_id")
    session_id = body.get("session_id")
    user_message = body.get("message")

    # Load session
    manager.load_session(user_id, session_id)

    # Get history from Redis
    messages_dict = manager.redis_mem.get_messages(session_id)
    messages = messages_from_dict(messages_dict)

    # Add user message
    manager.add_message(user_id, session_id, messages_to_dict([HumanMessage(content=user_message)])[0])


    async def generate():
        buffer = ""
        async for chunk in llm.astream(messages + [HumanMessage(content=user_message)]):
            delta = chunk.content or ""
            buffer += delta
            yield delta

        # Save AI message
        manager.add_message(user_id, session_id, messages_to_dict([AIMessage(content=buffer)])[0])
        # Backup to Postgres
        manager.backup_session(user_id, session_id)

    return StreamingResponse(generate(), media_type="text/plain")

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

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Message text was: {data}")
    except WebSocketDisconnect:
            await websocket.close()
            print("Disconnected connection")


@app.websocket("/chat/stream")
async def chat_stream(websocket: WebSocket):
    await websocket.accept()
    user_id = None
    session_id = None
    buffer = ""

    try:
        while True:
            # 1️⃣ Receive message from client
            data = await websocket.receive_json()
            print(data)
            user_id = data.get("user_id")
            session_id = data.get("session_id")
            user_message = data.get("message")

            print(user_id, session_id, user_message)

            # 2️⃣ Load session from manager
            d=manager.load_session(user_id, session_id)

            print(d)

            # 3️⃣ Get existing history from Redis
            messages_dict = manager.redis_mem.get_messages(session_id)
            print(messages_dict)
            messages = messages_from_dict(messages_dict)

            print(messages_dict)
            # 4️⃣ Add user message to memory
            manager.add_message(
                user_id, session_id,
                messages_to_dict([HumanMessage(content=user_message)])[0]
            )

            # 5️⃣ Stream LLM output live
            buffer = ""
            async for chunk in llm.astream(messages + [HumanMessage(content=user_message)]):
                delta = chunk.content or ""
                buffer += delta
                await websocket.send_text(delta)

            # 6️⃣ After stream complete, save AI message + backup
            manager.add_message(
                user_id, session_id,
                messages_to_dict([AIMessage(content=buffer)])[0]
            )


            # Notify completion
            await websocket.send_json({"event": "end", "message": buffer})

    except WebSocketDisconnect:
        print(f"Client disconnected: user_id={user_id}, session_id={session_id}")
        # 🧠 Backup if user disconnected mid-stream
        if user_id and session_id:
            try:
                manager.backup_session(user_id, session_id)
                print("✅ Backup completed after disconnect")
            except Exception as e:
                print(f"⚠️ Backup failed on disconnect: {e}")

    except Exception as e:
        print(f"Error: {e}")
        await websocket.send_json({"error": str(e.with_traceback())})

        # 🧠 Always backup on any unexpected error
        if user_id and session_id:
            try:
                manager.backup_session(user_id, session_id)
                print("✅ Backup completed after error")
            except Exception as e:
                print(f"⚠️ Backup failed on error: {e}")

        await websocket.close()