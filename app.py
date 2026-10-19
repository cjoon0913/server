import os
import asyncio
import subprocess
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from groq import Groq

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Render 환경 변수에서 로드
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
# 감시할 대상 유튜브 채널 (★원하시는 채널 핸들로 변경하세요)
TARGET_YOUTUBE_URL = "https://www.youtube.com/@ytnnews24/live"

groq_client = Groq(api_key=GROQ_API_KEY)
active_connections: list[WebSocket] = []

async def translate_text(text: str, target_lang: str = "en") -> str:
    try:
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=ko&tl={target_lang}&dt=t&q={text}"
        async with httpx.AsyncClient() as client:
            res = await client.get(url, timeout=5.0)
            data = res.json()
            if data and data[0]:
                return "".join([part[0] for part in data[0]])
    except Exception:
        pass
    return text

async def broadcast(data: dict):
    for ws in list(active_connections):
        try:
            await ws.send_json(data)
        except Exception:
            active_connections.remove(ws)

async def live_audio_pipeline():
    chunk_file = "stream_chunk.wav"
    while True:
        try:
            proc = subprocess.run(
                ["yt-dlp", "-g", "-f", "bestaudio", TARGET_YOUTUBE_URL],
                capture_output=True, text=True, timeout=20
            )
            audio_stream_url = proc.stdout.strip()

            if not audio_stream_url or "m3u8" not in audio_stream_url:
                await asyncio.sleep(20)
                continue

            cmd = [
                "ffmpeg", "-y", "-i", audio_stream_url,
                "-t", "5", "-ar", "16000", "-ac", "1",
                "-c:a", "pcm_s16le", chunk_file
            ]
            res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            if res.returncode != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) == 0:
                await asyncio.sleep(2)
                continue

            with open(chunk_file, "rb") as f:
                transcription = groq_client.audio.transcriptions.create(
                    file=(chunk_file, f.read()),
                    model="whisper-large-v3",
                    language="ko",
                    response_format="text"
                ).strip()

            if len(transcription) >= 2:
                en_trans = await translate_text(transcription, "en")
                await broadcast({
                    "original": transcription,
                    "translated": en_trans,
                    "time": "LIVE"
                })

        except Exception as e:
            print("파이프라인 로그:", e)
            await asyncio.sleep(10)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(live_audio_pipeline())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)