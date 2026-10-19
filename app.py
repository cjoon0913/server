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

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# ★ 중요: 채널 핸들(@sbsnews/live) 대신 유튜브에서 지금 재생 중인 SBS 라이브의 실제 영상 주소(watch?v=...)를 넣는 것이 가장 확실합니다.
TARGET_YOUTUBE_URL = "https://www.youtube.com/watch?v=ilWUxgMNEiY"

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
    except Exception as e:
        print(f"[번역 실패] {e}", flush=True)
    return text

async def broadcast(data: dict):
    for ws in list(active_connections):
        try:
            await ws.send_json(data)
        except Exception:
            active_connections.remove(ws)

def extract_stream_url(url: str):
    cmd = ["yt-dlp", "-g", "-f", "ba/b", "--no-warnings", url]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
    return res.returncode, res.stdout.strip(), res.stderr.strip()

def record_chunk(stream_url: str, outfile: str, duration: int = 5):
    cmd = [
        "ffmpeg", "-y", "-i", stream_url,
        "-t", str(duration), "-ar", "16000", "-ac", "1",
        "-c:a", "pcm_s16le", outfile
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    return res.returncode

async def live_audio_pipeline():
    chunk_file = "stream_chunk.wav"
    print(f"[파이프라인 가동] 대상: {TARGET_YOUTUBE_URL}", flush=True)
    
    while True:
        try:
            print("[1] 유튜브 오디오 스트림 주소 파싱 중...", flush=True)
            code, stream_url, err = await asyncio.to_thread(extract_stream_url, TARGET_YOUTUBE_URL)
            
            if code != 0 or not stream_url:
                print(f"[오류: yt-dlp 실패] {err[:150]}", flush=True)
                print("[안내] 채널 주소 대신 실제 재생 영상 링크(watch?v=...)를 사용해 주세요. 15초 후 재시도합니다.", flush=True)
                await asyncio.sleep(15)
                continue

            print("[2] 스트림 주소 획득 성공! 5초 슬라이스 수신 시작", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[재연결 필요] 스트림이 만료되었거나 끊겼습니다. 주소를 재갱신합니다.", flush=True)
                    break

                with open(chunk_file, "rb") as f:
                    transcription = groq_client.audio.transcriptions.create(
                        file=(chunk_file, f.read()),
                        model="whisper-large-v3",
                        language="ko",
                        response_format="text"
                    ).strip()

                if len(transcription) >= 2:
                    print(f"🎙️ [음성 인식] {transcription}", flush=True)
                    en_trans = await translate_text(transcription, "en")
                    print(f"🌐 [번역 완료] {en_trans}", flush=True)
                    await broadcast({
                        "original": transcription,
                        "translated": en_trans,
                        "time": "LIVE"
                    })
                else:
                    print("🔇 [음성 없음/배경음악]", flush=True)

        except Exception as e:
            print(f"[파이프라인 예외] {e}", flush=True)
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
