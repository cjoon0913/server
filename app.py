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

# SBS 24시간 실시간 뉴스 공식 온에어 (유튜브 ilWUxgMNEiY와 동일 방송본, 봇 차단 없음)
TARGET_STREAM_URL = "https://news.sbs.co.kr/news/live.do"

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
        print(f"[번역 예외] {e}", flush=True)
    return text

async def broadcast(data: dict):
    for ws in list(active_connections):
        try:
            await ws.send_json(data)
        except Exception:
            active_connections.remove(ws)

# 실시간 오디오 스트림(m3u8) 파싱 함수
def extract_live_stream(url: str):
    cmd = [
        "yt-dlp", "-g", "-f", "bestaudio/best",
        "--no-warnings", url
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    lines = [l.strip() for l in res.stdout.strip().splitlines() if l.strip()]
    for line in lines:
        if "m3u8" in line or line.startswith("http"):
            return 0, line, ""
    return res.returncode, "", res.stderr.strip()

# 실시간 5초 오디오 슬라이스 추출
def record_chunk(stream_url: str, outfile: str, duration: int = 5):
    cmd = [
        "ffmpeg", "-y",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "3",
        "-i", stream_url,
        "-t", str(duration), "-ar", "16000", "-ac", "1",
        "-c:a", "pcm_s16le", outfile
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    return res.returncode

async def live_audio_pipeline():
    chunk_file = "stream_chunk.wav"
    print(f"[파이프라인 가동] 대상: {TARGET_STREAM_URL}", flush=True)

    while True:
        try:
            print("[1] 라이브 스트림 오디오 HLS 주소 추출 중...", flush=True)
            code, stream_url, err = await asyncio.to_thread(extract_live_stream, TARGET_STREAM_URL)

            if code != 0 or not stream_url:
                print(f"[스트림 추출 실패] {err[:120]}", flush=True)
                await asyncio.sleep(8)
                continue

            print("[2] 실시간 오디오 스트림 직결 성공! 5초 슬라이스 전사 시작", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[스트림 만료/재연결] 새 스트림 주소로 갱신합니다.", flush=True)
                    break

                # Groq Whisper-large-v3 초고속 STT (약 0.3초 소요)
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
                    print("🔇 [음성 없음 / 묵음 구간]", flush=True)

        except Exception as e:
            print(f"[파이프라인 예외] {e}", flush=True)
            await asyncio.sleep(5)

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
