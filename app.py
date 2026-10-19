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

# ★ 목표 유튜브 라이브 스트리밍 URL
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
        print(f"[번역 예외] {e}", flush=True)
    return text

async def broadcast(data: dict):
    for ws in list(active_connections):
        try:
            await ws.send_json(data)
        except Exception:
            active_connections.remove(ws)

def get_cookie_file():
    for path in ["/etc/secrets/cookies.txt", "cookies.txt"]:
        if os.path.exists(path) and os.path.getsize(path) > 10:
            return path
    return None

# 유튜브 라이브 HLS 스트림 전용 추출기 (스마트 TV 클라이언트 + 쿠키 결합)
def extract_youtube_live_stream(url: str):
    cookie_path = get_cookie_file()
    cookie_args = ["--cookies", cookie_path] if cookie_path else []

    # 1차 시도: TV 클라이언트 (리로드 에러 및 봇 챌린지 완전 배제)
    cmd_tv = [
        "yt-dlp", "-g", "-f", "ba/b",
        "--extractor-args", "youtube:player_client=tv",
        *cookie_args,
        "--no-warnings", url
    ]
    res = subprocess.run(cmd_tv, capture_output=True, text=True, timeout=25)
    lines = [l.strip() for l in res.stdout.strip().splitlines() if l.strip()]
    for line in lines:
        if "m3u8" in line or line.startswith("http"):
            print(">> [성공] 유튜브 스마트 TV 클라이언트로 HLS 오디오 스트림 획득!", flush=True)
            return 0, line, ""

    # 2차 시도: 안드로이드 클라이언트 폴백
    cmd_android = [
        "yt-dlp", "-g", "-f", "ba/b",
        "--extractor-args", "youtube:player_client=android",
        *cookie_args,
        "--no-warnings", url
    ]
    res_android = subprocess.run(cmd_android, capture_output=True, text=True, timeout=25)
    lines_android = [l.strip() for l in res_android.stdout.strip().splitlines() if l.strip()]
    for line in lines_android:
        if "m3u8" in line or line.startswith("http"):
            print(">> [성공] 유튜브 안드로이드 클라이언트로 HLS 오디오 스트림 획득!", flush=True)
            return 0, line, ""

    err = res.stderr.strip() or res_android.stderr.strip()
    return 1, "", err

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
    cookie_path = get_cookie_file()
    print(f"[파이프라인 가동] 대상 유튜브: {TARGET_YOUTUBE_URL}", flush=True)
    if cookie_path:
        print(f"[쿠키 확인] {cookie_path} 인증을 사용하여 유튜브 보안을 통과합니다.", flush=True)

    while True:
        try:
            print("[1] 유튜브 라이브 스트림 오디오 HLS 주소 추출 중...", flush=True)
            code, stream_url, err = await asyncio.to_thread(extract_youtube_live_stream, TARGET_YOUTUBE_URL)

            if code != 0 or not stream_url:
                print(f"[스트림 추출 실패] {err[:150]}", flush=True)
                await asyncio.sleep(8)
                continue

            print("[2] 유튜브 라이브 오디오 직결 성공! 5초 슬라이스 전사 시작", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[스트림 만료] 스트림 주소를 새로 갱신합니다.", flush=True)
                    break

                # Groq Whisper-large-v3 초고속 STT (약 0.3초)
                with open(chunk_file, "rb") as f:
                    transcription = groq_client.audio.transcriptions.create(
                        file=(chunk_file, f.read()),
                        model="whisper-large-v3",
                        language="ko",
                        response_format="text"
                    ).strip()

                if len(transcription) >= 2:
                    print(f"🎙️ [유튜브 음성] {transcription}", flush=True)
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
