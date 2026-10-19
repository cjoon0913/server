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
TARGET_VIDEO_ID = "ilWUxgMNEiY"  # SBS 뉴스 라이브 고유 ID

groq_client = Groq(api_key=GROQ_API_KEY)
active_connections: list[WebSocket] = []

# 전 세계 공개 Invidious API 미러 노드 목록 (봇 차단 완전 우회용)
INVIDIOUS_NODES = [
    "https://inv.tux.pizza",
    "https://invidious.nerdvpn.de",
    "https://yewtu.be",
    "https://invidious.jing.rocks",
    "https://invidious.projectsegfau.lt"
]

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

# Invidious 분산 API를 통해 봇 차단 없이 실시간 m3u8 주소 추출
async def fetch_live_stream_url(video_id: str):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
        for node in INVIDIOUS_NODES:
            try:
                api_url = f"{node}/api/v1/videos/{video_id}"
                res = await client.get(api_url, headers=headers)
                if res.status_code == 200:
                    data = res.json()
                    # 1. 실시간 HLS m3u8 주소가 제공되는 경우 (최우선)
                    if "hlsUrl" in data and data["hlsUrl"]:
                        print(f"[{node}] HLS 라이브 스트림 주소 획득 성공!", flush=True)
                        return data["hlsUrl"]
                    
                    # 2. adaptiveFormats에서 오디오 스트림 추출
                    formats = data.get("adaptiveFormats", [])
                    for f in formats:
                        if f.get("type", "").startswith("audio/"):
                            print(f"[{node}] 오디오 스트림 획득 성공!", flush=True)
                            return f.get("url")
            except Exception:
                continue

    # 폴백: yt-dlp web_embedded 직접 시도
    try:
        cmd = [
            "yt-dlp", "-g", "-f", "ba/b",
            "--extractor-args", "youtube:player_client=web_embedded;player_skip=webpage",
            "--referer", "https://www.youtube.com/",
            f"https://www.youtube.com/watch?v={video_id}"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        stream = res.stdout.strip()
        if stream and ("m3u8" in stream or stream.startswith("http")):
            print("[yt-dlp Embedded] 스트림 획득 성공!", flush=True)
            return stream
    except Exception:
        pass

    return None

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
    print(f"[파이프라인 가동] 대상 영상 ID: {TARGET_VIDEO_ID}", flush=True)

    while True:
        try:
            print("[1] 분산 API 노드를 통해 라이브 오디오 스트림 주소 파싱 중...", flush=True)
            stream_url = await fetch_live_stream_url(TARGET_VIDEO_ID)

            if not stream_url:
                print("[재시도] 스트림 주소 탐색 중... 10초 후 다시 시도합니다.", flush=True)
                await asyncio.sleep(10)
                continue

            print("[2] 오디오 스트림 직결 완료! 5초 슬라이스 수신 시작", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[스트림 만료] 스트림 주소를 새로 갱신합니다.", flush=True)
                    break

                # Groq Whisper-large-v3 초고속 STT
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
                    print("🔇 [음성 없음/묵음 구간]", flush=True)

        except Exception as e:
            print(f"[파이프라인 루프 예외] {e}", flush=True)
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
