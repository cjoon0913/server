import os
import re
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
# 유튜브 라이브 전체 URL 또는 11자리 영상 ID 모두 지원
TARGET_YOUTUBE = "https://www.youtube.com/watch?v=ilWUxgMNEiY"

groq_client = Groq(api_key=GROQ_API_KEY)
active_connections: list[WebSocket] = []

def extract_video_id(url_or_id: str) -> str:
    url_or_id = url_or_id.strip()
    match = re.search(r"(?:v=|\/live\/|\/embed\/|youtu\.be\/|^)([a-zA-Z0-9_-]{11})", url_or_id)
    return match.group(1) if match else url_or_id

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

# 1순위: Google 공식 Innertube API 직결 (봇 검증 완전 제외 클라이언트)
async def fetch_via_innertube(video_id: str) -> str | None:
    api_url = "https://www.youtube.com/youtubei/v1/player?key=AIzaSyAO_FJ2SlqU8Q4usvWRL55afuZ0fiWm3UU&prettyPrint=false"
    
    clients = [
        {
            "name": "ANDROID_CREATOR",
            "context": {"client": {"clientName": "ANDROID_CREATOR", "clientVersion": "23.49.100", "androidSdkVersion": 31, "hl": "ko", "gl": "KR"}},
            "headers": {"User-Agent": "com.google.android.apps.youtube.creator/23.49.100 (Linux; U; Android 11)"}
        },
        {
            "name": "TVHTML5",
            "context": {"client": {"clientName": "TVHTML5", "clientVersion": "7.20230405.08.01"}},
            "headers": {"User-Agent": "Mozilla/5.0 (SMART-TV; Linux; Tizen 6.0) AppleWebKit/538.1"}
        },
        {
            "name": "WEB_EMBEDDED",
            "context": {
                "client": {"clientName": "WEB_EMBEDDED_PLAYER", "clientVersion": "1.20240306.01.00"},
                "thirdParty": {"embedUrl": f"https://www.youtube.com/embed/{video_id}"}
            },
            "headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Referer": f"https://www.youtube.com/embed/{video_id}"}
        }
    ]

    async with httpx.AsyncClient(timeout=8.0) as http_client:
        for c in clients:
            try:
                payload = {"context": c["context"], "videoId": video_id}
                res = await http_client.post(api_url, json=payload, headers=c["headers"])
                if res.status_code == 200:
                    data = res.json()
                    playability = data.get("playabilityStatus", {})
                    if playability.get("status") == "OK":
                        streaming = data.get("streamingData", {})
                        if "hlsManifestUrl" in streaming:
                            print(f"[{c['name']}] HLS 라이브 스트림 주소 획득 성공!", flush=True)
                            return streaming["hlsManifestUrl"]
                        for fmt in streaming.get("adaptiveFormats", []):
                            if "audio" in fmt.get("mimeType", "") and "url" in fmt:
                                print(f"[{c['name']}] 오디오 스트림 획득 성공!", flush=True)
                                return fmt["url"]
            except Exception as e:
                print(f"[{c['name']} 시도 중 오류] {e}", flush=True)
    return None

# 2순위: yt-dlp android_creator / tv 엔진 폴백
def fetch_via_ytdlp(video_id: str) -> str | None:
    strategies = [
        "youtube:player_client=android_creator;player_skip=webpage,configs",
        "youtube:player_client=tv;player_skip=webpage,configs"
    ]
    for client_arg in strategies:
        try:
            cmd = [
                "yt-dlp", "-g", "-f", "ba/b",
                "--extractor-args", client_arg,
                "--no-warnings",
                f"https://www.youtube.com/watch?v={video_id}"
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            lines = [l.strip() for l in res.stdout.strip().splitlines() if l.strip()]
            for line in lines:
                if "m3u8" in line or line.startswith("http"):
                    print(f"[yt-dlp {client_arg.split(';')[0]}] 스트림 획득 성공!", flush=True)
                    return line
        except Exception:
            pass
    return None

async def get_live_stream_url(video_id: str) -> str | None:
    # 1. Innertube 공식 직결
    stream = await fetch_via_innertube(video_id)
    if stream:
        return stream

    # 2. yt-dlp 우회 엔진 폴백
    stream = await asyncio.to_thread(fetch_via_ytdlp, video_id)
    if stream:
        return stream

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
    video_id = extract_video_id(TARGET_YOUTUBE)
    print(f"[파이프라인 가동] 대상 영상 ID: {video_id}", flush=True)

    while True:
        try:
            print("[1] YouTube Innertube 직결로 실시간 오디오 스트림 주소 파싱 중...", flush=True)
            stream_url = await get_live_stream_url(video_id)

            if not stream_url:
                print("[재시도] 스트림 주소 탐색 중... 5초 후 다시 시도합니다.", flush=True)
                await asyncio.sleep(5)
                continue

            print("[2] 실시간 오디오 스트림 수신 시작 (5초 슬라이스)", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[스트림 만료] 스트림 주소를 갱신합니다.", flush=True)
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
                    print("🔇 [음성 없음 / 배경음]", flush=True)

        except Exception as e:
            print(f"[파이프라인 루프 오류] {e}", flush=True)
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
