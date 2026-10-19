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
TARGET_YOUTUBE = "https://www.youtube.com/watch?v=ilWUxgMNEiY"

groq_client = Groq(api_key=GROQ_API_KEY)
active_connections: list[WebSocket] = []

# Edge 브라우저 User-Agent (쿠키 세션 불일치 에러 방지용)
EDGE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0"

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

def get_cookie_file():
    for path in ["/etc/secrets/cookies.txt", "cookies.txt"]:
        if os.path.exists(path) and os.path.getsize(path) > 10:
            return path
    return None

# 1. Innertube ANDROID_VR 직결 (봇 검증을 건너뛰는 전용 API)
async def fetch_innertube_vr(video_id: str) -> str | None:
    api_url = "https://www.youtube.com/youtubei/v1/player?prettyPrint=false"
    payload = {
        "context": {
            "client": {
                "clientName": "ANDROID_VR",
                "clientVersion": "1.56.21",
                "deviceMake": "Oculus",
                "deviceModel": "Quest 3",
                "osName": "Android",
                "osVersion": "12",
                "hl": "ko",
                "gl": "KR"
            }
        },
        "videoId": video_id
    }
    headers = {
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; Quest 3 Build/SQ3A.220605.009.A1)",
        "Content-Type": "application/json"
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(api_url, json=payload, headers=headers)
            if res.status_code == 200:
                data = res.json()
                playability = data.get("playabilityStatus", {})
                status = playability.get("status")
                reason = playability.get("reason", "")
                
                print(f"[Innertube VR 진단] 상태: {status} / 사유: {reason}", flush=True)

                if status == "OK":
                    streaming = data.get("streamingData", {})
                    if "hlsManifestUrl" in streaming:
                        return streaming["hlsManifestUrl"]
                    for fmt in streaming.get("adaptiveFormats", []):
                        if "audio" in fmt.get("mimeType", "") and "url" in fmt:
                            return fmt["url"]
    except Exception as e:
        print(f"[Innertube VR 요청 예외] {e}", flush=True)
    return None

# 2. yt-dlp android_vr 클라이언트 우회
def fetch_ytdlp_vr(url: str) -> str | None:
    cmd = [
        "yt-dlp", "-g", "-f", "ba/b",
        "--extractor-args", "youtube:player_client=android_vr;player_skip=webpage,configs",
        "--no-warnings", url
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    lines = [l.strip() for l in res.stdout.strip().splitlines() if l.strip()]
    for line in lines:
        if "m3u8" in line or line.startswith("http"):
            return line
    if res.stderr.strip():
        print(f"[yt-dlp VR 오류] {res.stderr.strip()[:120]}", flush=True)
    return None

# 3. yt-dlp 쿠키 + Edge 헤더 결합 폴백
def fetch_ytdlp_cookie_edge(url: str) -> str | None:
    cookie_path = get_cookie_file()
    if not cookie_path:
        return None

    cmd = [
        "yt-dlp", "-g", "-f", "ba/b",
        "--cookies", cookie_path,
        "--user-agent", EDGE_USER_AGENT,
        "--extractor-args", "youtube:player_client=mweb;player_skip=webpage",
        "--no-warnings", url
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    lines = [l.strip() for l in res.stdout.strip().splitlines() if l.strip()]
    for line in lines:
        if "m3u8" in line or line.startswith("http"):
            return line
    if res.stderr.strip():
        print(f"[yt-dlp 쿠키+Edge 오류] {res.stderr.strip()[:120]}", flush=True)
    return None

async def resolve_stream(url_or_id: str) -> str | None:
    video_id = extract_video_id(url_or_id)
    watch_url = f"https://www.youtube.com/watch?v={video_id}"

    # 1단계: ANDROID_VR Innertube 직결
    stream = await fetch_innertube_vr(video_id)
    if stream:
        print(">> [성공] ANDROID_VR 직결로 라이브 스트림 획득", flush=True)
        return stream

    # 2단계: yt-dlp VR 클라이언트
    stream = await asyncio.to_thread(fetch_ytdlp_vr, watch_url)
    if stream:
        print(">> [성공] yt-dlp android_vr 클라이언트로 스트림 획득", flush=True)
        return stream

    # 3단계: 쿠키 + Edge User-Agent 결합
    stream = await asyncio.to_thread(fetch_ytdlp_cookie_edge, watch_url)
    if stream:
        print(">> [성공] yt-dlp 쿠키/Edge 결합으로 스트림 획득", flush=True)
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
    print(f"[파이프라인 가동] 대상: {TARGET_YOUTUBE}", flush=True)

    while True:
        try:
            print("[1] 라이브 오디오 스트림 탐색 시작...", flush=True)
            stream_url = await resolve_stream(TARGET_YOUTUBE)

            if not stream_url:
                print("[대기] 스트림 획득 실패. 6초 후 재시도합니다.", flush=True)
                await asyncio.sleep(6)
                continue

            print("[2] 실시간 오디오 슬라이스 수신 및 통역 가동!", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[스트림 만료] 새 스트림 주소로 갱신합니다.", flush=True)
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
                    print("🔇 [음성 없음 / 묵음 구간]", flush=True)

        except Exception as e:
            print(f"[파이프라인 예외 발생] {e}", flush=True)
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
