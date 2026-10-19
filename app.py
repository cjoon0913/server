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

# 데이터센터 IP 봇 차단 우회 추출 함수
def extract_stream_url(url: str):
    # 1차 시도: iOS 모바일 클라이언트로 봇 검증 우회
    cmd_ios = [
        "yt-dlp", "-g", "-f", "ba/b",
        "--extractor-args", "youtube:player_client=ios,android,mweb",
        "--no-warnings", url
    ]
    res = subprocess.run(cmd_ios, capture_output=True, text=True, timeout=25)
    lines = [line.strip() for line in res.stdout.strip().splitlines() if line.strip()]
    if lines:
        for line in lines:
            if "m3u8" in line:
                return 0, line, ""
        return 0, lines[0], ""

    # 2차 시도: TV 임베디드 클라이언트로 2중 우회
    cmd_tv = [
        "yt-dlp", "-g", "-f", "ba/b",
        "--extractor-args", "youtube:player_client=tv_embedded,web_embedded",
        "--no-warnings", url
    ]
    res_tv = subprocess.run(cmd_tv, capture_output=True, text=True, timeout=25)
    lines_tv = [line.strip() for line in res_tv.stdout.strip().splitlines() if line.strip()]
    if lines_tv:
        for line in lines_tv:
            if "m3u8" in line:
                return 0, line, ""
        return 0, lines_tv[0], ""

    return res.returncode, "", res.stderr.strip()

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
    print(f"[파이프라인 가동] 대상: {TARGET_YOUTUBE_URL}", flush=True)

    while True:
        try:
            print("[1] 유튜브 오디오 스트림 주소 파싱 중 (모바일 우회 적용)...", flush=True)
            code, stream_url, err = await asyncio.to_thread(extract_stream_url, TARGET_YOUTUBE_URL)

            if code != 0 or not stream_url:
                print(f"[오류: yt-dlp 실패] {err[:150]}", flush=True)
                await asyncio.sleep(10)
                continue

            print("[2] 스트림 주소 획득 성공! 5초 슬라이스 수신 시작", flush=True)

            while True:
                rec_code = await asyncio.to_thread(record_chunk, stream_url, chunk_file, 5)
                if rec_code != 0 or not os.path.exists(chunk_file) or os.path.getsize(chunk_file) < 1000:
                    print("[스트림 재연결] 주소를 새로 갱신합니다.", flush=True)
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
                    print("🔇 [음성 없음]", flush=True)

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
