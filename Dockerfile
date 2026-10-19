FROM python:3.10-slim

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render는 서버 포트를 자체적으로 할당하므로 $PORT 변수를 사용합니다.
CMD uvicorn app:app --host 0.0.0.0 --port $PORT
