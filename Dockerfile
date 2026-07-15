# YoinkT — single image for localhost + kubernetes
FROM python:3.12-slim AS base

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg aria2 curl unzip ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Deno = JS runtime yt-dlp uses to solve YouTube's n-throttle challenge. Without
# a JS runtime, YouTube can drop downloads to ~50 KB/s. Installed to /usr/local/bin.
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
 && deno --version

WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app.py .
COPY backend/static ./static

# non-root
RUN useradd -r -u 10001 app
USER app

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/api/health')"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
