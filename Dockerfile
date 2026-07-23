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

# Extraction engine build. PINNED BY DEFAULT — this is a deliberate trade.
#
# DEFAULT: track the latest build. This reverses the earlier "pin by default"
# decision after it caused a real outage: a pinned engine went stale and
# YouTube began bot-challenging every request, because YouTube's anti-bot
# internals change faster than a pin gets bumped. For a downloader, engine
# FRESHNESS is a correctness property, not a nice-to-have — a stale engine
# doesn't just run slower, it stops working. So freshness wins by default and
# a pin is the escape hatch for when a specific new build regresses.
#
#   docker build .                                       -> latest (this default)
#   docker build --build-arg ENGINE_VERSION=2026.8.1 .   -> pin a specific build
#
# "latest" and "" both mean newest; a version string pins it. /api/health
# reports the resolved build + age so a stale pin is visible.
ARG ENGINE_VERSION=latest
RUN if [ -z "$ENGINE_VERSION" ] || [ "$ENGINE_VERSION" = "latest" ]; then \
      pip install --no-cache-dir --pre --upgrade yt-dlp; \
    else \
      pip install --no-cache-dir "yt-dlp==${ENGINE_VERSION}"; \
    fi \
 && python -c "import yt_dlp.version as v; print('engine build:', v.__version__)"

COPY backend/app.py .
COPY backend/static ./static

# non-root
RUN useradd -r -u 10001 app
USER app

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/api/health')"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
