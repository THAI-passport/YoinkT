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
# Tracking same-day builds buys freshness, and freshness only pays off when a
# platform change BREAKS extraction. CI already rebuilds weekly and runs a
# nightly canary, so breakage gets caught there instead of here. Meanwhile
# every engine bump is a throughput risk: this project has already lost half
# its 1080p download speed once to an engine-side client change (v38 bug log).
# Continuous churn on the download path is the wrong trade for a service whose
# speed was tuned by hand.
#
# So: ship a known-good build, and treat "move forward" as a decision, not a
# default. /api/health reports the build and its age, which is what tells you
# when to make that decision.
#
#   docker build .                            -> pinned (this default)
#   docker build --build-arg ENGINE_VERSION=2026.8.1 .   -> a specific build
#   docker build --build-arg ENGINE_VERSION="" .         -> latest, incl. same-day
ARG ENGINE_VERSION=2026.07.04
RUN if [ -n "$ENGINE_VERSION" ]; then \
      pip install --no-cache-dir "yt-dlp==${ENGINE_VERSION}"; \
    else \
      pip install --no-cache-dir --pre --upgrade yt-dlp; \
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
