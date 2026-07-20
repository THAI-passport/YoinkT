# YoinkT 🎬⬇️

<p align="center">
  <img src="image/logo.png" alt="YoinkT Logo" width="350" />
</p>

> **Grab it and go!** A stateless, streaming media download service for YouTube, X, Facebook, Instagram, and TikTok.

YoinkT allows you to paste a URL and instantly stream video or audio in the resolution you choose. **Nothing touches the disk** — media is piped straight through the server to the client. This means zero storage costs and easy horizontal scaling!

---

## ✨ Features

- **Stateless Streaming**: No temp files, no queues. Video and audio are piped directly using `yt-dlp` and `ffmpeg`.
- **Multi-Platform Support**: Works with YouTube, X (Twitter), Facebook, Instagram, and TikTok.
- **Auto-merge**: Merges the best video and audio streams seamlessly on the fly (for 1080p+).
- **Docker & Kubernetes Ready**: Run it locally in seconds, or scale it on a k8s cluster.
- **Zero-dependency UI**: Ships with a beautiful, fast Vanilla JS frontend (`backend/static/index.html`).

## 🚀 How it works

1. `GET /api/info?url=...` — Extracts metadata and returns a curated format list (one best option per resolution + audio-only).
2. `GET /api/download?url=...&kind=...` — Streams the file to the client:
   - `p:<id>` (Progressive, ≤720p): Pipes a single file to stdout.
   - `m:<height>` (Merged, 1080p+): `ffmpeg` stream-copies best video + best audio into MKV, piped to stdout (no re-encode).
   - `a:audio`: Best audio passthrough.

## 💻 Running Locally

### Using Docker (Recommended)
```bash
docker compose up --build
# Open http://localhost:8000
```

### Native Python (Requires `ffmpeg` on PATH)
```bash
cd backend
pip install -r requirements.txt
uvicorn app:app --reload
```

### Quick Start Script

`./run-local.sh [youtube-url]` verifies the source, stops any stale server, starts the app (Docker if available, else native Python), and runs a self-test — optionally including a real download test if you pass a URL.

## ☸️ Kubernetes Deployment

```bash
docker build -t <registry>/YoinkT:1 . && docker push <registry>/YoinkT:1
# Edit image in k8s/deployment.yaml, host in k8s/ingress.yaml
kubectl apply -f k8s/
```

### Stealth profiles

Some platforms fingerprint the **TLS handshake**, not just the User-Agent — a stock Python HTTPS client is identifiable as "not a browser" before any HTTP is sent, and gets checkpointed no matter how good your cookies are. YoinkT ships browser-grade handshakes, on by default for X / Facebook / Instagram / TikTok and deliberately **off for YouTube** (PO tokens are YouTube's lever there, and a mismatched handshake can lose you the full-speed formats).

```bash
STEALTH_PROFILES="x=chrome,instagram=safari"   # override per site
STEALTH_PROFILES=off                           # disable entirely
```

If a request is refused with a client-level block ("confirm you're not a bot", "rate-limit reached", checkpoint), YoinkT retries it **once** with a profile before giving up. The transport is an optional dependency: if it isn't installed, everything falls back to the stock client rather than erroring. `GET /api/health` reports what's actually active per site.

### Engine freshness

The extraction engine rots — platforms change their internals and a build from last month stops working. It's the single most common cause of "it worked yesterday".

The image **pins its engine build on purpose.** Freshness only pays off when extraction actually breaks, whereas every engine bump is a throughput risk — this project has already lost half its 1080p speed once to an engine-side client change. So moving forward is a decision, not a default:

```bash
docker build .                                      # pinned (default)
docker build --build-arg ENGINE_VERSION=2026.8.1 .  # a specific build
docker build --build-arg ENGINE_VERSION="" .        # latest, incl. same-day builds
```

What earns the right to pin is the rest of the setup:

- **CI runs a weekly nightly canary** — the same test suite against the newest engine build. It never gates the build; a red canary means "look before you bump".
- **`GET /api/health` reports the shipped build**, its age in days, and a `stale` flag past `ENGINE_STALE_DAYS` (default 21). Reporting only, nothing is blocked.

```bash
curl -s localhost:8000/api/health | python -m json.tool
# "engine": {"version": "2026.7.4", "age_days": 16, "stale": false, "channel": "stable", ...}
```

When extraction starts failing on a site, that's the signal to bump the pin — not the calendar.

**Bot-Blocking Note**: Datacenter IPs are often blocked by YouTube. To fix this, you can configure either:
- **`PROXY_URL`**: Route traffic through a residential proxy.
- **`COOKIES_FILE`**: Mount a `cookies.txt` (from a logged-in browser) as a Kubernetes Secret:
  `kubectl create secret generic YoinkT-cookies --from-file=cookies.txt`

## ⚙️ Configuration (Environment Variables)

| Env | Default | Purpose |
|-----|---------|---------|
| `PROXY_URL` | unset | Proxy for `yt-dlp` and `ffmpeg` |
| `COOKIES_FILE` | unset | Path to global `cookies.txt` fallback |
| `COOKIES_FILE_{SITE}`| unset | Per-site cookies (e.g., `YOUTUBE`, `INSTAGRAM`) |
| `ENABLED_SITES` | `youtube,x,facebook,instagram,tiktok` | Supported sites |
| `API_KEY` | unset | If set, requires `?key=` or `X-API-Key` on `/api` routes |
| `RATE_LIMIT_RPM` | `0` | Per-IP requests/min (Abuse guard for public deployments) |
| `MAX_CONCURRENCY` | `4` | Simultaneous extractions/streams per instance |

## 🛠 Frontend Development

The default UI is a hand-bundled vanilla JS file located at `backend/static/index.html`. 
There is also a React + TypeScript version in `frontend/`. To build and replace the vanilla UI:

```bash
cd frontend
npm install && npm run build
cp -r dist/* ../backend/static/
```

## 🧪 Testing

Tests live in `tests/`:
- `test_x_offline.py` — offline unit tests, no network required.
- `smoke_live.py` — live smoke test against real sites (requires network access).

```bash
cd backend && pip install -r requirements.txt
pytest ../tests/test_x_offline.py
```

## ⚠️ Legal & Maintenance

- **Maintenance**: `yt-dlp` breaks when YouTube changes its APIs. Rebuilding the docker image regularly ensures you pull the latest `yt-dlp` version.
- **Legal**: YouTube ToS prohibits downloading without permission. Use this tool for your own content, Creative Commons, or where you have rights. Public deployment is at your own risk.
