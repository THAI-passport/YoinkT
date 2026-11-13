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

### Egress guard

The site allowlist checks the URL you *submit*. It says nothing about the URLs YoinkT then fetches — format URLs from the extractor, CDN URLs from the photo resolver, `og:video`/`og:image` tags scraped from page HTML, and every redirect hop along the way. Those are the ones the server actually opens sockets to, and they're now validated too:

- **scheme allowlist** — http/https only. Python's default URL opener also speaks `file://` and `ftp://`, so a resolved `file:///etc/passwd` would previously have been read and streamed back. That handler is no longer wired up at all.
- **destination denylist** — loopback, RFC1918, link-local (including the `169.254.169.254` cloud-metadata address), unique-local v6, multicast, and reserved ranges are refused.
- **redirects revalidated per hop** — a public CDN URL that 302s to an internal address is stopped mid-chain.

`ALLOW_PRIVATE_EGRESS=1` disables the address checks (not the scheme check) if you're fetching from a local test server. Behind `PROXY_URL`, address checks are skipped because the proxy — not YoinkT — resolves the name; the scheme check still applies.

Known limit: this validates before connecting, so DNS rebinding (a name that resolves public here and private a moment later) isn't covered. Closing that needs connect-time IP pinning, which Python's urllib doesn't expose.

### Engine freshness

The extraction engine rots — platforms change their internals and a build from last month stops working. It's the single most common cause of "it worked yesterday". For a downloader this isn't just a slowdown: a stale engine gets bot-challenged by YouTube and stops working entirely.

So the image **tracks the latest engine build by default**, and a pin is the escape hatch for when a specific new build regresses:

```bash
docker build .                                      # latest (default)
docker build --build-arg ENGINE_VERSION=2026.8.1 .  # pin a specific build
```

`ENGINE_VERSION` (a file at the repo root, also the compose/CI value) is the single source of truth; `latest` means newest, a version string pins.

- **CI runs the suite against the latest build** (what ships) and a non-gating floor-check against the requirements minimum.
- **`GET /api/health` reports the resolved build**, its age in days, and a `stale` flag past `ENGINE_STALE_DAYS` (default 21) — so if you *do* pin and it drifts, it's visible.

```bash
curl -s localhost:8000/api/health | python -m json.tool
# "engine": {"version": "2026.7.20", "age_days": 3, "stale": false, "channel": "nightly", ...}
```

When extraction starts failing on a site, that's the signal to bump the pin — not the calendar.

### Full resolution without cookies — PO tokens (no Docker)

YouTube now serves the full resolution ladder only to requests carrying a **PO token**. Without one, the cookie-free `tv`/`ios`/`mweb` player clients still get you *past* the bot wall, but capped at 360p. To get 1080p without cookies, you need a token — and you don't need Docker for it:

```bash
# one-time: clone + transpile the bgutil provider using Deno (which you already have)
SETUP_POT=1 ./run-local.sh
```

This uses the bgutil provider's **script mode** — the plugin mints tokens locally through Deno/Node, no server, no Docker, no cookies. After setup it's auto-detected. Check it's active:

```bash
curl -s localhost:8000/api/health | python3 -c "import sys,json; d=json.load(sys.stdin); print('pot_mode:', d['pot_mode'], '| yt_clients:', d['yt_clients'])"
# pot_mode: script | yt_clients: ['web']   <- full-res web client, token-backed
```

Priority is static token (`PO_TOKEN`) > HTTP server (`POT_PROVIDER_URL`, needs Docker/node) > script mode > off. Script mode spawns a runtime per request, so it's for a personal instance; use the HTTP server for public/multi-user deploys. A PO token *may* not be enough on a genuinely flagged IP (VPN/datacenter) — that still wants cookies or a residential proxy.

**Bot-Blocking Note**: Datacenter IPs are often blocked by YouTube. To fix this, you can configure either:
- **PO token (script mode)**: `SETUP_POT=1 ./run-local.sh` — cookie-free, Docker-free, full resolution (above).
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
| `MAX_CONCURRENCY` | `6` | Simultaneous extractions/streams per instance |
| `MAX_DURATION_SECS` | `0` | Refuse videos longer than this (0 = no cap; e.g. `14400` for public boxes) |
| `ZIP_MAX_BYTES` | `67108864` | Carousel-zip total byte budget (RAM) |
| `ZIP_SINGLE_MAX_BYTES` | `33554432` | Per-image cap inside the zip (skipped, not truncated) |

## 🛠 Frontend Development

The default UI is the hand-bundled vanilla JS file at `backend/static/index.html`
(it is the only UI wired to `/api/health`, `media[]`, error hints, lanes).
`frontend/` is a stale React prototype (YouTube-only, no `media[]`/`meta`) kept
for reference — do not build from it unless you plan to bring it up to parity.

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
