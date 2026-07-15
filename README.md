# YoinkT 🎬⬇️

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

## 🌐 Deploying Frontend to GitHub Pages (gh-pages)

You **can** host the UI on GitHub Pages! However, because YoinkT requires a backend to fetch and stream the videos, you will need to host the backend separately (e.g., on Render, Railway, Fly.io, or your own server).

### Steps to host on gh-pages:
1. Deploy your backend to a public server (e.g., `https://api.yourdomain.com`).
2. Ensure your backend has **CORS enabled** so your GitHub Pages domain can talk to it.
3. Update the frontend API calls in `backend/static/index.html` (or in the React `frontend/` source) to point to your new backend URL instead of relative paths (change `/api/...` to `https://api.yourdomain.com/api/...`).
4. Commit the `index.html` (and assets) to a `gh-pages` branch on your GitHub repository!

## ☸️ Kubernetes Deployment

```bash
docker build -t <registry>/YoinkT:1 . && docker push <registry>/YoinkT:1
# Edit image in k8s/deployment.yaml, host in k8s/ingress.yaml
kubectl apply -f k8s/
```

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

## ⚠️ Legal & Maintenance

- **Maintenance**: `yt-dlp` breaks when YouTube changes its APIs. Rebuilding the docker image regularly ensures you pull the latest `yt-dlp` version.
- **Legal**: YouTube ToS prohibits downloading without permission. Use this tool for your own content, Creative Commons, or where you have rights. Public deployment is at your own risk.
