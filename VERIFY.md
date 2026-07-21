# VERIFY — run this before trusting v40–v42

Everything in v40–v42 was developed offline against stubs. The test suite (209
checks) never touched the network or the real engine. **Nothing here has been
proven to work against a live site.** This is the shortest path to finding out.

Stop at the first failure — later steps assume earlier ones passed.

---

## 1. Baseline still works (5 min)

```bash
./run-local.sh
```

Expect: `engine: 2026.07.04`, `engine build: 2026.07.04`, health responds,
footer shows `v42-always-mp4`.

```bash
curl -s localhost:8000/api/health | python3 -m json.tool
```

Check:
- `engine.version` matches ENGINE_VERSION, `engine.stale` is `false`
- `stealth.available` — if `false`, curl-cffi didn't install; stealth is
  silently doing nothing (by design, but you should know)
- `scratch` points somewhere with real disk
- `turbo_budget` shows `total_connections: 16`, `lanes: 4`

## 2. The egress guard doesn't block real downloads (HIGHEST RISK)

The guard is proven to *reject* private addresses. It has **never** been shown
to *permit* a real CDN URL. A false rejection breaks all downloads.

```bash
./run-local.sh 'https://www.youtube.com/watch?v=<something-short>'
```

Expect a 1080p merge and a speed sample. If you see
`upstream URL rejected: ...` in `YoinkT.log`, the guard is over-blocking —
set `ALLOW_PRIVATE_EGRESS=1` to confirm that's the cause, then report which URL
tripped it. Do not leave that flag on.

## 3. Speed — settle the original question

This started with "v39 feels slower". Get a number:

```bash
for i in 1 2 3; do
  curl -s -o /dev/null --max-time 20 -w "%{speed_download}\n" \
    "http://localhost:8000/api/download?url=<URL>&kind=m:1080"
done
```

Compare medians against the pre-v39 build. If it's slower on a *pinned* engine,
the cause is not the engine and not the flags — see `yoinkt-hardening` skill §3.

Also worth checking, since it was never measured: whether `--http-chunk-size
10M` still helps now that PO tokens are on. `HTTP_CHUNK_SIZE=0` disables it.

## 4. Turbo — codec guarantee and connection budget

```bash
# QuickTime toggle ON + turbo ON: the file must be H.264
curl -s -o /tmp/t.mp4 "http://localhost:8000/api/download?url=<URL>&kind=c:1080&turbo=1"
ffprobe -v error -select_streams v:0 -show_entries stream=codec_name /tmp/t.mp4
```

Expect `codec_name=h264`. Anything else means the v41 fix didn't hold.

Then a bulk run with Turbo on and 3+ lanes, watching:

```bash
watch -n1 'curl -s localhost:8000/api/health | python3 -c "import sys,json;print(json.load(sys.stdin)[\"turbo_budget\"])"'
```

`active` should rise to at most `lanes` (4), and connections per download drop
as it does. Confirm bulk isn't slower than before — the whole point was that
fewer connections shouldn't cost throughput.

## 5. Failure paths return real errors

```bash
# nonexistent video + turbo -> expect 502 with the engine's message, NOT a 0-byte file
curl -s -o /tmp/f.mp4 -w "%{http_code} %{size_download}\n" \
  "http://localhost:8000/api/download?url=https://www.youtube.com/watch?v=aaaaaaaaaaa&turbo=1&kind=m:720"

# turbo + clip -> expect 400 naming both options
curl -s -w "\n%{http_code}\n" \
  "http://localhost:8000/api/download?url=<URL>&kind=m:720&turbo=1&start=1&end=5"
```

## 6. Kubernetes (only if you deploy there)

Turbo/SponsorBlock were broken on the old manifest. Confirm the fix:

```bash
kubectl apply -f k8s/
kubectl exec deploy/YoinkT -- df -h /tmp /scratch
```

`/scratch` must be disk-backed and ~8Gi; `/tmp` stays 16Mi tmpfs. Then run one
turbo download of a video larger than 16 MiB. Watch for OOMKills:

```bash
kubectl get pod -l app=YoinkT -w
```

---

## If something fails and I'm not around

The `yoinkt-hardening` skill has the reasoning behind every one of these
changes, including which are deliberate trade-offs and which must not be
"simplified" back (notably: the info-lock refcount, the strict `c:` selector,
and never putting `medium: Memory` on the scratch volume).

Uncommitted work lives in `COMMIT_MSG_v40.txt`, `COMMIT_MSG_lockfix.txt`, and
`COMMIT_MSG_v41_v42.txt`. Delete whichever you don't use.
