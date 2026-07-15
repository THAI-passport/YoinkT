"""Live smoke test — hits a RUNNING YoinkT server and checks one public URL per
site actually resolves media. This is the real-breakage signal the offline
suite can't give (it stubs the network). yt-dlp rot / platform changes surface
here first.

Usage:  python tests/smoke_live.py [http://localhost:8000]

Exit 0 if every ENABLED site returns usable media; non-zero otherwise. Datacenter
IPs / missing cookies will fail some sites — that's expected off a home network.
Override the per-site sample URLs with env SMOKE_URL_<SITE>.
"""
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SMOKE_BASE", "http://localhost:8000")

# public, stable-ish samples; override via env if any rot away
SAMPLES = {
    "youtube": os.environ.get("SMOKE_URL_YOUTUBE", "https://www.youtube.com/watch?v=aqz-KE-bpKQ"),  # Big Buck Bunny
    "x": os.environ.get("SMOKE_URL_X", ""),
    "facebook": os.environ.get("SMOKE_URL_FACEBOOK", ""),
    "instagram": os.environ.get("SMOKE_URL_INSTAGRAM", ""),
    "tiktok": os.environ.get("SMOKE_URL_TIKTOK", ""),
}


def _get(path):
    req = urllib.request.Request(BASE + path, headers={"X-API-Key": os.environ.get("API_KEY", "")})
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read())


def main():
    try:
        health = _get("/api/health")
    except Exception as e:
        print(f"FAIL: server not reachable at {BASE}: {e}")
        return 1
    print(f"server {health.get('version')} — sites: {list((health.get('sites') or {}).keys())}")
    enabled = set((health.get("sites") or {}).keys())

    passed = failed = skipped = 0
    for site, url in SAMPLES.items():
        if site not in enabled:
            continue
        if not url:
            print(f"SKIP {site}: no sample URL (set SMOKE_URL_{site.upper()})")
            skipped += 1
            continue
        try:
            info = _get("/api/info?url=" + urllib.parse.quote(url, safe=""))
            n = len(info.get("formats") or []) + len(info.get("media") or [])
            if n > 0:
                print(f"PASS {site}: '{(info.get('title') or '')[:40]}' — {n} option(s)")
                passed += 1
            else:
                print(f"FAIL {site}: no formats/media returned")
                failed += 1
        except Exception as e:
            print(f"FAIL {site}: {str(e)[:160]}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
