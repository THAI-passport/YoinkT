"""YoinkT — stateless streaming download service
(YouTube + X/Twitter + Facebook + Instagram).

Endpoints:
  GET /api/health              liveness/readiness probe
  GET /api/info?url=           video metadata + curated format list (+media[] for X)
  GET /api/download?url=&kind= stream file to client (no disk writes)

kind values:
  p:<format_id>   progressive single file -> Range proxy (resumable) or yt-dlp pipe
  m:<height>      DASH merge (YouTube/Facebook): yt-dlp -> FIFOs -> ffmpeg -> fMP4
  a:audio         bestaudio (m4a/webm passthrough)
  i:<index>       photo (X/IG): Range-proxy the image CDN URL — no subprocess
  v:<index>       one video of a multi-video post: best MP4, Range-proxied

Site notes:
  x          video anon OK; photos via gallery-dl; cookies for NSFW/protected
  facebook   public video anon OK (watch/reel/fb.watch); PHOTO posts usually
             need COOKIES_FILE_FACEBOOK. Merge path enabled (FB HD is often
             DASH-only, like YouTube). Bundle site: yt-dlp video + gdl photos.
  instagram  cookies effectively REQUIRED (COOKIES_FILE_INSTAGRAM). Reels are
             video (yt-dlp, sometimes anon); PHOTO posts/carousels need cookies.
  tiktok     video anon OK, no-watermark by default (yt-dlp uses the play addr,
             not the stamped download addr). Photo slideshows via gallery-dl.

Config (env):
  PROXY_URL          optional proxy for yt-dlp/ffmpeg (needed on datacenter IPs / k8s)
  COOKIES_FILE       optional global Netscape cookies.txt (mounted Secret on k8s)
  COOKIES_FILE_X / COOKIES_FILE_YOUTUBE / COOKIES_FILE_FACEBOOK / COOKIES_FILE_INSTAGRAM
                     per-site cookies. WHY per-site: X extraction that works
                     anonymously can BREAK when cookies are attached (yt-dlp
                     #12549), and an invalidated session on one site must not
                     degrade the others. Site-specific beats global when both set.
  ENABLED_SITES      comma list, default "youtube,x,facebook,instagram"
  MAX_CONCURRENCY    simultaneous downloads per pod (default 6)
  STEALTH_PROFILES   per-site browser handshake, e.g. "x=chrome,instagram=safari";
                     "off" disables. Default: on for the social sites, OFF for
                     YouTube (PO tokens are YouTube's lever; a mismatched
                     handshake there can cost the full-speed formats). Needs the
                     optional curl-cffi transport — absence is detected and
                     silently downgrades to the stock client.
  ENGINE_STALE_DAYS  age past which /api/health flags the extraction engine as
                     stale (default 21). Reporting only; nothing is blocked.
  ALLOW_PRIVATE_EGRESS
                     "1" lets resolved media URLs point at private/loopback
                     addresses. OFF by default — the site allowlist guards the
                     URL a user SUBMITS, this guards the ones we actually open
                     sockets to (format URLs, CDN URLs, scraped og: tags, and
                     every redirect hop). Enable only to fetch from a local
                     test server.
  ZIP_MAX_PHOTOS / ZIP_MAX_BYTES
                     carousel-zip caps: how many photos (60) and how many bytes
                     (256 MiB). The zip is built in RAM, so the byte cap is the
                     one that bounds memory.
"""

import asyncio
import datetime
import functools
import importlib
import importlib.util
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import suppress

log = logging.getLogger("uvicorn.error")

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_VERSION = "YoinkT v43-always-mp4-quality"  # bump on behavior changes; shown in UI footer
# NOTE: keep the substring "always-mp4" — run-local.sh greps for it

PROXY_URL = os.environ.get("PROXY_URL") or None
COOKIES_FILE = os.environ.get("COOKIES_FILE") or None
# per-site cookies override the global file for that site only (see docstring)
SITE_COOKIES = {
    "youtube": os.environ.get("COOKIES_FILE_YOUTUBE") or None,
    "x": os.environ.get("COOKIES_FILE_X") or None,
    "facebook": os.environ.get("COOKIES_FILE_FACEBOOK") or None,
    "instagram": os.environ.get("COOKIES_FILE_INSTAGRAM") or None,
    "tiktok": os.environ.get("COOKIES_FILE_TIKTOK") or None,
}
# runtime cookie overrides — populated by the /api/cookies upload endpoint.
# Checked BEFORE env in _cookies_for. SINGLE-INSTANCE only (in-process, not
# shared across k8s pods); for multi-pod use mounted Secrets (env) instead.
SITE_COOKIES_RUNTIME: dict[str, str] = {}
ENABLED_SITES = {s.strip() for s in os.environ.get(
    "ENABLED_SITES", "youtube,x,facebook,instagram,tiktok").split(",") if s.strip()}

# ---- deploy guardrails (all OFF by default; enable for public exposure) ----
# require ?key=/X-API-Key on every /api endpoint except /api/health.
API_KEY = os.environ.get("API_KEY") or None
# naive per-IP requests/min token bucket (per-process; approximate on k8s).
RATE_LIMIT_RPM = int(os.environ.get("RATE_LIMIT_RPM", "0"))  # 0 = disabled
# allow browser cookie.txt upload -> writable dir. Default ON for localhost
# convenience; set 0 on any shared/public deploy.
ALLOW_COOKIE_UPLOAD = os.environ.get("ALLOW_COOKIE_UPLOAD", "1") not in ("0", "false", "")
COOKIES_DIR = os.environ.get("COOKIES_DIR") or os.path.join(tempfile.gettempdir(), "YoinkT-cookies")
# PO token support (present as a legitimate client -> full-speed servers, no
# "confirm you're not a bot"). Either a static token or a bgutil provider URL.
PO_TOKEN = os.environ.get("PO_TOKEN") or None          # e.g. "web.gvs+XXXX"
POT_PROVIDER_URL = os.environ.get("POT_PROVIDER_URL") or None  # bgutil http provider
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "6"))
# kill a stream that produces no data for this long — frees the semaphore slot
# a hung yt-dlp/ffmpeg would otherwise hold forever (queue starvation).
STREAM_STALL_TIMEOUT = int(os.environ.get("STREAM_STALL_TIMEOUT", "120"))
# parallel DASH fragment downloads per stream — helps fragmented formats.
# googlevideo throttles per-connection; N connections ≈ N x throughput.
CONCURRENT_FRAGMENTS = int(os.environ.get("CONCURRENT_FRAGMENTS", "16"))
# ranged chunking for UNfragmented formats (most video-only DASH streams are
# plain https, where concurrent-fragments does nothing) — this is the throttle
# dodge for them. Set "0" to disable if CDN starts 403ing ranged requests.
HTTP_CHUNK_SIZE = os.environ.get("HTTP_CHUNK_SIZE", "10M")

# ---- scratch storage --------------------------------------------------------
# /tmp and the buffered download path want OPPOSITE things and must not share a
# volume. The streaming merge path puts named pipes in /tmp: bytes flow through
# them, nothing accumulates, so a tiny RAM-backed tmpfs is exactly right. The
# buffered path (turbo, SponsorBlock) writes the WHOLE video file — hundreds of
# MB — because aria2c can't pipe to stdout and SponsorBlock cuts a finished file.
#
# On the shipped k8s manifest /tmp was `emptyDir: {medium: Memory, sizeLimit:
# 16Mi}`, so every turbo/SponsorBlock download of a video larger than 16 MiB
# failed there; raising that limit would have been worse, since tmpfs pages are
# charged to the pod's memory cgroup and the failure becomes an OOMKill instead
# of a disk error. Docker and native runs never showed it — both give /tmp real
# disk. So: buffered downloads get their own DISK-backed directory.
SCRATCH_DIR = os.environ.get("SCRATCH_DIR") or tempfile.gettempdir()

# ---- turbo connection budget ------------------------------------------------
# aria2c opens many parallel connections because the platform throttles each one
# (~2-4 MB/s). 16 connections is therefore ~32-64 MB/s ~= 256-512 Mbps, which
# already saturates a typical link with a SINGLE download. Running N batch lanes
# at 16 each does NOT multiply throughput — the bottleneck has moved to the
# user's own pipe — it just multiplies the connection count against one CDN,
# which is the pattern that trips rate-limiting and bot-detection.
#
# So the right total is a CONSTANT ("enough to saturate the link"), not something
# that scales with lanes: divide a fixed budget across whatever is in flight.
# Counted server-side rather than from a client-supplied lane count, so it also
# holds across multiple tabs and multiple users, and can't be lied about.
TURBO_CONN_BUDGET = int(os.environ.get("TURBO_CONN_BUDGET", "16"))
# Floor: below ~4 connections aria2c's advantage collapses (each is throttled)
# and you pay turbo's buffering cost for nothing. Better to queue a lane than
# run it so degraded that turbo is pointless.
TURBO_MIN_CONNS = int(os.environ.get("TURBO_MIN_CONNS", "4"))

# ---- stealth profiles -------------------------------------------------------
# Some platforms fingerprint the TLS handshake, not just the User-Agent: a
# stock Python HTTPS client is identifiable as "not a browser" before a single
# byte of HTTP is sent, and gets checkpointed regardless of cookies. A stealth
# profile makes the extractor's requests handshake like a real browser build.
#
# Deliberately NOT enabled for youtube: PO tokens (above) are YouTube's real
# lever, and a mismatched handshake there can cost us the full-speed formats.
# The social sites are where the TLS check actually bites.
#
# Override wholesale with STEALTH_PROFILES="x=chrome,instagram=safari" or
# disable entirely with STEALTH_PROFILES="off".
_STEALTH_DEFAULTS = {
    "youtube": None,
    "x": "chrome",
    "facebook": "chrome",
    "instagram": "chrome",
    "tiktok": "chrome",
}
STREAM_PROFILE_RAW = os.environ.get("STEALTH_PROFILES", "")

# ---- engine provenance ------------------------------------------------------
# The extraction engine is the part of YoinkT that rots: platforms change their
# internals and an engine built last month stops working. We can't stop that,
# but we can make it VISIBLE instead of showing up as mystery 502s — health
# reports the engine build and flags it once it's older than this many days.
ENGINE_STALE_DAYS = int(os.environ.get("ENGINE_STALE_DAYS", "21"))

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(title="YoinkT", docs_url=None, redoc_url=None)
sem = asyncio.Semaphore(MAX_CONCURRENCY)
# Turbo lanes are capped so every running one still clears TURBO_MIN_CONNS out
# of the shared budget; lane N+1 queues instead of running degraded.
TURBO_LANES = max(1, TURBO_CONN_BUDGET // max(1, TURBO_MIN_CONNS))
turbo_sem = asyncio.Semaphore(TURBO_LANES)
_turbo_active = 0  # in-flight turbo prepares; drives the per-download split


def _turbo_conns(active: int) -> int:
    """Connections for one turbo download, given how many are running."""
    return max(TURBO_MIN_CONNS, TURBO_CONN_BUDGET // max(1, active))


# Aggregate scratch budget. N buffered downloads stage N whole videos at once;
# without this the only limit was the volume filling up mid-download, which
# surfaces as a confusing partial failure. Reserve up front, refuse politely.
SCRATCH_BUDGET = int(os.environ.get("SCRATCH_BUDGET", str(8 * 1024**3)))
_scratch_reserved = 0


def _estimate_bytes(info: dict, kind: str) -> int:
    """Best-effort size of what the buffered path is about to stage.

    Deliberately generous: under-estimating means admitting a download that
    then fills the volume, which is the failure we're preventing. A merge also
    holds both source streams AND the muxed output at once, hence the 2.2x."""
    fmts = info.get("formats") or []

    def _sz(f):
        return f.get("filesize") or f.get("filesize_approx") or 0

    if kind.startswith("p:"):
        f = next((x for x in fmts if x.get("format_id") == kind[2:]), None)
        base = _sz(f) if f else 0
        mult = 1.2
    elif kind.startswith(("m:", "c:")):
        h = int(kind[2:]) if kind[2:].isdigit() else 0
        vids = [f for f in fmts if f.get("height") == h and f.get("acodec") == "none"]
        auds = [f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"]
        base = (max((_sz(f) for f in vids), default=0)
                + max((_sz(f) for f in auds), default=0))
        mult = 2.2   # both inputs on disk plus the muxed output
    else:
        auds = [f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"]
        base = max((_sz(f) for f in auds), default=0)
        mult = 2.0   # transcode writes a second file
    if not base:
        # No size metadata (common for DASH): fall back to duration x a
        # pessimistic bitrate rather than admitting an unbounded download.
        dur = float(info.get("duration") or 0)
        base = int(dur * 1.5 * 1024**2 / 8) if dur else 512 * 1024**2
    return int(base * mult)


def _scratch_root() -> str:
    """Writable directory for buffered downloads. Falls back to the system temp
    dir with a warning rather than failing the request — a misconfigured
    SCRATCH_DIR should degrade to the old behavior, not break turbo."""
    try:
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        if os.access(SCRATCH_DIR, os.W_OK):
            return SCRATCH_DIR
        log.warning("SCRATCH_DIR %s not writable; using %s",
                    SCRATCH_DIR, tempfile.gettempdir())
    except Exception as e:
        log.warning("SCRATCH_DIR %s unusable (%s); using %s",
                    SCRATCH_DIR, e, tempfile.gettempdir())
    return tempfile.gettempdir()

# naive per-IP token bucket: ip -> [window_start_epoch, count]. Per-process, so
# on k8s each pod counts separately (approximate) — good enough as an abuse
# guard, not a billing meter.
_rl_buckets: dict[str, list] = {}


@app.middleware("http")
async def _guardrails(request, call_next):
    from fastapi.responses import JSONResponse
    path = request.url.path
    if path.startswith("/api") and path != "/api/health":
        if API_KEY:
            given = (request.headers.get("x-api-key")
                     or request.query_params.get("key"))
            if given != API_KEY:
                return JSONResponse({"detail": "invalid or missing API key"}, status_code=401)
        if RATE_LIMIT_RPM > 0:
            ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                  or (request.client.host if request.client else "?"))
            now = time.time()
            b = _rl_buckets.get(ip)
            if not b or now - b[0] >= 60:
                _rl_buckets[ip] = [now, 1]
            else:
                b[1] += 1
                if b[1] > RATE_LIMIT_RPM:
                    return JSONResponse(
                        {"detail": f"rate limit {RATE_LIMIT_RPM}/min exceeded"},
                        status_code=429)
            if len(_rl_buckets) > 10000:  # cheap cap so the dict can't grow forever
                for k in [k for k, v in _rl_buckets.items() if now - v[0] >= 60]:
                    _rl_buckets.pop(k, None)
    return await call_next(request)

YOUTUBE_RE = re.compile(
    r"^https?://(www\.|m\.|music\.)?(youtube\.com|youtu\.be|youtubekids\.com)/", re.I
)
# Site registry — a STRICT allowlist, never a generic "any URL" (SSRF guard:
# these URLs get handed to yt-dlp/ffmpeg/urllib). Add sites here only.
SITE_RES: dict[str, re.Pattern] = {
    "youtube": YOUTUBE_RE,
    # tweets only (status URLs) — profiles/searches are out of scope
    "x": re.compile(
        r"^https?://(www\.|mobile\.)?(x\.com|twitter\.com)/\w{1,20}/status/\d+", re.I),
    # video: watch / reel / share / page videos / fb.watch
    # photos: photo / photo.php / posts / permalink / story / <page>/photos/
    "facebook": re.compile(
        r"^https?://((www|m|web)\.)?facebook\.com/"
        r"(watch|reel/\d+|share/[vrp]/[\w-]+|video\.php|photo|photo\.php|"
        r"permalink\.php|story\.php|[\w.]{1,60}/(videos|posts|photos)/[\w.]+)"
        r"|^https?://fb\.watch/[\w-]+", re.I),
    # posts / reels only — profiles/stories out of scope (stories need login + expire)
    "instagram": re.compile(
        r"^https?://(www\.)?instagram\.com/([\w.]{1,40}/)?(p|reel|reels|tv)/[\w-]+", re.I),
    # video: @user/video/<id>, vm./vt. shortlinks, /t/<id>
    # photo slideshows: @user/photo/<id>
    "tiktok": re.compile(
        r"^https?://((www|m)\.)?tiktok\.com/(@[\w.]{1,30}/(video|photo)/\d+|t/[\w-]+)"
        r"|^https?://(vm|vt)\.tiktok\.com/[\w-]+", re.I),
}
# sites whose posts mix photos+videos -> combined yt-dlp + gallery-dl bundle.
# Facebook is here TOO (photo posts) even though it also supports DASH merge:
# the bundle path runs yt-dlp (video) AND gallery-dl (photos) side by side.
BUNDLE_SITES = {"x", "instagram", "facebook", "tiktok"}
# sites with YouTube-style DASH (video/audio split) -> m:/c: merge kinds allowed
MERGE_SITES = {"youtube", "facebook"}


def _site_of(url: str) -> str | None:
    for site, rx in SITE_RES.items():
        if site in ENABLED_SITES and rx.match(url):
            return site
    return None


def _require_site(url: str) -> str:
    site = _site_of(url)
    if not site:
        raise HTTPException(
            400, "only YouTube, X/Twitter, Facebook, and Instagram post URLs accepted")
    return site


def _require_youtube(url: str) -> None:
    if _site_of(url) != "youtube":
        raise HTTPException(400, "only YouTube URLs accepted")


def _cookies_for(site: str) -> str | None:
    # runtime upload > per-site env > global env
    return SITE_COOKIES_RUNTIME.get(site) or SITE_COOKIES.get(site) or COOKIES_FILE


# ---- egress guard -----------------------------------------------------------
# The site allowlist (SITE_RES) validates the URL the USER submits. It says
# nothing about the URLs we end up fetching: format URLs from the extractor,
# CDN URLs from the photo resolver, and og:video/og:image URLs scraped out of
# page HTML. Those are the ones this process actually opens sockets to, and
# until now nothing checked them. Two concrete holes that closes:
#
#   1. urllib's default opener includes FileHandler/FTPHandler/DataHandler, so
#      a resolved "file:///etc/passwd" would be read and streamed to the caller.
#   2. urllib follows redirects by default and revalidates nothing, so a CDN
#      URL that 302s to 169.254.169.254 (cloud metadata) or 127.0.0.1 would be
#      followed and its response returned.
#
# So: scheme allowlist, destination-IP denylist, and every redirect hop
# revalidated. Use _egress_opener() instead of urllib.request.build_opener()
# for anything reachable from a request.
#
# HONEST LIMIT: this is a check-then-connect design, so a DNS name that
# resolves to a public IP here and a private one microseconds later (DNS
# rebinding) is not covered. Closing that needs connect-time pinning, which
# urllib doesn't expose. Out of scope for this threat model — the realistic
# vectors are redirects and scraped URLs, both of which ARE covered.
ALLOW_PRIVATE_EGRESS = os.environ.get(
    "ALLOW_PRIVATE_EGRESS", "0") in ("1", "true", "yes")

_EGRESS_SCHEMES = {"http", "https"}


def _egress_reject_reason(url: str) -> str | None:
    """None if this URL is safe to open, else a short reason. Never raises."""
    import ipaddress
    import socket
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(url)
    except Exception:
        return "unparseable URL"
    if parts.scheme.lower() not in _EGRESS_SCHEMES:
        return f"scheme {parts.scheme!r} not allowed"
    host = parts.hostname
    if not host:
        return "no host"
    if ALLOW_PRIVATE_EGRESS:
        return None
    if PROXY_URL:
        # With an egress proxy the NAME is resolved at the proxy, not here, so
        # local resolution proves nothing about where the connection lands.
        # Scheme check still applies; IP checks are the proxy's job.
        return None
    try:
        infos = socket.getaddrinfo(host, parts.port or
                                   (443 if parts.scheme == "https" else 80),
                                   proto=socket.IPPROTO_TCP)
    except Exception as e:
        return f"cannot resolve host ({type(e).__name__})"
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            return f"unparseable address {addr!r}"
        # covers loopback, RFC1918, link-local (incl. 169.254 metadata),
        # unique-local v6, multicast, reserved, and unspecified
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_multicast or ip.is_reserved or ip.is_unspecified):
            return f"resolves to non-public address {addr}"
    return None


def _validate_egress(url: str) -> str:
    reason = _egress_reject_reason(url)
    if reason:
        log.warning("egress blocked: %s (%s)", url[:120], reason)
        raise HTTPException(502, f"upstream URL rejected: {reason}")
    return url


def _egress_opener(*extra_handlers):
    """An opener that can ONLY speak http/https and revalidates every redirect.

    Built from an explicit handler list rather than urllib.request.build_opener,
    because build_opener always mixes in the defaults — including the file://
    and ftp:// handlers this is meant to exclude."""
    import urllib.request

    class _GuardedRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            _validate_egress(newurl)  # raises -> redirect chain aborts here
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    od = urllib.request.OpenerDirector()
    for h in (urllib.request.HTTPHandler(), urllib.request.HTTPSHandler(),
              urllib.request.HTTPErrorProcessor(),
              urllib.request.HTTPDefaultErrorHandler(), _GuardedRedirect()):
        od.add_handler(h)
    if PROXY_URL:
        od.add_handler(urllib.request.ProxyHandler(
            {"http": PROXY_URL, "https": PROXY_URL}))
    for h in extra_handlers:
        od.add_handler(h)
    return od


# ---- stealth profiles -------------------------------------------------------

def _parse_stealth(raw: str) -> dict[str, str | None]:
    """STEALTH_PROFILES env -> {site: profile|None}. "off" disables everything."""
    if raw.strip().lower() in ("off", "none", "0", "false"):
        return {s: None for s in _STEALTH_DEFAULTS}
    out = dict(_STEALTH_DEFAULTS)
    for pair in raw.split(","):
        if "=" not in pair:
            continue
        site, _, prof = pair.partition("=")
        site, prof = site.strip().lower(), prof.strip()
        if site in out:
            out[site] = prof or None
    return out


STEALTH_PROFILES = _parse_stealth(STREAM_PROFILE_RAW)


@functools.lru_cache(maxsize=1)
def _stealth_available() -> bool:
    """Whether browser-handshake requests are actually possible in this build.

    Feature-detected, never assumed: the transport is an OPTIONAL dependency
    (see requirements.txt). If it's missing — slim image, pip resolution failure
    on an exotic arch — every request silently falls back to the stock client
    rather than erroring. Degraded, not broken."""
    if importlib.util.find_spec("curl_cffi") is None:
        return False
    try:
        importlib.import_module("yt_dlp.networking.impersonate")
        return True
    except Exception:
        return False


def _stealth_for(site: str) -> str | None:
    prof = STEALTH_PROFILES.get(site)
    return prof if (prof and _stealth_available()) else None


def _stealth_target(profile: str):
    """Profile string -> the engine's target object (Python API paths only)."""
    from yt_dlp.networking.impersonate import ImpersonateTarget
    return ImpersonateTarget.from_str(profile)


# ---- engine provenance ------------------------------------------------------

_ENGINE_DATE_RE = re.compile(r"^(\d{4})\.(\d{2})\.(\d{2})")


@functools.lru_cache(maxsize=1)
def _engine_info() -> dict:
    """Build stamp of the extraction engine + how stale it is.

    Engine builds are date-versioned (YYYY.MM.DD[.nnnnnn]), so age is readable
    straight off the version string — no network call, no release-feed polling.
    Cached: this is fixed for the life of the process."""
    info: dict = {"version": None, "age_days": None, "stale": None,
                  "channel": None, "photos": None}
    try:
        from yt_dlp.version import __version__ as ver
        info["version"] = ver
        # 4th segment = nightly/master build counter; plain 3 segments = stable
        info["channel"] = "nightly" if ver.count(".") >= 3 else "stable"
        m = _ENGINE_DATE_RE.match(ver)
        if m:
            built = datetime.date(*(int(x) for x in m.groups()))
            age = (datetime.date.today() - built).days
            info["age_days"] = age
            info["stale"] = age > ENGINE_STALE_DAYS
    except Exception as e:  # engine import must never take health down
        log.warning("engine version probe failed: %s", e)
    try:
        from gallery_dl.version import __version__ as gver
        info["photos"] = gver
    except Exception:
        pass
    return info


_POT_ON = bool(PO_TOKEN or POT_PROVIDER_URL)


def _extractor_args_dict() -> dict:
    """PO-token extractor args for the yt-dlp Python API (dict of dict of lists)."""
    ea: dict = {}
    yt: dict = {}
    if PO_TOKEN:
        yt["po_token"] = [PO_TOKEN]
    if POT_PROVIDER_URL:
        ea["youtubepot-bgutilhttp"] = {"base_url": [POT_PROVIDER_URL]}
    if not _POT_ON:
        # No provider configured: tell yt-dlp NOT to attempt PO tokens at all.
        # Otherwise the installed bgutil plugin probes a provider on every
        # request, fails, and adds seconds of latency for zero benefit.
        yt["fetch_pot"] = ["never"]
    if yt:
        ea["youtube"] = yt
    return ea


def _extractor_args_cli() -> list[str]:
    """Same, as yt-dlp CLI --extractor-args flags for the subprocess paths."""
    out: list[str] = []
    if PO_TOKEN:
        out += ["--extractor-args", f"youtube:po_token={PO_TOKEN}"]
    if POT_PROVIDER_URL:
        out += ["--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_PROVIDER_URL}"]
    if not _POT_ON:
        out += ["--extractor-args", "youtube:fetch_pot=never"]
    return out


def _ydl_opts(site: str = "youtube") -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        # X multi-video posts / IG carousels extract as playlists — allow them
        "noplaylist": site not in BUNDLE_SITES,
        "skip_download": True,
        "remote_components": ["ejs:github"],
    }
    if PROXY_URL:
        opts["proxy"] = PROXY_URL
    ck = _cookies_for(site)
    if ck:
        opts["cookiefile"] = ck
    ea = _extractor_args_dict()  # namespaced per-extractor; harmless off-youtube
    if ea:
        opts["extractor_args"] = ea
    prof = _stealth_for(site)
    if prof:
        with suppress(Exception):  # bad profile name must not kill extraction
            opts["impersonate"] = _stealth_target(prof)
    # NB: no config-isolation flag needed here. This path drives the engine
    # through its Python API, which takes options ONLY from this dict — user
    # config files are a CLI-layer concern (see _base_flags).
    return opts


# Age-gate best-effort: which player clients yt-dlp impersonates on retry.
# NOTE (honest): "Sign in to confirm your age" is a HARD gate — YouTube now
# demands an authenticated session. yt-dlp already adds tv_embedded/creator by
# default, so this retry rarely wins; mweb + a PO token is the only lever that
# still sometimes works. Real fix is cookies (a throwaway Google account is
# fine — the cookie need only prove "logged in", not be your main account).
_YT_AGE_CLIENTS = os.environ.get("YT_AGE_CLIENTS", "default,tv,mweb,web_safari")


def _is_age_error(e: Exception) -> bool:
    m = str(e).lower()
    return ("confirm your age" in m or "inappropriate for some users" in m
            or "age-restricted" in m or "confirm you’re" in m and "age" in m)


# Phrases the social platforms use when they've decided the CLIENT is the
# problem rather than the content — i.e. exactly the case a browser handshake
# can fix. Kept narrow on purpose: "login required" on a genuinely private post
# is NOT here, because retrying that just burns a request.
_BLOCK_HINTS = (
    "not a bot", "confirm you're not", "confirm you’re not",
    "rate-limit reached", "rate limit reached", "please wait a few minutes",
    "blocked", "captcha", "checkpoint", "temporarily restricted",
)


def _is_block_error(e: Exception) -> bool:
    m = str(e).lower()
    return any(h in m for h in _BLOCK_HINTS)


# ---- error taxonomy ---------------------------------------------------------
# Extraction failures used to surface as `502 extract failed: <raw engine
# stderr>`. That text is written for someone reading a terminal with the full
# command in scroll-back, not for someone who pasted a link into a web page —
# it names internal clients and flags, and never says what to DO. Every rule
# below maps a recognised failure to an action the user can actually take.
#
# Ordered: first match wins, so put specific patterns above general ones.
# `hint` is what the UI shows; `status` distinguishes "your fault" (4xx) from
# "our/upstream fault" (5xx) so the frontend can decide whether to offer retry.
_ERROR_RULES: list[tuple[tuple[str, ...], int, str]] = [
    (("sign in to confirm you're not a bot", "sign in to confirm you’re not a bot",
      "confirm you're not a bot", "confirm you’re not a bot"),
     403, "{site} is challenging this server as a bot. Add cookies for {site} "
          "(the cookie wizard), or route the server through a residential proxy."),
    (("confirm your age", "age-restricted", "inappropriate for some users"),
     403, "This video is age-restricted. {site} requires a signed-in session — "
          "add cookies for {site} (a throwaway account is fine)."),
    (("login required", "requires authentication", "you must be logged in",
      "private video", "this post is private", "not available to you"),
     403, "This post is private or needs a login. Add cookies for {site} from "
          "an account that can see it."),
    (("rate-limit reached", "rate limit reached", "too many requests",
      "please wait a few minutes", "http error 429"),
     429, "{site} is rate-limiting this server. Wait a few minutes and retry; "
          "if you're batch-downloading, use fewer lanes."),
    (("not available in your country", "geo restricted", "geo-restricted",
      "blocked in your country", "not available from your location"),
     451, "{site} blocks this content from the server's location. Set PROXY_URL "
          "to somewhere it's available."),
    (("video unavailable", "this video is not available", "content isn't available",
      "no longer available", "has been removed", "video has been removed"),
     404, "That video is unavailable or has been removed."),
    (("is not a valid url", "unsupported url", "no suitable extractor"),
     400, "That link isn't one YoinkT can handle. Check it points at a single "
          "post or video on a supported site."),
    (("members-only", "join this channel", "paid members", "purchase", "rental"),
     402, "This is paywalled or members-only content."),
    (("live event will begin", "premiere", "this live event has not started",
      "is not currently live"),
     425, "This is a scheduled or upcoming stream — nothing to download yet."),
    (("unable to download webpage", "unable to connect", "connection reset",
      "timed out", "temporary failure in name resolution", "network is unreachable"),
     504, "Couldn't reach {site} from the server. Check the server's network "
          "or proxy settings and retry."),
    (("no space left", "enospc"),
     507, "The server ran out of scratch space. Free space in SCRATCH_DIR, or "
          "give the pod a bigger disk-backed volume."),
    (("nsig extraction failed", "unable to extract", "player response",
      "failed to parse json", "signature extraction"),
     502, "{site} changed something the extraction engine doesn't understand "
          "yet. Update the engine build (ENGINE_VERSION) and retry."),
]

_SITE_LABEL = {"youtube": "YouTube", "x": "X", "facebook": "Facebook",
               "instagram": "Instagram", "tiktok": "TikTok"}


def _classify_error(exc: BaseException, site: str = "youtube") -> HTTPException:
    """Turn an engine failure into an HTTP error a person can act on.

    The raw engine text is still attached as `detail.raw` (truncated) so the
    cause isn't lost — the UI shows `hint`, a bug report can quote `raw`."""
    raw = str(exc).strip()
    low = raw.lower()
    label = _SITE_LABEL.get(site, site)
    for needles, status, hint in _ERROR_RULES:
        if any(n in low for n in needles):
            return HTTPException(status, {"hint": hint.format(site=label),
                                          "raw": raw[-400:]})
    # Unrecognised: say so honestly rather than inventing a diagnosis. A stale
    # engine is the most common cause of a novel error, so point there.
    return HTTPException(502, {
        "hint": f"{label} extraction failed for an unrecognised reason. If this "
                "persists, the extraction engine build may need updating.",
        "raw": raw[-400:]})


def _extract(url: str) -> dict:
    import yt_dlp

    site = _site_of(url) or "youtube"
    opts = _ydl_opts(site)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception as e:
        # one best-effort age-gate retry with bypass clients (YouTube only)
        if site == "youtube" and _is_age_error(e) and not _cookies_for(site):
            log.info("age-gate hit; retrying player_client=%s", _YT_AGE_CLIENTS)
            ea = dict(opts.get("extractor_args") or {})
            yt = dict(ea.get("youtube") or {})
            yt["player_client"] = _YT_AGE_CLIENTS.split(",")
            opts2 = {**opts, "extractor_args": {**ea, "youtube": yt}}
            with yt_dlp.YoutubeDL(opts2) as ydl:
                return ydl.extract_info(url, download=False)
        # Client-level block on a site we did NOT pre-arm with a stealth
        # profile: retry once with one. Cheap (a second handshake), bounded
        # (only on the narrow phrase list above), and it turns the most common
        # social-site failure from a dead 502 into a success. Never loops —
        # `not opts.get("impersonate")` means a stealth attempt can't retry.
        if (_is_block_error(e) and _stealth_available()
                and not opts.get("impersonate")):
            fallback = STEALTH_PROFILES.get(site) or "chrome"
            log.info("client-level block on %s; retrying as %s", site, fallback)
            with suppress(Exception):
                opts3 = {**opts, "impersonate": _stealth_target(fallback)}
                with yt_dlp.YoutubeDL(opts3) as ydl:
                    return ydl.extract_info(url, download=False)
        raise


# Info cache: extraction is the slow part of starting a download. yt-dlp stream
# URLs live ~6h, so caching the whole extract for a few minutes is safe and
# makes quality-switches / re-downloads / the download-after-info-fetch start
# INSTANTLY instead of re-extracting. Per-URL lock collapses duplicate
# concurrent extractions (e.g. info + download firing together) into one.
_INFO_TTL = int(os.environ.get("INFO_CACHE_TTL", "300"))
_INFO_CACHE: dict[str, tuple[float, dict]] = {}
_info_locks: dict[str, asyncio.Lock] = {}
# How many coroutines are currently inside _extract_cached for a given URL —
# i.e. holding its lock OR queued for it. Only the last one out may reclaim the
# lock. See the WHY in _extract_cached's finally block: asyncio.Lock.locked()
# is NOT sufficient to answer "is anyone still using this?".
_info_lock_users: dict[str, int] = {}


async def _extract_cached(url: str) -> dict:
    hit = _INFO_CACHE.get(url)
    if hit and time.time() - hit[0] < _INFO_TTL:
        return hit[1]
    lock = _info_locks.setdefault(url, asyncio.Lock())
    _info_lock_users[url] = _info_lock_users.get(url, 0) + 1
    try:
        async with lock:
            hit = _INFO_CACHE.get(url)
            if hit and time.time() - hit[0] < _INFO_TTL:
                return hit[1]
            info = await asyncio.to_thread(_extract, url)
            _INFO_CACHE[url] = (time.time(), info)
            if len(_INFO_CACHE) > 256:  # simple LRU-ish cap
                oldest = min(_INFO_CACHE, key=lambda k: _INFO_CACHE[k][0])
                _INFO_CACHE.pop(oldest, None)
                if not _info_lock_users.get(oldest):
                    _info_locks.pop(oldest, None)
            return info
    finally:
        # Locks were once reclaimed only alongside a CACHE eviction, so a URL
        # whose extraction FAILED leaked its lock forever — no cache entry was
        # written, so nothing ever evicted it. A dead site or a client retrying
        # bad URLs grew this dict without bound.
        #
        # WHY A REFCOUNT AND NOT `lock.locked()`: asyncio.Lock.release() clears
        # _locked BEFORE the next waiter is scheduled, so there is a window
        # where locked() is False while a waiter is still queued. Popping the
        # lock there means the next caller setdefault()s a SECOND lock for the
        # same URL and extracts concurrently with the waiter — precisely the
        # duplicate-collapse this lock exists to prevent. The refcount counts
        # holders AND waiters, so only the genuine last one out reclaims.
        n = _info_lock_users.get(url, 1) - 1
        if n > 0:
            _info_lock_users[url] = n
        else:
            _info_lock_users.pop(url, None)
            if url not in _INFO_CACHE:
                _info_locks.pop(url, None)


def _safe_name(title: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", title)[:120]
    # Content-Disposition header is latin-1; drop non-ASCII to stay valid
    return name.encode("ascii", "ignore").decode().strip() or "video"


def _codec_label(vcodec: str) -> str:
    v = (vcodec or "").lower()
    if v.startswith("avc1"):
        return "h264"
    if v.startswith(("vp9", "vp09")):
        return "vp9"
    if v.startswith("av01"):
        return "av1"
    return v.split(".")[0] or "?"


def _fsize(f: dict) -> int:
    return f.get("filesize") or f.get("filesize_approx") or 0


def _curate_formats(info: dict) -> list[dict]:
    """Return one best option per height + audio-only."""
    fmts = info.get("formats") or []
    out: list[dict] = []

    # best-audio size — merged/compat downloads are video+audio, so the size
    # estimate (and the progress bar) must include audio, else the bar hits
    # 100% early (worse at low resolutions where audio is a bigger share).
    _aud = [f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"]
    aud_fs = _fsize(max(_aud, key=lambda f: (f.get("abr") or f.get("tbr") or 0))) if _aud else 0
    # heights that have an H.264 (avc1) video -> QuickTime-playable when the
    # "QuickTime (H.264)" toggle is on. Above ~1080p YouTube is VP9/AV1 only.
    avc_heights = {f["height"] for f in fmts if f.get("acodec") == "none"
                   and str(f.get("vcodec", "")).startswith("avc1") and f.get("height")}

    # progressive (video+audio in one file) — cheapest to serve, passthrough
    prog: dict[int, dict] = {}
    for f in fmts:
        if f.get("vcodec") != "none" and f.get("acodec") != "none" and f.get("height"):
            h = f["height"]
            if h not in prog or (f.get("tbr") or 0) > (prog[h].get("tbr") or 0):
                prog[h] = f

    # DASH video-only heights available for merge
    dash_heights = sorted(
        {f["height"] for f in fmts if f.get("vcodec") != "none"
         and f.get("acodec") == "none" and f.get("height")},
        reverse=True,
    )

    for h in dash_heights:
        if h in prog and h <= 720:
            f = prog[h]
            out.append({
                "kind": f"p:{f['format_id']}",
                "label": f"{h}p",
                "height": h,
                "ext": f.get("ext", "mp4"),
                "fps": f.get("fps"),
                "filesize": f.get("filesize") or f.get("filesize_approx"),
                "note": "progressive",
                "resumable": True,  # single direct URL -> Range proxy, browser-native
            })
        else:
            # best QUALITY at this height regardless of codec (matches the
            # download path selection — keep the two in sync)
            best = max(
                (f for f in fmts if f.get("height") == h and f.get("acodec") == "none"),
                key=lambda f: (f.get("tbr") or 0, f.get("filesize") or 0),
            )
            vfs = _fsize(best)
            out.append({
                "kind": f"m:{h}",
                "label": f"{h}p",
                "height": h,
                "ext": "mp4",
                "fps": best.get("fps"),
                "tbr": round(best.get("tbr") or 0),
                "filesize": (vfs + aud_fs) if vfs else None,  # video + audio
                "note": f"merged · {_codec_label(best.get('vcodec'))}",
                "h264": h in avc_heights,  # can serve QuickTime-safe H.264 here
            })

    # progressive heights with NO DASH sibling (Facebook: "sd"/"hd" are often
    # the only options; YouTube never hits this — DASH covers every height).
    for h in sorted(prog, reverse=True):
        if any(o["height"] == h for o in out):
            continue
        f = prog[h]
        out.append({
            "kind": f"p:{f['format_id']}",
            "label": f"{h}p",
            "height": h,
            "ext": f.get("ext", "mp4"),
            "fps": f.get("fps"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "note": "progressive",
            "resumable": True,
        })
    out.sort(key=lambda o: -o["height"])

    # compat entry: best H.264 height (H.264+AAC copy = plays in QuickTime /
    # everything). Max-quality picks above are often VP9/AV1, which QuickTime
    # cannot decode — real user complaint. H.264 doesn't exist >1080p on
    # YouTube, so this is the ceiling for "plays everywhere".
    avc = [f for f in fmts if f.get("acodec") == "none" and f.get("height")
           and str(f.get("vcodec", "")).startswith("avc1")]
    if avc:
        cb = max(avc, key=lambda f: (f["height"], f.get("tbr") or 0))
        ch = cb["height"]
        best_at_h = next((o for o in out if o["height"] == ch), None)
        if not (best_at_h and "h264" in best_at_h["note"]):
            out.append({
                "kind": f"c:{ch}",
                "label": f"{ch}p",
                "height": ch,
                "ext": "mp4",
                "fps": cb.get("fps"),
                "tbr": round(cb.get("tbr") or 0),
                "filesize": (_fsize(cb) + aud_fs) if _fsize(cb) else None,
                "note": "compat · h264",
            })

    audio = [f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"]
    if audio:
        best_a = max(audio, key=lambda f: (f.get("abr") or f.get("tbr") or 0))
        _ext = best_a.get("ext", "m4a")
        _codec = str(best_a.get("acodec") or "")
        # The best audio YouTube serves is usually OPUS. Handing it back
        # untouched is both higher quality AND cheaper than the MP3 entry
        # below, which decodes and re-encodes it (generation loss + CPU).
        # Label it with the real codec so the choice is visible instead of
        # everything being an anonymous "audio only".
        _nice = ("Opus" if "opus" in _codec else
                 "AAC" if _codec.startswith("mp4a") else
                 "FLAC" if "flac" in _codec else
                 _codec.split(".")[0].upper() or _ext.upper())
        out.append({
            "kind": "a:audio",
            "label": f"audio · {_nice}",
            "height": 0,
            "ext": _ext,
            "fps": None,
            "filesize": best_a.get("filesize") or best_a.get("filesize_approx"),
            "note": f"{int(best_a.get('abr') or 0)}kbps · original, no re-encode",
            "resumable": True,
            "lossless_passthrough": True,
        })
        # Second-best DISTINCT codec, when there is one — lets someone on an
        # Apple device take AAC directly rather than falling back to MP3,
        # which would be a needless re-encode of the same audio.
        _alt = None
        for f in sorted(audio, key=lambda f: -(f.get("abr") or f.get("tbr") or 0)):
            c = str(f.get("acodec") or "")
            same = ("opus" in c) == ("opus" in _codec) and \
                   c.startswith("mp4a") == _codec.startswith("mp4a")
            if not same:
                _alt = f
                break
        if _alt:
            c = str(_alt.get("acodec") or "")
            alt_nice = ("Opus" if "opus" in c else
                        "AAC" if c.startswith("mp4a") else
                        c.split(".")[0].upper())
            out.append({
                "kind": f"a:{_alt['format_id']}",
                "label": f"audio · {alt_nice}",
                "height": 0,
                "ext": _alt.get("ext", "m4a"),
                "fps": None,
                "filesize": _alt.get("filesize") or _alt.get("filesize_approx"),
                "note": f"{int(_alt.get('abr') or 0)}kbps · original, no re-encode",
                "resumable": True,
                "lossless_passthrough": True,
            })
        out.append({
            "kind": "mp3",
            "label": "MP3",
            "height": 0,
            "ext": "mp3",
            "fps": None,
            "filesize": None,  # transcoded, size unknown ahead of time
            "note": "re-encoded · for old players",
        })
    return out


# ---------- social bundle sites (X / Instagram) ----------
#
# yt-dlp handles post VIDEO (pre-merged H.264+AAC MP4s + HLS — no DASH split,
# so no FIFO merge needed; the best http MP4 is a single direct URL, perfect
# for the resumable Range proxy). yt-dlp does NOT handle image-only posts, so
# PHOTOS are resolved by gallery-dl in URL-resolution mode (`-j` = dump JSON,
# downloads nothing): YoinkT range-proxies the returned CDN URLs itself.
# Statelessness preserved — gallery-dl never writes media. Both engines run
# concurrently; either may fail alone (video-only post -> gdl empty;
# photo-only post -> yt-dlp "No video").

_GDL_TIMEOUT = int(os.environ.get("GDL_TIMEOUT", "30"))
_GDL_NOVIDEO = {  # per-extractor "images only" switch
    "x": "extractor.twitter.videos=false",
    "instagram": "extractor.instagram.videos=false",
    "facebook": "extractor.facebook.videos=false",
    # tiktok slideshows: gallery-dl returns images; leaving videos on is fine,
    # the extension filter drops the mp4/mp3 — no per-extractor key needed.
}


def _gdl_available() -> bool:
    """True if gallery-dl is usable. Prefer the importable MODULE over a
    `gallery-dl` binary on PATH: pip installs the module reliably, but the
    console script often isn't on the subprocess PATH (venvs, user site) — the
    old shutil.which() check wrongly reported "not installed" in that case."""
    if importlib.util.find_spec("gallery_dl") is not None:
        return True
    return bool(shutil.which("gallery-dl"))


def _gdl_cmd() -> list[str]:
    # run as a module with THIS interpreter -> no PATH dependency
    if importlib.util.find_spec("gallery_dl") is not None:
        return [sys.executable, "-m", "gallery_dl"]
    return ["gallery-dl"]


@functools.lru_cache(maxsize=1)
def _gdl_config_flag() -> list[str]:
    """Config-isolation flag for the photo resolver, if this build has one.

    Same reasoning as _base_flags: ambient config files must not reach a
    server process. Probed against --help rather than hardcoded, because the
    flag's spelling has moved between releases and guessing wrong would abort
    every photo resolution with a usage error — the failure mode we're trying
    to prevent. No match -> no flag, and photo resolution works as before."""
    try:
        out = subprocess.run(_gdl_cmd() + ["--help"], capture_output=True,
                             text=True, timeout=15).stdout
    except Exception as e:
        log.warning("photo resolver help probe failed: %s", e)
        return []
    for flag in ("--ignore-config", "--config-ignore"):
        if flag in out:
            return [flag]
    return []


def _gdl_photos(url: str, site: str = "x") -> list[dict]:
    """Resolve post photos via gallery-dl -j. Returns [{url, ext}]."""
    if not _gdl_available():
        return []
    cmd = _gdl_cmd() + ["-j"] + _gdl_config_flag()
    if site in _GDL_NOVIDEO:
        cmd += ["-o", _GDL_NOVIDEO[site]]
    if PROXY_URL:
        cmd += ["--proxy", PROXY_URL]
    ck = _cookies_for(site)
    if ck:
        cmd += ["--cookies", ck]
    cmd.append(url)
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=_GDL_TIMEOUT)
    except subprocess.TimeoutExpired:
        log.error("gallery-dl timed out for %s", url)
        return []
    if r.returncode != 0 and not r.stdout.strip():
        if r.stderr:
            log.error("gallery-dl failed: %s", r.stderr.decode(errors="replace")[-400:])
        return []
    import json
    try:
        msgs = json.loads(r.stdout)
    except ValueError:
        return []
    out = []
    for m in msgs:
        # gallery-dl message tuples: [3, url, metadata] = a file URL
        if (isinstance(m, list) and len(m) >= 2 and m[0] == 3
                and isinstance(m[1], str) and m[1].startswith("http")):
            meta = m[2] if len(m) > 2 and isinstance(m[2], dict) else {}
            ext = (meta.get("extension") or "jpg").lower()
            if ext in ("jpg", "jpeg", "png", "webp", "gif"):
                out.append({"url": m[1], "ext": ext})
    return out


# ---------- anonymous og:meta / embed scraper (last resort, no cookies) ----------
#
# Public IG/FB posts still expose their media through Open Graph meta tags and
# (for IG) the /embed/ endpoint — the same public preview data link-unfurlers
# read. This is what the snap* sites lean on. Best-effort: works for PUBLIC
# posts on a residential IP; datacenter IPs or private posts still need cookies.
# Runs ONLY when yt-dlp AND gallery-dl both came back empty.

_OG_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _og_fetch_html(url: str, site: str) -> str:
    import urllib.request
    headers = {"User-Agent": _OG_UA, "Accept-Language": "en-US,en;q=0.9"}
    ck = _cookies_for(site)
    if ck and os.path.exists(ck):
        # reuse cookies if the operator set them — helps on blocked IPs
        try:
            import http.cookiejar
            cj = http.cookiejar.MozillaCookieJar(ck)
            cj.load(ignore_discard=True, ignore_expires=True)
            opener = _egress_opener(urllib.request.HTTPCookieProcessor(cj))
            return opener.open(urllib.request.Request(_validate_egress(url),
                                                      headers=headers),
                               timeout=20).read().decode("utf-8", "replace")
        except HTTPException:
            raise  # an egress rejection is a real answer — don't fall through
        except Exception:
            pass
    return _egress_opener().open(
        urllib.request.Request(_validate_egress(url), headers=headers),
        timeout=20).read().decode("utf-8", "replace")


def _unescape_url(u: str) -> str:
    import html
    return html.unescape(u).replace("\\u0026", "&").replace("\\/", "/")


def _ext_of(u: str, default: str) -> str:
    m = re.search(r"\.(jpe?g|png|webp|gif|mp4)(?:\?|$)", u, re.I)
    return (m.group(1).lower().replace("jpeg", "jpg") if m else default)


def _og_scrape(url: str, site: str) -> dict:
    """Return {'photos':[{url,ext}], 'videos':[{url,ext}]} for a PUBLIC post."""
    fetch_url = url
    if site == "instagram":
        m = re.search(r"/(p|reel|reels|tv)/([\w-]+)", url)
        if m:  # embed endpoint is served logged-out; the main URL redirects to login
            fetch_url = f"https://www.instagram.com/{m.group(1)}/{m.group(2)}/embed/captioned/"
    try:
        html = _og_fetch_html(fetch_url, site)
    except Exception as e:
        log.info("og scrape fetch failed (%s): %s", site, str(e)[:160])
        return {"photos": [], "videos": []}

    photos: list[dict] = []
    videos: list[dict] = []
    seen: set[str] = set()

    def add(bucket, u, default_ext):
        u = _unescape_url(u)
        if u.startswith("http") and u not in seen:
            seen.add(u)
            bucket.append({"url": u, "ext": _ext_of(u, default_ext)})

    for prop in ("og:video:secure_url", "og:video:url", "og:video"):
        for m in re.finditer(
            rf'<meta[^>]+(?:property|name)=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)',
            html, re.I):
            add(videos, m.group(1), "mp4")
    # IG embed JSON: video_url / display_url (display_url = higher-res than og:image)
    for m in re.finditer(r'"video_url":"([^"]+)"', html):
        add(videos, m.group(1), "mp4")
    for prop in ("og:image:url", "og:image:secure_url", "og:image"):
        for m in re.finditer(
            rf'<meta[^>]+(?:property|name)=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)',
            html, re.I):
            add(photos, m.group(1), "jpg")
    for m in re.finditer(r'"display_url":"([^"]+)"', html):
        add(photos, m.group(1), "jpg")
    # IG embed <img class="EmbeddedMediaImage" src="...">
    for m in re.finditer(r'class="[^"]*EmbeddedMediaImage[^"]*"[^>]+src="([^"]+)"', html):
        add(photos, m.group(1), "jpg")

    # if a video was found, drop its poster image (avoid a dupe "photo" of the frame)
    if videos:
        photos = [p for p in photos if "video" not in p["url"].split("?")[0].lower()][:1] if photos else []
    return {"photos": photos, "videos": videos}


def _curate_prog_formats(info: dict) -> list[dict]:
    """One entry per height from the progressive http MP4s (+MP3). No merge,
    no compat — X/IG are already H.264+AAC in one file."""
    fmts = info.get("formats") or []
    prog: dict[int, dict] = {}
    for f in fmts:
        # http (not HLS) formats with both streams; height sometimes missing
        if (f.get("vcodec") != "none" and f.get("acodec") != "none"
                and str(f.get("protocol", "")).startswith("http")
                and not str(f.get("format_id", "")).startswith("hls")):
            h = f.get("height") or 0
            if h not in prog or (f.get("tbr") or 0) > (prog[h].get("tbr") or 0):
                prog[h] = f
    out = []
    for h in sorted(prog, reverse=True):
        f = prog[h]
        out.append({
            "kind": f"p:{f['format_id']}",
            "label": f"{h}p" if h else "video",
            "height": h,
            "ext": "mp4",
            "fps": f.get("fps"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "note": "progressive",
            "resumable": True,
        })
    if out:
        out.append({"kind": "mp3", "label": "MP3", "height": 0, "ext": "mp3",
                    "fps": None, "filesize": None, "note": "audio · mp3"})
    return out


def _best_prog_mp4(entry: dict) -> dict | None:
    """Best progressive http MP4 of one video entry (for v:<i> downloads)."""
    cands = [f for f in (entry.get("formats") or [])
             if f.get("vcodec") != "none" and f.get("acodec") != "none"
             and str(f.get("protocol", "")).startswith("http")
             and not str(f.get("format_id", "")).startswith("hls")]
    return max(cands, key=lambda f: (f.get("tbr") or 0), default=None)


# bundle cache: combined yt-dlp video info + gallery-dl photo URLs, same
# TTL/lock pattern as _extract_cached (photo CDN URLs are long-lived).
_X_CACHE: dict[str, tuple[float, dict]] = {}
_x_locks: dict[str, asyncio.Lock] = {}


async def _bundle_cached(url: str, site: str) -> dict:
    hit = _X_CACHE.get(url)
    if hit and time.time() - hit[0] < _INFO_TTL:
        return hit[1]
    lock = _x_locks.setdefault(url, asyncio.Lock())
    async with lock:
        hit = _X_CACHE.get(url)
        if hit and time.time() - hit[0] < _INFO_TTL:
            return hit[1]

        async def _video():
            try:
                return await asyncio.to_thread(_extract, url)
            except Exception as e:  # photo-only post: "No video could be found"
                log.info("%s video extract: %s", site, str(e)[:200])
                return None

        async def _photos():
            try:
                return await asyncio.to_thread(_gdl_photos, url, site)
            except Exception as e:
                log.error("%s photo resolve: %s", site, str(e)[:200])
                return []

        vinfo, photos = await asyncio.gather(_video(), _photos())
        og_videos: list[dict] = []
        # og:meta / IG-embed scrape — public posts, no cookies. Run whenever
        # yt-dlp found NO video (photo-only OR mixed post whose video failed),
        # so a mixed post still surfaces its video via og even if gallery-dl
        # already returned the photos.
        if vinfo is None:
            og = await asyncio.to_thread(_og_scrape, url, site)
            og_videos = og["videos"]
            if not photos:  # don't duplicate photos gallery-dl already found
                photos = og["photos"]
        if vinfo is None and not photos and not og_videos:
            env = f"COOKIES_FILE_{site.upper()}"
            gdl_missing = "" if _gdl_available() else \
                " (gallery-dl not importable — photos can't be resolved; run "\
                "'pip install gallery-dl', or delete .venv and re-run run-local.sh)"
            if gdl_missing:
                hint = gdl_missing
            elif site == "instagram":
                hint = (f" — this Instagram post is private or IP-blocked; "
                        f"set {env} (cookies.txt from a logged-in browser)")
            elif site == "facebook":
                hint = (f" — this Facebook post is private or IP-blocked; "
                        f"set {env} (cookies.txt from a logged-in browser)")
            else:
                hint = f" (deleted, protected, or NSFW — {env} may help)"
            raise HTTPException(
                502, f"extract failed: no video or photos found in this post{hint}")
        bundle = {"vinfo": vinfo, "photos": photos, "og_videos": og_videos}
        _X_CACHE[url] = (time.time(), bundle)
        if len(_X_CACHE) > 256:
            oldest = min(_X_CACHE, key=lambda k: _X_CACHE[k][0])
            _X_CACHE.pop(oldest, None)
            _x_locks.pop(oldest, None)
        return bundle


def _social_name(site: str, vinfo: dict) -> str:
    """Filename base for a social post: '<uploader>_<id>_<YYYY-MM-DD>'.
    Stable and unique — beats yt-dlp's generic 'Video by <user>' title."""
    v = vinfo or {}
    uploader = (v.get("uploader") or v.get("uploader_id")
                or v.get("channel") or site)
    vid = v.get("id") or v.get("display_id") or ""
    date = time.strftime("%Y-%m-%d")
    parts = [p for p in (str(uploader).strip(), str(vid).strip(), date) if p]
    return _safe_name(re.sub(r"\s+", "_", "_".join(parts))) or f"{site}_post"


def _curate_for(site: str, info: dict) -> list[dict]:
    """Merge sites (Facebook) get the full curation incl. DASH merge kinds;
    single-file sites (X/IG/TikTok) get the progressive-only curation."""
    return _curate_formats(info) if site in MERGE_SITES else _curate_prog_formats(info)


def _social_api_info(url: str, bundle: dict, site: str = "x") -> dict:
    vinfo, photos = bundle["vinfo"], bundle["photos"]
    formats, media, entries = [], [], []
    title = uploader = thumb = None
    duration = None
    if vinfo:
        title = vinfo.get("title")
        uploader = vinfo.get("uploader") or vinfo.get("channel")
        thumb = vinfo.get("thumbnail")
        if vinfo.get("_type") == "playlist" or vinfo.get("entries"):
            entries = [e for e in (vinfo.get("entries") or []) if e]
            thumb = thumb or (entries[0].get("thumbnail") if entries else None)
            if len(entries) == 1:
                formats = _curate_for(site, entries[0])
                duration = entries[0].get("duration")
            else:  # multi-video post: one best-MP4 item per video
                for i, e in enumerate(entries):
                    best = _best_prog_mp4(e)
                    media.append({
                        "type": "video", "kind": f"v:{i}",
                        "label": f"Video {i + 1}",
                        "ext": "mp4", "thumbnail": e.get("thumbnail"),
                        "filesize": best and (best.get("filesize")
                                              or best.get("filesize_approx")),
                        "duration": e.get("duration"), "resumable": True,
                    })
        else:
            formats = _curate_for(site, vinfo)
            duration = vinfo.get("duration")
    # og-scraped direct videos (public post, no yt-dlp entry) -> g:<i>
    for i, v in enumerate(bundle.get("og_videos") or []):
        media.append({
            "type": "video", "kind": f"g:{i}", "label": f"Video {i + 1}",
            "ext": v.get("ext", "mp4"), "thumbnail": thumb, "filesize": None,
            "resumable": True,
        })
    for i, p in enumerate(photos):
        media.append({
            "type": "photo", "kind": f"i:{i}", "label": f"Photo {i + 1}",
            "ext": p["ext"], "thumbnail": p["url"], "filesize": None,
            "resumable": True,
        })
    if not thumb and photos:
        thumb = photos[0]["url"]
    return {
        "site": site,
        "id": vinfo.get("id") if vinfo else None,
        "title": title or {"instagram": "Instagram post", "facebook": "Facebook post",
                           "tiktok": "TikTok post"}.get(site, "X post"),
        "channel": uploader,
        "duration": duration,
        "thumbnail": thumb,
        "formats": formats,
        "media": media,
        "subs": [],
        "chapters": 0,
    }


def _vtt_to_srt(vtt: str) -> str:
    """Naive WEBVTT -> SRT: drop header/styles, comma timestamps, number cues."""
    out, n = [], 0
    for block in re.split(r"\n\s*\n", vtt.replace("\r", "")):
        lines = [l for l in block.strip().split("\n") if l]
        while lines and "-->" not in lines[0]:
            lines.pop(0)  # drop WEBVTT/NOTE/STYLE/cue-id lines
        if not lines:
            continue
        ts = re.sub(r"(\d{2}:\d{2}(?::\d{2})?)\.(\d{3})", lambda m: (
            ("00:" + m.group(1)) if m.group(1).count(":") == 1 else m.group(1)
        ) + "," + m.group(2), lines[0].split(" align")[0].split(" position")[0])
        text = [re.sub(r"<[^>]+>", "", l) for l in lines[1:]]
        if not text:
            continue
        n += 1
        out.append(f"{n}\n{ts}\n" + "\n".join(text))
    return "\n\n".join(out) + "\n"


def _fetch_url(u: str) -> bytes:
    return _egress_opener().open(_validate_egress(u), timeout=30).read()


# ---- SponsorBlock: mark mode --------------------------------------------
# `sb=1` (remove) cuts the segments out, which means the whole file has to be
# downloaded and re-cut — that's why it lives on the buffered path and needs
# scratch disk. `sb=mark` instead turns each segment into a CHAPTER, which
# costs nothing: the streaming merge already writes an ffmetadata chapter
# block, so marked segments are just more chapters. Fully streaming, no disk,
# no re-encode. The user skips with their player's chapter controls.
_SB_API = os.environ.get("SPONSORBLOCK_API", "https://sponsor.ajay.app")
_SB_CATEGORIES = os.environ.get(
    "SPONSORBLOCK_CATEGORIES",
    "sponsor,selfpromo,interaction,intro,outro,preview,music_offtopic")
_SB_LABELS = {
    "sponsor": "Sponsor", "selfpromo": "Self-promo", "interaction": "Interaction",
    "intro": "Intro", "outro": "Outro", "preview": "Preview",
    "music_offtopic": "Non-music", "filler": "Filler",
}


def _sb_segments(video_id: str) -> list[dict]:
    """Fetch SponsorBlock segments for a video. Returns [] on any failure —
    marking is a nice-to-have and must never fail a download."""
    import json
    from urllib.parse import quote
    cats = json.dumps([c.strip() for c in _SB_CATEGORIES.split(",") if c.strip()])
    url = (f"{_SB_API}/api/skipSegments?videoID={quote(video_id)}"
           f"&categories={quote(cats)}")
    try:
        raw = _egress_opener().open(_validate_egress(url), timeout=10).read()
        data = json.loads(raw.decode("utf-8", "replace"))
    except Exception as e:
        log.info("sponsorblock lookup failed for %s: %s", video_id, str(e)[:120])
        return []
    segs = []
    for item in data if isinstance(data, list) else []:
        seg = item.get("segment") or []
        if len(seg) == 2 and seg[1] > seg[0]:
            segs.append({"start": float(seg[0]), "end": float(seg[1]),
                         "category": item.get("category", "segment")})
    segs.sort(key=lambda s: s["start"])
    return segs


def _merge_sb_chapters(chapters: list | None, segs: list[dict]) -> list[dict]:
    """Fold SponsorBlock segments into the chapter list as extra chapters.

    Kept simple on purpose: ffmpeg chapters must not overlap, so rather than
    trying to split existing chapters around each segment (which produces
    fragile off-by-one boundaries on long videos), SB segments WIN for their
    time range and surrounding chapters are trimmed around them."""
    marks = [{"start_time": s["start"], "end_time": s["end"],
              "title": f"[{_SB_LABELS.get(s['category'], s['category'])}]"}
             for s in segs]
    if not marks:
        return list(chapters or [])
    out = []
    for ch in (chapters or []):
        cs, ce = ch.get("start_time", 0), ch.get("end_time", 0)
        for m in marks:
            if m["start_time"] < ce and m["end_time"] > cs:
                # overlaps a mark: keep only the part before it
                ce = min(ce, m["start_time"])
        if ce > cs:
            out.append({**ch, "start_time": cs, "end_time": ce})
    out.extend(marks)
    out.sort(key=lambda c: c["start_time"])
    return out


def _ffmeta_chapters(chapters: list[dict]) -> str:
    lines = [";FFMETADATA1"]
    for c in chapters:
        s, e = c.get("start_time"), c.get("end_time")
        if s is None or e is None:
            continue
        title = str(c.get("title") or "").replace("\n", " ")
        title = re.sub(r"([=;#\\\n])", r"\\\1", title)
        lines += ["[CHAPTER]", "TIMEBASE=1/1000",
                  f"START={int(s*1000)}", f"END={int(e*1000)}", f"title={title}"]
    return "\n".join(lines) + "\n"


@app.get("/api/health")
async def health():
    eng = _engine_info()
    return {"ok": True, "version": APP_VERSION,
            "max_concurrency": MAX_CONCURRENCY,
            "turbo": bool(shutil.which("aria2c")),
            # Surfaced so a slow bulk run can be diagnosed without guessing:
            # connections are a shared budget, not per-lane.
            "turbo_budget": {"total_connections": TURBO_CONN_BUDGET,
                             "min_per_download": TURBO_MIN_CONNS,
                             "lanes": TURBO_LANES,
                             "active": _turbo_active},
            "scratch": _scratch_root(),
            "js_runtime": bool(shutil.which("deno") or shutil.which("node")),
            "pot": bool(PO_TOKEN or POT_PROVIDER_URL),
            "proxy": bool(PROXY_URL), "cookies": bool(COOKIES_FILE),
            # engine build + how old it is. A stale engine is the single most
            # common cause of "it worked yesterday" — surface it here so it's
            # one curl away instead of a guess.
            "engine": eng,
            "stealth": {"available": _stealth_available(),
                        "profiles": {s: _stealth_for(s)
                                     for s in sorted(ENABLED_SITES & SITE_RES.keys())}},
            "sites": {s: {"cookies": bool(_cookies_for(s)),
                          "stealth": _stealth_for(s)}
                      for s in sorted(ENABLED_SITES & SITE_RES.keys())},
            "gallery_dl": _gdl_available(),
            "api_key": bool(API_KEY), "rate_limit": RATE_LIMIT_RPM,
            "cookie_upload": ALLOW_COOKIE_UPLOAD}


@app.post("/api/cookies/{site}")
async def api_cookies_upload(site: str, request: Request):
    """Accept a Netscape cookies.txt for one site (raw body or multipart).
    Writes to COOKIES_DIR and registers a runtime override. SINGLE-INSTANCE
    convenience — not shared across k8s pods (use mounted Secrets there)."""
    if not ALLOW_COOKIE_UPLOAD:
        raise HTTPException(403, "cookie upload disabled (ALLOW_COOKIE_UPLOAD=0)")
    if site not in SITE_RES:
        raise HTTPException(400, "unknown site")
    body = await request.body()
    text = body.decode("utf-8", "replace")
    # strip a multipart wrapper if the browser sent FormData
    if "filename=" in text[:200] and "\r\n\r\n" in text:
        text = text.split("\r\n\r\n", 1)[1].rsplit("\r\n--", 1)[0]
    if "\t" not in text and "# Netscape" not in text and "# HTTP Cookie" not in text:
        raise HTTPException(400, "not a Netscape cookies.txt (tab-separated)")
    os.makedirs(COOKIES_DIR, exist_ok=True)
    path = os.path.join(COOKIES_DIR, f"{site}.txt")
    with open(path, "w") as f:
        if not text.lstrip().startswith("#"):
            f.write("# Netscape HTTP Cookie File\n")
        f.write(text)
    os.chmod(path, 0o600)
    SITE_COOKIES_RUNTIME[site] = path
    # dropping cookies can change which formats resolve — clear caches for it
    _INFO_CACHE.clear()
    _X_CACHE.clear()
    log.info("cookies uploaded for %s -> %s", site, path)
    return {"ok": True, "site": site}


@app.delete("/api/cookies/{site}")
async def api_cookies_clear(site: str):
    if not ALLOW_COOKIE_UPLOAD:
        raise HTTPException(403, "cookie upload disabled")
    path = SITE_COOKIES_RUNTIME.pop(site, None)
    if path:
        with suppress(OSError):
            os.remove(path)
    _INFO_CACHE.clear()
    _X_CACHE.clear()
    return {"ok": True, "site": site, "cleared": bool(path)}


@app.get("/api/info")
async def api_info(url: str = Query(...)):
    site = _require_site(url)
    if site in BUNDLE_SITES:
        async with sem:
            try:
                bundle = await _bundle_cached(url, site)
            except HTTPException:
                raise
            except Exception as e:
                raise _classify_error(e, site) from e
        return _social_api_info(url, bundle, site)
    async with sem:
        try:
            info = await _extract_cached(url)
        except Exception as e:  # yt-dlp raises many types; surface message
            raise _classify_error(e, site) from e
    subs = [{"lang": l, "auto": False} for l in (info.get("subtitles") or {})]
    subs += [{"lang": l, "auto": True} for l in (info.get("automatic_captions") or {})
             if l not in {s["lang"] for s in subs}]
    return {
        "site": site,
        "id": info.get("id"),
        "title": info.get("title"),
        "channel": info.get("channel") or info.get("uploader"),
        "duration": info.get("duration"),
        "thumbnail": info.get("thumbnail"),
        "formats": _curate_formats(info),
        "subs": subs[:30],
        "chapters": len(info.get("chapters") or []),
    }


@app.get("/api/playlist")
async def api_playlist(url: str = Query(...)):
    _require_youtube(url)

    def _extract_flat():
        import yt_dlp

        opts = _ydl_opts() | {"extract_flat": "in_playlist", "noplaylist": False}
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    async with sem:
        try:
            info = await asyncio.to_thread(_extract_flat)
        except Exception as e:
            raise _classify_error(e, "youtube") from e
    entries = info.get("entries") or []
    out, avail = [], 0
    for e in entries[:400]:
        if not e:  # phantom slot
            out.append({"url": None, "title": None, "duration": None,
                        "thumbnail": None, "available": False})
            continue
        title = e.get("title")
        vid = e.get("id")
        # deleted/private/members-only videos keep a slot but can't be fetched.
        # extract_flat marks them: title "[Private video]"/"[Deleted video]",
        # or availability set, or no id at all.
        placeholder = bool(title) and title.startswith("[") and title.endswith("]")
        unwatchable = str(e.get("availability") or "") in {
            "private", "premium_only", "subscriber_only", "needs_auth"}
        ok = bool(vid) and not placeholder and not unwatchable
        if ok:
            avail += 1
        out.append({
            "url": e.get("url") or (f"https://www.youtube.com/watch?v={vid}" if vid else None),
            "title": title,
            "duration": e.get("duration"),
            "thumbnail": (e.get("thumbnails") or [{}])[-1].get("url"),
            "available": ok,
        })
    return {
        "title": info.get("title") or "playlist",
        "count": len(entries),
        "available": avail,
        "entries": out,
    }


@app.get("/api/subs")
async def api_subs(url: str = Query(...), lang: str = Query(...)):
    _require_youtube(url)
    try:
        info = await _extract_cached(url)
    except Exception as e:
        raise _classify_error(e, "youtube") from e
    pools = [info.get("subtitles") or {}, info.get("automatic_captions") or {}]
    tracks = next((p[lang] for p in pools if lang in p), None)
    if not tracks:
        raise HTTPException(404, "no subtitles for that language")
    track = next((t for t in tracks if t.get("ext") == "vtt"), tracks[0])
    try:
        raw = await asyncio.to_thread(_fetch_url, track["url"])
    except Exception as e:
        raise _classify_error(e, _site_of(url) or "youtube") from e
    srt = _vtt_to_srt(raw.decode("utf-8", "replace"))
    name = _safe_name(info.get("title", "video"))
    from fastapi.responses import Response
    return Response(srt, media_type="application/x-subrip", headers={
        "Content-Disposition": f'attachment; filename="{name}.{lang}.srt"'})


async def _stream_proc(cmd: list[str], media_type: str, filename: str):
    """Spawn subprocess, stream stdout to client, kill on disconnect.

    Semaphore must be held for the WHOLE stream, so acquire inside the
    generator — not in the enclosing function, which returns immediately.
    """

    async def gen():
        async with sem:
            # cwd MUST be writable: --concurrent-fragments buffers fragment
            # temp files in cwd even with -o -. /app is read-only for the
            # app user -> yt-dlp dies instantly if cwd isn't moved.
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=tempfile.gettempdir(),
            )
            try:
                while True:
                    # STALL WATCHDOG: kill if the subprocess produces no data
                    # for STREAM_STALL_TIMEOUT. A hung yt-dlp (bot-check /
                    # network stall) would otherwise hold its semaphore slot
                    # forever at "0 B · Starting", starving the queue. Note we
                    # block on stdout.read here, NOT on yield — a paused client
                    # sits in the yield below, so this never false-kills it.
                    try:
                        chunk = await asyncio.wait_for(
                            proc.stdout.read(256 * 1024), timeout=STREAM_STALL_TIMEOUT)
                    except asyncio.TimeoutError:
                        log.error("stream stalled %ss, killing %s", STREAM_STALL_TIMEOUT, cmd[0])
                        break
                    if not chunk:
                        break
                    yield chunk
            finally:
                if proc.returncode is None:
                    with suppress(ProcessLookupError):
                        proc.kill()
                err = (await proc.stderr.read()).decode(errors="replace")
                await proc.wait()
                if proc.returncode not in (0, None, -9) and err:
                    log.error("download failed (%s) url=%s: %s",
                              cmd[0], cmd[-1], err.strip()[-800:])

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    return StreamingResponse(gen(), media_type=media_type, headers=headers)


def _base_flags(site: str = "youtube") -> list[str]:
    # Config isolation comes FIRST and is not optional. The engine binary
    # otherwise merges settings from several system/user config paths, so a
    # stray file on the host — or in a future base image — silently rewrites
    # the flags we build here (a --format or --paths override would break the
    # streaming contract outright). A server must be reproducible: every knob
    # comes from this function or from env, nothing from ambient files.
    flags = ["--ignore-config", "--no-config-locations",
             "--no-playlist", "--quiet", "--remote-components", "ejs:github"]
    if PROXY_URL:
        flags += ["--proxy", PROXY_URL]
    ck = _cookies_for(site)
    if ck:
        flags += ["--cookies", ck]
    prof = _stealth_for(site)
    if prof:
        flags += ["--impersonate", prof]
    flags += _extractor_args_cli()
    return flags


def _common_flags(site: str = "youtube") -> list[str]:
    # streaming paths: yt-dlp's own parallel fragments + range chunking
    flags = _base_flags(site) + ["--concurrent-fragments", str(CONCURRENT_FRAGMENTS)]
    if HTTP_CHUNK_SIZE not in ("0", "", "off"):
        flags += ["--http-chunk-size", HTTP_CHUNK_SIZE]
    # NB (v38): do NOT force youtube player_client here. v34 injected
    # player_client=default,tv,mweb,web_safari on every no-cookie merge to help
    # age-gated MERGES — but the tv/mweb clients hand back throttled
    # single-connection formats, which halved normal 1080p download speed
    # (~10 -> ~2-4 MB/s). The default clients already auto-add tv_embedded for
    # age-gated videos, and progressive/audio still work via the _extract age
    # retry, so age-gated merges lose little while normal speed is restored.
    return flags


def _stream_merge(
    url: str, v_id: str, a_id: str, filename: str,
    audio_copy: bool = True, chapters: list | None = None,
    site: str = "youtube",
) -> StreamingResponse:
    """Merge DASH video+audio into MKV, piped to client.

    ffmpeg fetching googlevideo URLs directly is SLOW: one plain connection,
    no range chunking, per-connection throttle. Instead two yt-dlp workers
    (each with concurrent fragments) download in parallel and feed ffmpeg
    through named pipes. ffmpeg only muxes (-c copy) — near-zero CPU.

    Open/spawn order matters to avoid FIFO deadlock:
      spawn ffmpeg -> open v write-end (unblocks when ffmpeg opens v) ->
      spawn v writer -> open a write-end (ffmpeg opens a only after probing v,
      which needs v data flowing) -> spawn a writer.

    Output is ALWAYS fragmented MP4 (frag_keyframe+empty_moov) — the only MP4
    flavor writable to a non-seekable pipe. Video is ALWAYS stream-copied
    (never transcode video: massive CPU). Audio: copy when already AAC
    (audio_copy=True), else transcode Opus->AAC — audio encode is cheap
    (~100x realtime). VP9/AV1 copied into MP4 is valid (vp09/av01 boxes);
    plays in browsers/VLC/mpv/Win11; very old QuickTime may refuse — that is
    a codec problem, not a container problem, and MKV wouldn't fix it either.
    """
    mux = ["-movflags", "frag_keyframe+empty_moov+default_base_moof",
           "-strict", "experimental", "-f", "mp4"]
    acodec = ["-c:a", "copy"] if audio_copy else ["-c:a", "aac", "-b:a", "160k"]
    media_type = "video/mp4"

    async def gen():
        async with sem:
            tmp = tempfile.mkdtemp(prefix="ytgrab-")
            v_fifo, a_fifo = os.path.join(tmp, "v"), os.path.join(tmp, "a")
            os.mkfifo(v_fifo)
            os.mkfifo(a_fifo)
            procs = []

            def _errfile(tag: str):
                return open(os.path.join(tmp, f"{tag}.err"), "wb")

            meta_args: list[str] = []
            if chapters:
                meta = os.path.join(tmp, "ffmeta.txt")
                with open(meta, "w") as mf:
                    mf.write(_ffmeta_chapters(chapters))
                meta_args = ["-i", meta, "-map_metadata", "2", "-map_chapters", "2"]
            try:
                with _errfile("ffmpeg") as ef:
                    ff = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-hide_banner", "-loglevel", "error",
                        "-i", v_fifo, "-i", a_fifo, *meta_args,
                        "-map", "0:v:0", "-map", "1:a:0",
                        "-c:v", "copy", *acodec, *mux, "pipe:1",
                        stdout=subprocess.PIPE, stderr=ef,
                    )
                procs.append(("ffmpeg", ff))
                for tag, fifo, fmt_id in (("video", v_fifo, v_id), ("audio", a_fifo, a_id)):
                    fd = await asyncio.to_thread(open, fifo, "wb")
                    try:
                        with _errfile(tag) as ef:
                            w = await asyncio.create_subprocess_exec(
                                "yt-dlp", *_common_flags(site), "-f", fmt_id, "-o", "-", url,
                                stdout=fd, stderr=ef,
                                cwd=tmp,  # fragment temp files need writable cwd
                            )
                        procs.append((tag, w))
                    finally:
                        # CRITICAL: close parent's write-end copy. The writer
                        # holds its own dup; if the parent keeps this open the
                        # FIFO never delivers EOF and ffmpeg hangs at the end
                        # of every merge. Found by live pipeline test.
                        fd.close()
                while True:
                    try:
                        chunk = await asyncio.wait_for(
                            ff.stdout.read(256 * 1024), timeout=STREAM_STALL_TIMEOUT)
                    except asyncio.TimeoutError:
                        log.error("merge stalled %ss, killing", STREAM_STALL_TIMEOUT)
                        break
                    if not chunk:
                        break
                    yield chunk
            finally:
                for _, p in procs:
                    if p.returncode is None:
                        with suppress(ProcessLookupError):
                            p.kill()
                for tag, p in procs:
                    await p.wait()
                    if p.returncode not in (0, -9):
                        with suppress(OSError):
                            err = open(os.path.join(tmp, f"{tag}.err"), "rb").read()
                            if err:
                                log.error("merge %s failed: %s",
                                          tag, err.decode(errors="replace").strip()[-800:])
                shutil.rmtree(tmp, ignore_errors=True)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    return StreamingResponse(gen(), media_type=media_type, headers=headers)


def _clip_cmd(inputs: list[str], start: float, dur: float,
              acodec: list[str], maps: list[str]) -> list[str]:
    """ffmpeg fetches direct URLs for clips: -ss before -i = fast keyframe
    seek server-side, only the slice is downloaded. Slower per-byte than the
    yt-dlp workers but clips are short — simplicity wins here."""
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
    if PROXY_URL:
        cmd += ["-http_proxy", PROXY_URL]
    for u in inputs:
        cmd += ["-ss", f"{start:.3f}", "-i", u]
    cmd += [*maps, "-c:v", "copy", *acodec, "-t", f"{dur:.3f}",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "-strict", "experimental", "-f", "mp4", "pipe:1"]
    return cmd


def _range_proxy(direct_url: str, request: Request, filename: str,
                 media_type: str, extra_headers: dict | None = None) -> StreamingResponse:
    """Proxy a single direct (googlevideo/twimg/fbcdn/igcdn) URL, forwarding
    the client's Range header. Gives resumable, browser-native downloads for
    progressive/audio formats — no subprocess, no memory blob, and the browser
    can resume a dropped download. The direct URL is IP-bound to THIS server,
    so we must proxy rather than redirect. Blocking urllib read is pushed to
    threads. extra_headers: the format's yt-dlp `http_headers` — social CDNs
    (twimg/fbcdn/cdninstagram) can 403 bare requests; YouTube never needed
    them, which is why v30 X downloads failed silently."""
    import urllib.request

    req_headers = {"User-Agent": "Mozilla/5.0"}
    for k, v in (extra_headers or {}).items():
        if k.lower() not in ("range", "host", "accept-encoding") and isinstance(v, str):
            req_headers[k] = v
    rng = request.headers.get("range")
    if rng:
        req_headers["Range"] = rng
    opener = _egress_opener()

    try:
        r = opener.open(urllib.request.Request(_validate_egress(direct_url),
                                               headers=req_headers), timeout=30)
    except HTTPException:
        raise  # egress rejection already carries its own status + reason
    except Exception as e:
        log.error("range proxy failed for %s…: %s", direct_url[:120], e)
        raise HTTPException(502, f"upstream fetch failed: {e}") from e

    status = getattr(r, "status", 200) or 200
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
    }
    for h in ("Content-Length", "Content-Range"):
        v = r.headers.get(h)
        if v:
            headers[h] = v

    async def gen():
        async with sem:
            try:
                while True:
                    chunk = await asyncio.to_thread(r.read, 256 * 1024)
                    if not chunk:
                        break
                    yield chunk
            finally:
                with suppress(Exception):
                    r.close()

    return StreamingResponse(gen(), status_code=status,
                             media_type=media_type, headers=headers)


def _sb_selector(kind: str, info: dict, codec: str = "") -> str:
    """Map a YoinkT kind to a yt-dlp format selector for the buffered path
    (turbo / SponsorBlock — yt-dlp owns the whole download+merge+cut pipeline
    there, so the format choice has to be expressed as a selector string
    instead of the explicit format IDs the streaming path picks).

    MUST mirror the streaming path's codec policy. It previously did not:
    `c:1080` and `m:1080` both returned `bestvideo[height<=1080]+bestaudio`,
    which silently discarded the H.264 constraint. `c:` exists ONLY to
    guarantee a QuickTime-playable file, and `codec=h264` is the same promise
    from the UI toggle — so turbo produced VP9/AV1 files that QuickTime can't
    open, with no warning."""
    if kind.startswith("p:"):
        return kind[2:]
    if kind.startswith(("m:", "c:")):
        h = int(kind[2:])
        strict = kind.startswith("c:")       # compat kind: H.264 or nothing
        want_avc = strict or codec == "h264"  # UI toggle: H.264 preferred
        if want_avc:
            # exact height first, then any height <= h — matching the
            # streaming path, which looks for avc1 at exactly this height
            chain = [
                f"bestvideo[height={h}][vcodec^=avc1]+bestaudio[acodec^=mp4a]",
                f"bestvideo[height={h}][vcodec^=avc1]+bestaudio",
                f"bestvideo[height<={h}][vcodec^=avc1]+bestaudio",
                f"best[height<={h}][vcodec^=avc1]",
            ]
            if not strict:
                # m:+codec=h264 falls back to best-quality when no H.264
                # exists at all, exactly like the streaming branch does.
                # c: does NOT: the streaming path 404s there, and a silent
                # non-H.264 file is precisely the bug being fixed.
                chain += [f"bestvideo[height={h}]+bestaudio",
                          f"bestvideo[height<={h}]+bestaudio",
                          f"best[height<={h}]"]
            return "/".join(chain)
        # quality over codec (streaming path's default): prefer the exact
        # height, let yt-dlp's own sort pick the best codec at it
        return (f"bestvideo[height={h}]+bestaudio/"
                f"bestvideo[height<={h}]+bestaudio/best[height<={h}]")
    return "bestaudio"


async def _file_download(url: str, selector: str, name: str, audio_only: bool,
                         sb: bool, turbo: bool, site: str = "youtube",
                         est_bytes: int = 0) -> StreamingResponse:
    """Buffered download path for options that CAN'T stream:
      - turbo: aria2c opens many parallel connections (each gets its own
        YouTube throttle allowance) -> much faster single downloads, but
        aria2c can't pipe to stdout, so it writes to a temp file.
      - sb: SponsorBlock removal post-processes (cuts) the whole file.
    yt-dlp downloads to a temp dir, then we stream the finished file and
    delete it. NOT zero-storage — needs real disk in /tmp (k8s: size the tmp
    emptyDir well above 16Mi). Both options combine."""

    # PREPARE FIRST, then respond. This used to run inside the response
    # generator, which meant the 200 and Content-Disposition were already on
    # the wire before yt-dlp ran — so any failure ended the generator and the
    # client received a correctly-named, completely EMPTY .mp4 with no error
    # at all. Every turbo/SponsorBlock failure was invisible.
    #
    # This path is fully buffered by nature (aria2c can't pipe to stdout;
    # SponsorBlock cuts the finished file), so the client is waiting either
    # way — doing the work before the response costs no extra latency and
    # buys real HTTP status codes. Long prepares need the same generous
    # ingress read timeouts the streaming paths already require.
    # Pre-flight: reserve the estimated size against the shared budget AND
    # check the volume actually has room. Refusing up front with a clear 507
    # beats filling the volume and failing halfway through — especially in a
    # batch, where one oversized item would otherwise take the whole run down.
    global _scratch_reserved
    root = _scratch_root()
    if est_bytes > SCRATCH_BUDGET:
        raise HTTPException(
            507, {"hint": f"This download needs about {est_bytes // 1024**2} MB of "
                          f"scratch space but the budget is "
                          f"{SCRATCH_BUDGET // 1024**2} MB. Raise SCRATCH_BUDGET, "
                          "or download it without Turbo/SponsorBlock (the "
                          "streaming path needs no disk).",
                  "raw": f"est={est_bytes} budget={SCRATCH_BUDGET}"})
    if _scratch_reserved + est_bytes > SCRATCH_BUDGET:
        raise HTTPException(
            503, {"hint": "The server is already staging as much as it can hold. "
                          "Retry in a moment, or use fewer batch lanes.",
                  "raw": f"reserved={_scratch_reserved} est={est_bytes} "
                         f"budget={SCRATCH_BUDGET}"})
    try:
        free = shutil.disk_usage(root).free
    except Exception:
        free = None
    if free is not None and est_bytes > free:
        raise HTTPException(
            507, {"hint": f"Not enough free space in {root}: needs about "
                          f"{est_bytes // 1024**2} MB, {free // 1024**2} MB free. "
                          "On Kubernetes, grow the scratch volume.",
                  "raw": f"est={est_bytes} free={free} dir={root}"})
    _scratch_reserved += est_bytes

    # DISK-backed scratch, never the RAM tmpfs that holds the merge FIFOs —
    # this writes the whole video (see SCRATCH_DIR).
    tmp = tempfile.mkdtemp(prefix="ytgrab-dl-", dir=root)

    def _build_cmd(conns: int) -> list[str]:
        outtmpl = os.path.join(tmp, "out.%(ext)s")
        c = ["yt-dlp", *_base_flags(site), "-f", selector, "-o", outtmpl]
        if turbo:
            # -x/-j scale with the shared budget; -s matches -x so a single
            # file is actually split that many ways. file-allocation=none
            # starts instantly, min-split-size=1M keeps every connection busy.
            c += ["--downloader", "aria2c",
                  "--downloader-args",
                  f"aria2c:-x{conns} -s{conns} -k1M -j{conns} "
                  "--min-split-size=1M --file-allocation=none "
                  "--optimize-concurrent-downloads=true"]
        else:
            c += ["--concurrent-fragments", str(CONCURRENT_FRAGMENTS)]
        if sb:
            c += ["--sponsorblock-remove", "default"]
        if audio_only:
            c += ["-x", "--audio-format", "mp3"]
        else:
            c += ["--merge-output-format", "mp4", "--remux-video", "mp4"]
        return c + [url]

    async def _prepare() -> None:
        """Run the download. Raises HTTPException on failure."""
        global _turbo_active
        conns = CONCURRENT_FRAGMENTS
        if turbo:
            _turbo_active += 1
            conns = _turbo_conns(_turbo_active)
            log.info("turbo prepare: %d connection(s), %d active",
                     conns, _turbo_active)
        try:
            # Slot held for the PREPARE only — that's the expensive part, and
            # unlike the streaming paths it happens in the handler rather than
            # the generator. Streaming the finished file back is disk->socket
            # and cheap. Aggregate scratch usage is still uncapped (P2).
            async with sem:
                proc = await asyncio.create_subprocess_exec(
                    *_build_cmd(conns), stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE, cwd=tmp)
                _, err = await proc.communicate()
        finally:
            if turbo:
                _turbo_active -= 1
        files = [f for f in os.listdir(tmp) if f.startswith("out.")]
        if proc.returncode != 0 or not files:
            msg = (err.decode(errors="replace").strip()[-400:] if err
                   else "produced no output")
            log.error("buffered download failed (turbo=%s sb=%s): %s",
                      turbo, sb, msg)
            mode = " + ".join(m for m, on in
                              (("Turbo", turbo), ("SponsorBlock", sb)) if on)
            if "No space left" in msg or "ENOSPC" in msg:
                raise HTTPException(
                    507, f"{mode} ran out of scratch space in {_scratch_root()}. "
                         "On Kubernetes give the pod a disk-backed volume and "
                         "point SCRATCH_DIR at it.")
            raise HTTPException(502, f"{mode} download failed: {msg}")

    try:
        if turbo:
            # Lane cap: every running turbo download still clears
            # TURBO_MIN_CONNS out of the shared budget. Acquired BEFORE sem,
            # consistently, so the two semaphores can't deadlock.
            async with turbo_sem:
                await _prepare()
        else:
            await _prepare()
        path = os.path.join(tmp, [f for f in os.listdir(tmp)
                                  if f.startswith("out.")][0])
    except BaseException:
        _scratch_reserved -= est_bytes
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    async def gen():
        try:
            with open(path, "rb") as fh:
                while chunk := await asyncio.to_thread(fh.read, 1024 * 1024):
                    yield chunk
        finally:
            global _scratch_reserved
            _scratch_reserved -= est_bytes
            shutil.rmtree(tmp, ignore_errors=True)

    ext = "mp3" if audio_only else "mp4"
    mt = "audio/mpeg" if audio_only else "video/mp4"
    return StreamingResponse(gen(), media_type=mt, headers={
        "Content-Disposition": f'attachment; filename="{name}.{ext}"',
        "Cache-Control": "no-store"})


@app.get("/api/download")
async def api_download(
    request: Request,
    url: str = Query(...), kind: str = Query(...),
    start: float | None = Query(None, ge=0), end: float | None = Query(None, gt=0),
    sb: str = Query(""), turbo: int = Query(0), codec: str = Query(""),
):
    site = _require_site(url)
    clip = start is not None and end is not None
    if clip and end <= start:
        raise HTTPException(400, "end must be after start")
    if turbo and not shutil.which("aria2c"):
        raise HTTPException(
            400, "Turbo needs aria2c. Install it (macOS: brew install aria2) "
            "or rebuild the Docker image, then retry.")
    sb = (sb or "").strip().lower()
    if sb in ("0", "false", "none"):
        sb = ""
    sb_remove = sb in ("1", "true", "remove")   # buffered: cuts segments out
    sb_mark = sb == "mark"                       # streaming: chapters only
    if sb and not (sb_remove or sb_mark):
        raise HTTPException(400, "sb must be 1 (remove) or 'mark'")
    if sb and site != "youtube":
        raise HTTPException(400, "SponsorBlock is YouTube-only")

    bundle = None
    if site in BUNDLE_SITES:
        try:
            bundle = await _bundle_cached(url, site)
        except HTTPException:
            raise
        except Exception as e:
            raise _classify_error(e, site) from e
        vinfo = bundle["vinfo"] or {}
        # single-video posts extract as a 1-entry playlist — unwrap it so the
        # p:/mp3 format lookups below see the real format list
        entries = [e for e in (vinfo.get("entries") or []) if e]
        info = entries[0] if len(entries) == 1 else vinfo
        name = _social_name(site, vinfo)
    else:
        try:
            info = await _extract_cached(url)
        except Exception as e:
            raise _classify_error(e, site) from e
        name = _safe_name(info.get("title", "video"))
    if clip:
        name += f"_clip_{int(start)}-{int(end)}"
        dur = end - start

    if kind == "z:all":  # zip every photo of a carousel into one download
        photos = (bundle or {}).get("photos") or []
        if not photos:
            raise HTTPException(404, "no photos to zip")

        def _build_zip() -> bytes:
            import io, zipfile, urllib.request
            cap = int(os.environ.get("ZIP_MAX_PHOTOS", "60"))
            # Byte budget, not just a photo count. ZIP_MAX_PHOTOS bounds how
            # many images we fetch; it says nothing about their SIZE, and this
            # blob lives in RAM. MAX_CONCURRENCY of these in flight at once is
            # the actual exposure, so cap the bytes too and stop early rather
            # than let one carousel of full-resolution images decide how much
            # memory the process uses.
            # 64 MiB, not 256: this blob is held in RAM, and the shipped k8s
            # pod limit is 512Mi. A 256 MiB zip plus one in-flight image plus
            # the interpreter would OOMKill the pod rather than degrade.
            # Raise it deliberately alongside the memory limit, not by default.
            budget = int(os.environ.get("ZIP_MAX_BYTES", str(64 * 1024 * 1024)))
            opener = _egress_opener()
            buf = io.BytesIO()
            # STORED (no compression): images are already compressed, and stored
            # entries keep this cheap. No disk touched -> statelessness preserved.
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as z:
                got = 0
                used = 0
                truncated = False
                for i, p in enumerate(photos[:cap], 1):
                    try:
                        req = urllib.request.Request(
                            _validate_egress(p["url"]),
                            headers={"User-Agent": _OG_UA})
                        # read one byte past the remaining budget so an
                        # over-budget image is detected without buffering all
                        # of it, then dropped whole (a truncated JPEG in the
                        # zip is worse than an absent one)
                        data = opener.open(req, timeout=30).read(budget - used + 1)
                    except HTTPException as e:
                        log.error("zip photo %d rejected: %s", i, e.detail)
                        continue
                    except Exception as e:
                        log.error("zip fetch failed photo %d: %s", i, str(e)[:120])
                        continue
                    if used + len(data) > budget:
                        log.warning("zip byte budget hit at photo %d/%d (%d bytes)",
                                    i, len(photos), budget)
                        truncated = True
                        break
                    z.writestr(f"{name}_photo{i}.{p['ext']}", data)
                    used += len(data)
                    got += 1
                if truncated:
                    z.writestr(
                        "TRUNCATED.txt",
                        f"Stopped after {got} of {len(photos)} photos: the "
                        f"{budget}-byte limit was reached. Raise ZIP_MAX_BYTES "
                        f"or download the remaining photos individually.\n")
            if not got:
                raise HTTPException(502, "could not fetch any photo for the zip")
            return buf.getvalue()

        async with sem:
            blob = await asyncio.to_thread(_build_zip)
        from fastapi.responses import Response
        return Response(blob, media_type="application/zip", headers={
            "Content-Disposition": f'attachment; filename="{name}.zip"',
            "Cache-Control": "no-store"})

    if kind.startswith("i:"):  # X/IG photo: range-proxy the CDN URL, no subprocess
        photos = (bundle or {}).get("photos") or []
        try:
            p = photos[int(kind[2:])]
        except (ValueError, IndexError):
            raise HTTPException(404, "photo not available")
        mt = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
              "webp": "image/webp", "gif": "image/gif"}.get(p["ext"], "application/octet-stream")
        n = int(kind[2:]) + 1
        return _range_proxy(p["url"], request, f"{name}_photo{n}.{p['ext']}", mt)

    if kind.startswith("g:"):  # og-scraped direct video URL (public post)
        vids = (bundle or {}).get("og_videos") or []
        try:
            v = vids[int(kind[2:])]
        except (ValueError, IndexError):
            raise HTTPException(404, "video not available")
        n = int(kind[2:]) + 1
        return _range_proxy(v["url"], request, f"{name}_video{n}.{v.get('ext', 'mp4')}",
                            "video/mp4")

    if kind.startswith("v:"):  # one video of a multi-video post / carousel
        vinfo = (bundle or {}).get("vinfo") or {}
        entries = [e for e in (vinfo.get("entries") or []) if e]
        try:
            entry = entries[int(kind[2:])]
        except (ValueError, IndexError):
            raise HTTPException(404, "video not available")
        best = _best_prog_mp4(entry)
        n = int(kind[2:]) + 1
        if best:
            return _range_proxy(best["url"], request, f"{name}_video{n}.mp4",
                                "video/mp4", extra_headers=best.get("http_headers"))
        # HLS-only entry: let yt-dlp remux it (rare)
        cmd = ["yt-dlp", *_common_flags(site), "--playlist-items", str(n),
               "-f", "best", "-o", "-", url]
        return await _stream_proc(cmd, "video/mp4", f"{name}_video{n}.mp4")

    # buffered path (can't stream): turbo (aria2c) and/or SponsorBlock.
    # Neither combines with clipping — clipping is an ffmpeg stream-copy of a
    # time range, the buffered path hands the whole pipeline to yt-dlp. This
    # used to fall through silently to the clip path with turbo/sb quietly
    # dropped; say so instead of pretending the option was honoured.
    if (sb_remove or turbo) and clip:
        opts = " and ".join(m for m, on in
                            (("Turbo", turbo), ("SponsorBlock", sb_remove)) if on)
        raise HTTPException(
            400, f"{opts} cannot be combined with clipping — "
                 "download the clip without it, or the full video with it.")
    if sb_remove or turbo:
        audio_only = kind == "mp3" or kind.startswith("a:")
        return await _file_download(url, _sb_selector(kind, info, codec), name,
                                    audio_only, sb_remove, bool(turbo), site=site,
                                    est_bytes=_estimate_bytes(info, kind))

    # sb=mark: fold SponsorBlock segments in as CHAPTERS. Streaming path, no
    # scratch disk, no re-encode — the merge already emits a chapter block.
    chapters = info.get("chapters")
    if sb_mark:
        vid = info.get("id") or ""
        segs = await asyncio.to_thread(_sb_segments, vid) if vid else []
        chapters = _merge_sb_chapters(chapters, segs)
        log.info("sponsorblock mark: %d segment(s) for %s", len(segs), vid)

    if kind == "mp3":  # transcode best audio -> streamable MP3
        audio = [f for f in (info.get("formats") or [])
                 if f.get("vcodec") == "none" and f.get("acodec") != "none"]
        if audio:
            best_a = max(audio, key=lambda f: (f.get("abr") or f.get("tbr") or 0))
            src = best_a["url"]
        elif site != "youtube" and _best_prog_mp4(info):
            # social posts rarely expose audio-only; strip from the best MP4
            src = _best_prog_mp4(info)["url"]
        else:
            raise HTTPException(404, "no audio format")
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
        if PROXY_URL:
            cmd += ["-http_proxy", PROXY_URL]
        ss = ["-ss", f"{start:.3f}"] if clip else []
        t = ["-t", f"{dur:.3f}"] if clip else []
        cmd += [*ss, "-i", src, *t, "-vn", "-c:a", "libmp3lame", "-q:a", "2",
                "-f", "mp3", "pipe:1"]
        return await _stream_proc(cmd, "audio/mpeg", f"{name}.mp3")

    if kind.startswith("p:"):  # progressive single file
        fmt_id = kind[2:]
        f = next((x for x in info.get("formats") or [] if x.get("format_id") == fmt_id), None)
        if not f:
            raise HTTPException(404, "format not available")
        if clip:
            cmd = _clip_cmd([f["url"]], start, dur, ["-c:a", "copy"], ["-map", "0"])
            return await _stream_proc(cmd, "video/mp4", f"{name}.mp4")
        # resumable: proxy the direct URL with Range support (browser-native)
        return _range_proxy(f["url"], request, f"{name}.mp4", "video/mp4",
                            extra_headers=f.get("http_headers"))

    if kind.startswith(("c:", "m:")) and site not in MERGE_SITES:
        raise HTTPException(400, "merge kinds not available for this site")

    if kind.startswith("c:"):  # compat: H.264 + AAC pure copy, QuickTime-safe
        height = int(kind[2:])
        fmts = info.get("formats") or []
        try:
            v = max(
                (f for f in fmts if f.get("height") == height and f.get("acodec") == "none"
                 and str(f.get("vcodec", "")).startswith("avc1")),
                key=lambda f: (f.get("tbr") or 0),
            )
            a = max(
                (f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"),
                key=lambda f: (str(f.get("acodec", "")).startswith("mp4a"),
                               f.get("abr") or f.get("tbr") or 0),
            )
        except ValueError:
            raise HTTPException(404, "compat format not available")
        audio_copy = str(a.get("acodec", "")).startswith("mp4a")
        if clip:
            ac = ["-c:a", "copy"] if audio_copy else ["-c:a", "aac", "-b:a", "160k"]
            cmd = _clip_cmd([v["url"], a["url"]], start, dur, ac,
                            ["-map", "0:v:0", "-map", "1:a:0"])
            return await _stream_proc(cmd, "video/mp4", f"{name}.mp4")
        return _stream_merge(url, v["format_id"], a["format_id"],
                             f"{name}.mp4", audio_copy=audio_copy,
                             chapters=chapters, site=site)

    if kind.startswith("m:"):  # DASH merge -> always MP4
        height = int(kind[2:])
        fmts = info.get("formats") or []
        # codec=h264 (QuickTime mode): prefer avc1 at this height so the file
        # plays in QuickTime/Apple players. VP9/AV1 don't play there. Falls
        # back to best-quality if no H.264 exists at this height.
        if codec == "h264":
            avc = [f for f in fmts if f.get("height") == height and f.get("acodec") == "none"
                   and str(f.get("vcodec", "")).startswith("avc1")]
            if avc:
                v = max(avc, key=lambda f: (f.get("tbr") or 0))
                a = max((f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"),
                        key=lambda f: (str(f.get("acodec", "")).startswith("mp4a"),
                                       f.get("abr") or f.get("tbr") or 0))
                ac = str(a.get("acodec", "")).startswith("mp4a")
                if clip:
                    acx = ["-c:a", "copy"] if ac else ["-c:a", "aac", "-b:a", "160k"]
                    cmd = _clip_cmd([v["url"], a["url"]], start, dur, acx,
                                    ["-map", "0:v:0", "-map", "1:a:0"])
                    return await _stream_proc(cmd, "video/mp4", f"{name}.mp4")
                return _stream_merge(url, v["format_id"], a["format_id"],
                                     f"{name}.mp4", audio_copy=ac, chapters=chapters, site=site)
        # QUALITY over codec: pick highest-bitrate video at this height no
        # matter the codec. YouTube's H.264 1080p is bitrate-starved and looks
        # visibly worse than VP9/AV1 at the same label — preferring avc1 for
        # copy-compat produced "1080p that doesn't feel like 1080p".
        try:
            v = max(
                (f for f in fmts if f.get("height") == height and f.get("acodec") == "none"),
                key=lambda f: (f.get("tbr") or 0, f.get("filesize") or 0),
            )
            a = max(
                (f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") != "none"),
                key=lambda f: (f.get("abr") or f.get("tbr") or 0),
            )
        except ValueError:
            raise HTTPException(404, "format not available")
        # audio: copy when already AAC, else transcode to AAC (cheap)
        audio_copy = str(a.get("acodec", "")).startswith("mp4a")
        if clip:
            ac = ["-c:a", "copy"] if audio_copy else ["-c:a", "aac", "-b:a", "160k"]
            cmd = _clip_cmd([v["url"], a["url"]], start, dur, ac,
                            ["-map", "0:v:0", "-map", "1:a:0"])
            return await _stream_proc(cmd, "video/mp4", f"{name}.mp4")
        return _stream_merge(url, v["format_id"], a["format_id"],
                             f"{name}.mp4", audio_copy=audio_copy,
                             chapters=chapters, site=site)

    if kind.startswith("a:"):
        # pick the actual best audio so the file extension matches its codec
        audio = [f for f in (info.get("formats") or [])
                 if f.get("vcodec") == "none" and f.get("acodec") != "none"]
        if not audio:
            raise HTTPException(404, "no audio format")
        want = kind[2:]
        if want and want != "audio":
            # explicit codec pick (a:<format_id>) — a second passthrough option
            # so Apple users can take AAC directly instead of re-encoding to MP3
            best_a = next((f for f in audio if f.get("format_id") == want), None)
            if not best_a:
                raise HTTPException(404, "audio format not available")
        else:
            best_a = max(audio, key=lambda f: (f.get("abr") or f.get("tbr") or 0))
        if clip:
            ac = (["-c:a", "copy"] if str(best_a.get("acodec", "")).startswith("mp4a")
                  else ["-c:a", "aac", "-b:a", "160k"])
            cmd = _clip_cmd([best_a["url"]], start, dur, ac, ["-map", "0:a:0"])
            return await _stream_proc(cmd, "application/octet-stream", f"{name}.m4a")
        ext = best_a.get("ext") or "m4a"
        # resumable: proxy the direct audio URL with Range support
        return _range_proxy(best_a["url"], request, f"{name}.{ext}",
                            "application/octet-stream",
                            extra_headers=best_a.get("http_headers"))

    raise HTTPException(400, "bad kind")


# serve frontend at / — explicit route with no-cache so browsers ALWAYS
# revalidate the page. Without this, heuristic caching served week-old UI
# and made updates invisible (real support incident).
if os.path.isdir(STATIC_DIR):

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(
            os.path.join(STATIC_DIR, "index.html"),
            headers={"Cache-Control": "no-cache"},
        )

    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
