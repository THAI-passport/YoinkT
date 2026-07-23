"""Offline unit tests for the v30 X changes — stubs fastapi (no PyPI in sandbox)."""
import sys, types, os, json

# ---- fastapi stubs ----
fa = types.ModuleType("fastapi")
class HTTPException(Exception):
    def __init__(self, status_code, detail=""):
        self.status_code, self.detail = status_code, detail
        super().__init__(detail)
class FastAPI:
    def __init__(self, **k): pass
    def get(self, *a, **k):
        def deco(f): return f
        return deco
    post = delete = get
    def middleware(self, *a, **k):
        def deco(f): return f
        return deco
    def mount(self, *a, **k): pass
def Query(default=..., **k): return default
class Request: pass
fa.FastAPI, fa.HTTPException, fa.Query, fa.Request = FastAPI, HTTPException, Query, Request
resp = types.ModuleType("fastapi.responses")
class _R:
    def __init__(self, *a, **k): pass
resp.FileResponse = resp.StreamingResponse = resp.Response = _R
static = types.ModuleType("fastapi.staticfiles")
static.StaticFiles = _R
sys.modules["fastapi"] = fa
sys.modules["fastapi.responses"] = resp
sys.modules["fastapi.staticfiles"] = static

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend"))
import app

ok = fail = 0
def check(name, cond):
    global ok, fail
    print(("PASS " if cond else "FAIL ") + name)
    ok, fail = ok + cond, fail + (not cond)

# ---- site registry / SSRF guard ----
check("yt url -> youtube", app._site_of("https://www.youtube.com/watch?v=x") == "youtube")
check("youtu.be -> youtube", app._site_of("https://youtu.be/abc") == "youtube")
check("x.com status -> x", app._site_of("https://x.com/user/status/123456") == "x")
check("twitter.com status -> x", app._site_of("https://twitter.com/user/status/9") == "x")
check("mobile.x.com -> x", app._site_of("https://mobile.x.com/u_1/status/55") == "x")
check("x profile REJECTED", app._site_of("https://x.com/someuser") is None)
check("arbitrary url REJECTED", app._site_of("https://evil.example.com/x.com/a/status/1") is None)
check("internal ip REJECTED", app._site_of("http://169.254.169.254/latest") is None)
try:
    app._require_youtube("https://x.com/u/status/1"); check("playlist guard yt-only", False)
except HTTPException as e:
    check("playlist guard yt-only", e.status_code == 400)

# ---- X format curation (synthetic yt-dlp tweet info) ----
xinfo = {"id": "123", "title": "user - some tweet", "uploader": "user",
         "duration": 30, "thumbnail": "https://pbs.twimg.com/t.jpg",
         "formats": [
             {"format_id": "hls-audio-64", "vcodec": "none", "acodec": "mp4a", "protocol": "m3u8_native"},
             {"format_id": "hls-720", "vcodec": "avc1", "acodec": "mp4a", "height": 720, "protocol": "m3u8_native"},
             {"format_id": "http-432", "vcodec": "avc1", "acodec": "mp4a", "height": 480, "tbr": 432, "protocol": "https", "filesize": 111, "url": "https://video.twimg.com/a.mp4"},
             {"format_id": "http-2176", "vcodec": "avc1", "acodec": "mp4a", "height": 720, "tbr": 2176, "protocol": "https", "filesize": 999, "url": "https://video.twimg.com/b.mp4"},
         ]}
fmts = app._curate_prog_formats(xinfo)
kinds = [f["kind"] for f in fmts]
check("curated: one per height + mp3", kinds == ["p:http-2176", "p:http-432", "mp3"])
check("curated: resumable + mp4", all(f.get("resumable") for f in fmts[:2]) and fmts[0]["ext"] == "mp4")
check("curated: no hls leaked", not any("hls" in k for k in kinds))
check("best mp4 picks highest tbr", app._best_prog_mp4(xinfo)["format_id"] == "http-2176")

# ---- _x_api_info shapes ----
b1 = {"vinfo": xinfo, "photos": []}
r1 = app._social_api_info("u", b1)
check("single video: formats set, no media", r1["site"] == "x" and len(r1["formats"]) == 3 and r1["media"] == [])

b2 = {"vinfo": None, "photos": [{"url": "https://pbs.twimg.com/p1.jpg", "ext": "jpg"},
                                {"url": "https://pbs.twimg.com/p2.png", "ext": "png"}]}
r2 = app._social_api_info("u", b2)
check("photo-only: 2 media, i: kinds", [m["kind"] for m in r2["media"]] == ["i:0", "i:1"])
check("photo-only: thumb fallback", r2["thumbnail"] == "https://pbs.twimg.com/p1.jpg")
check("photo-only: formats empty", r2["formats"] == [])

multi = {"id": "9", "title": "t", "uploader": "u", "_type": "playlist",
         "entries": [dict(xinfo), dict(xinfo)]}
r3 = app._social_api_info("u", {"vinfo": multi, "photos": [{"url": "https://p/3.jpg", "ext": "jpg"}]})
check("multi-video: v: kinds + photo", [m["kind"] for m in r3["media"]] == ["v:0", "v:1", "i:0"])
check("multi-video: no flat formats", r3["formats"] == [])

one_entry = {"id": "9", "title": "t", "uploader": "u", "_type": "playlist", "entries": [dict(xinfo)]}
r4 = app._social_api_info("u", {"vinfo": one_entry, "photos": []})
check("1-entry playlist unwrapped to formats", len(r4["formats"]) == 3 and r4["media"] == [])

# ---- gallery-dl -j parsing (fake binary) ----
os.makedirs("bin", exist_ok=True)
msgs = [[1, {"category": "twitter"}], [2, {"dir": "x"}],
        [3, "https://pbs.twimg.com/media/AAA?format=jpg&name=orig", {"extension": "jpg"}],
        [3, "https://pbs.twimg.com/media/BBB?format=png&name=orig", {"extension": "png"}],
        [3, "https://video.twimg.com/vid.mp4", {"extension": "mp4"}]]  # must be filtered
with open("bin/gallery-dl", "w") as f:
    f.write("#!/bin/sh\ncat <<'EOF'\n" + json.dumps(msgs) + "\nEOF\n")
os.chmod("bin/gallery-dl", 0o755)
os.environ["PATH"] = os.path.abspath("bin") + ":" + os.environ["PATH"]
app._gdl_cmd = lambda: ["gallery-dl"]
photos = app._gdl_photos("https://x.com/u/status/1")
check("gdl: 2 photos, mp4 filtered", [p["ext"] for p in photos] == ["jpg", "png"])

# ---- misc invariants ----
n = app._safe_name("tweet — ünïcode 🎉")
check("safe_name ascii", n and n.encode("ascii", "strict") is not None)
check("version bumped + grep-safe", "always-mp4" in app.APP_VERSION and app.APP_VERSION != "YoinkT v29-always-mp4")
check("cookies_for fallback", app._cookies_for("x") == app.COOKIES_FILE)


# ---- v31: Facebook / Instagram ----
check("fb watch -> facebook", app._site_of("https://www.facebook.com/watch?v=123") == "facebook")
check("fb reel -> facebook", app._site_of("https://www.facebook.com/reel/1234567") == "facebook")
check("fb share/v -> facebook", app._site_of("https://www.facebook.com/share/v/AbC-123/") == "facebook")
check("fb.watch -> facebook", app._site_of("https://fb.watch/abc-XYZ") == "facebook")
check("fb page videos -> facebook", app._site_of("https://www.facebook.com/somepage/videos/99887766") == "facebook")
check("fb profile REJECTED", app._site_of("https://www.facebook.com/someuser") is None)
check("ig post -> instagram", app._site_of("https://www.instagram.com/p/AbC12_-x/") == "instagram")
check("ig reel -> instagram", app._site_of("https://www.instagram.com/reel/XyZ987/") == "instagram")
check("ig user-scoped post -> instagram", app._site_of("https://www.instagram.com/someuser/p/AbC123/") == "instagram")
check("ig profile REJECTED", app._site_of("https://www.instagram.com/someuser/") is None)
check("bundle sites v31+", {"x", "instagram"} <= app.BUNDLE_SITES)
check("merge sites", app.MERGE_SITES == {"youtube", "facebook"})

# FB progressive-only info (no DASH): _curate_formats must fall back to any-height progressive
fb_info = {"formats": [
    {"format_id": "sd", "vcodec": "avc1", "acodec": "mp4a", "height": 360, "tbr": 500, "ext": "mp4", "url": "https://video.fbcdn.net/sd.mp4", "protocol": "https"},
    {"format_id": "hd", "vcodec": "avc1", "acodec": "mp4a", "height": 720, "tbr": 1500, "ext": "mp4", "url": "https://video.fbcdn.net/hd.mp4", "protocol": "https"},
]}
fb_fmts = app._curate_formats(fb_info)
fb_kinds = [f["kind"] for f in fb_fmts]
check("fb prog fallback: both heights", fb_kinds[:2] == ["p:hd", "p:sd"])
check("fb prog fallback: resumable", all(f.get("resumable") for f in fb_fmts[:2]))

# YouTube curation UNCHANGED by the fallback (DASH covers all prog heights)
yt_info = {"formats": [
    {"format_id": "22", "vcodec": "avc1", "acodec": "mp4a", "height": 720, "tbr": 1000, "ext": "mp4", "protocol": "https"},
    {"format_id": "137", "vcodec": "avc1.64", "acodec": "none", "height": 1080, "tbr": 4000, "filesize": 100, "protocol": "https"},
    {"format_id": "248", "vcodec": "vp9", "acodec": "none", "height": 1080, "tbr": 5000, "filesize": 120, "protocol": "https"},
    {"format_id": "398", "vcodec": "avc1.4d", "acodec": "none", "height": 720, "tbr": 2000, "filesize": 60, "protocol": "https"},
    {"format_id": "140", "vcodec": "none", "acodec": "mp4a", "abr": 128, "ext": "m4a", "protocol": "https"},
]}
yt_kinds = [f["kind"] for f in app._curate_formats(yt_info)]
check("yt curation intact", yt_kinds == ["m:1080", "p:22", "c:1080", "a:audio", "mp3"])

# IG bundle info shape
r5 = app._social_api_info("u", {"vinfo": None, "photos": [{"url": "https://scontent.cdninstagram.com/a.jpg", "ext": "jpg"}]}, "instagram")
check("ig site field + title", r5["site"] == "instagram" and r5["title"] == "Instagram post")

# gdl per-site option
import subprocess as _sp
_orig = _sp.run
seen = {}
def _spy(cmd, **kw):
    seen["cmd"] = cmd
    class R: returncode, stdout, stderr = 0, b"[]", b""
    return R()
_sp.run = _spy
app._gdl_photos("https://www.instagram.com/p/x/", "instagram")
_sp.run = _orig
check("gdl ig videos=false", "extractor.instagram.videos=false" in seen["cmd"])

check("version grep-safe", "always-mp4" in app.APP_VERSION)

# ---- v32: FB photos, TikTok, merge-aware bundle curation ----
check("fb photo.php -> facebook", app._site_of("https://www.facebook.com/photo.php?fbid=123") == "facebook")
check("fb /photo -> facebook", app._site_of("https://www.facebook.com/photo/?fbid=1&set=a.2") == "facebook")
check("fb page posts -> facebook", app._site_of("https://www.facebook.com/somepage/posts/pfbid0abc") == "facebook")
check("fb page photos -> facebook", app._site_of("https://www.facebook.com/somepage/photos/a.1/2/") == "facebook")
check("fb permalink -> facebook", app._site_of("https://www.facebook.com/permalink.php?story_fbid=1&id=2") == "facebook")
check("fb share/p -> facebook", app._site_of("https://www.facebook.com/share/p/AbC123/") == "facebook")
check("facebook in BUNDLE + MERGE", "facebook" in app.BUNDLE_SITES and "facebook" in app.MERGE_SITES)

check("tiktok video -> tiktok", app._site_of("https://www.tiktok.com/@user.name/video/7300000000000000000") == "tiktok")
check("tiktok photo -> tiktok", app._site_of("https://www.tiktok.com/@user/photo/7300000000000000001") == "tiktok")
check("tiktok vm short -> tiktok", app._site_of("https://vm.tiktok.com/ZMabc123/") == "tiktok")
check("tiktok /t/ -> tiktok", app._site_of("https://www.tiktok.com/t/ZTabc/") == "tiktok")
check("tiktok profile REJECTED", app._site_of("https://www.tiktok.com/@user") is None)
check("tiktok in BUNDLE, not MERGE", "tiktok" in app.BUNDLE_SITES and "tiktok" not in app.MERGE_SITES)
check("gdl facebook videos=false", "extractor.facebook.videos=false" == app._GDL_NOVIDEO["facebook"])
check("tiktok cookies env wired", "tiktok" in app.SITE_COOKIES)

# merge-aware bundle curation: FB single video -> full curation (m:/c: kinds);
# TikTok single video -> progressive only
fb_dash = {"formats": [
    {"format_id": "v1080", "vcodec": "avc1", "acodec": "none", "height": 1080, "tbr": 4000, "filesize": 100, "protocol": "https"},
    {"format_id": "a", "vcodec": "none", "acodec": "mp4a", "abr": 128, "protocol": "https"},
]}
fb_curated = app._curate_for("facebook", fb_dash)
check("fb merge-aware: m: kind emitted", any(k["kind"].startswith("m:") for k in fb_curated))
tk = {"formats": [
    {"format_id": "play", "vcodec": "h264", "acodec": "aac", "height": 1024, "tbr": 2000, "ext": "mp4", "url": "https://v.tiktokcdn.com/x.mp4", "protocol": "https"},
]}
tk_curated = app._curate_for("tiktok", tk)
check("tiktok prog-only", [f["kind"] for f in tk_curated] == ["play"[:0]+"p:play", "mp3"])

# social info uses merge-aware curation for FB single video
r_fb = app._social_api_info("u", {"vinfo": dict(fb_dash, title="fbvid"), "photos": []}, "facebook")
check("fb social_api uses full curation", any(k["kind"].startswith("m:") for k in r_fb["formats"]))
check("fb site field", r_fb["site"] == "facebook")

# tiktok photo-only bundle
r_tk = app._social_api_info("u", {"vinfo": None, "photos": [{"url": "https://p16.tiktokcdn.com/a.jpg", "ext": "jpg"}]}, "tiktok")
check("tiktok photo media", [m["kind"] for m in r_tk["media"]] == ["i:0"] and r_tk["title"] == "TikTok post")


# ---- v33: og:meta / IG-embed anonymous scraper ----
check("ext_of jpg from url", app._ext_of("https://x/a.jpg?x=1", "png") == "jpg")
check("ext_of jpeg->jpg", app._ext_of("https://x/a.JPEG", "png") == "jpg")
check("ext_of mp4", app._ext_of("https://x/v.mp4", "jpg") == "mp4")
check("ext_of default", app._ext_of("https://x/noext", "jpg") == "jpg")
check("unescape json url", app._unescape_url("https://x/a?b\\u0026c\\/d") == "https://x/a?b&c/d")
check("unescape html amp", app._unescape_url("https://x/a?b&amp;c") == "https://x/a?b&c")

# fake _og_fetch_html so no network
ig_photo_html = '''<html><head>
<meta property="og:image" content="https://scontent.cdninstagram.com/pic.jpg?a=1&amp;b=2"/>
</head><body>
<img class="EmbeddedMediaImage" src="https://scontent.cdninstagram.com/hires.jpg?x=9"/>
<script>{"display_url":"https://scontent.cdninstagram.com/display.jpg?token=z\\u0026s=1"}</script>
</body></html>'''
ig_video_html = '''<meta property="og:video" content="https://video.cdninstagram.com/reel.mp4?e=1"/>
<meta property="og:image" content="https://scontent.cdninstagram.com/poster.jpg"/>'''
fb_photo_html = '<meta property="og:image" content="https://scontent.fbcdn.net/photo.jpg?oh=1"/>'

app._og_fetch_html = lambda url, site: {"ig_p": ig_photo_html, "ig_v": ig_video_html, "fb_p": fb_photo_html}[app._TESTKEY]

app._TESTKEY = "ig_p"
r = app._og_scrape("https://www.instagram.com/p/ABC123/", "instagram")
check("og ig photo: images found", len(r["photos"]) >= 2 and r["videos"] == [])
check("og ig photo: html entity decoded", any("a=1&b=2" in p["url"] for p in r["photos"]))
check("og ig photo: display_url decoded", any("token=z&s=1" in p["url"] for p in r["photos"]))

app._TESTKEY = "ig_v"
r = app._og_scrape("https://www.instagram.com/reel/XYZ/", "instagram")
check("og ig video: video found", len(r["videos"]) == 1 and r["videos"][0]["ext"] == "mp4")

app._TESTKEY = "fb_p"
r = app._og_scrape("https://www.facebook.com/share/p/abc/", "facebook")
check("og fb photo: image found", len(r["photos"]) == 1 and r["photos"][0]["ext"] == "jpg")

# IG embed URL rewrite
seen_url = {}
app._og_fetch_html = lambda url, site: (seen_url.__setitem__("u", url), "")[1]
app._og_scrape("https://www.instagram.com/p/SHORT1/", "instagram")
check("og ig uses embed endpoint", "/p/SHORT1/embed/captioned/" in seen_url["u"])

# _social_api_info surfaces og_videos as g: kinds
r_og = app._social_api_info("u", {"vinfo": None, "photos": [], "og_videos": [{"url": "https://v/x.mp4", "ext": "mp4"}]}, "facebook")
check("og_videos -> g: kind", [m["kind"] for m in r_og["media"]] == ["g:0"])

check("version grep-safe", "always-mp4" in app.APP_VERSION)

# ---- v34: age-gate error detection + retry clients ----
class _AgeErr(Exception): pass
check("age err: confirm your age", app._is_age_error(_AgeErr("Sign in to confirm your age. This video may be inappropriate")))
check("age err: inappropriate", app._is_age_error(_AgeErr("inappropriate for some users")))
check("age err: not a normal error", not app._is_age_error(_AgeErr("HTTP Error 404: Not Found")))
check("age clients include mweb", "mweb" in app._YT_AGE_CLIENTS and app._YT_AGE_CLIENTS.startswith("default"))
# youtube CLI merge (no cookies) injects the bypass clients; other sites don't
yt_flags = app._common_flags("youtube")
check("yt cli merge NO forced player_client (v38 speed fix)", not any("player_client=" in f for f in yt_flags))
x_flags = app._common_flags("x")
check("x cli merge no player_client", not any("player_client=" in f for f in x_flags))
check("version bumped", app.APP_VERSION.startswith("YoinkT v"))

# ---- v35: social filenames, runtime cookies, guardrails config ----
check("social name v37 uploader_id", app._social_name("x", {"uploader": "jane", "id": "42"}).startswith("jane_42_"))
check("social name fallback has site+date", app._social_name("tiktok", {}).startswith("tiktok_"))

# runtime cookie override beats env
app.SITE_COOKIES_RUNTIME["x"] = "/tmp/x.txt"
check("runtime cookie wins", app._cookies_for("x") == "/tmp/x.txt")
app.SITE_COOKIES_RUNTIME.pop("x")
check("runtime cleared -> env/global", app._cookies_for("x") == (app.SITE_COOKIES.get("x") or app.COOKIES_FILE))

check("guardrail defaults off", app.API_KEY is None and app.RATE_LIMIT_RPM == 0)
check("cookie upload default on", app.ALLOW_COOKIE_UPLOAD is True)
check("cookies dir set", isinstance(app.COOKIES_DIR, str) and len(app.COOKIES_DIR) > 0)

# og runs for mixed post (video failed, photos present) -> og_videos surfaced
r_mix = app._social_api_info("u", {"vinfo": None, "photos": [{"url":"https://p/a.jpg","ext":"jpg"}], "og_videos": [{"url":"https://v/x.mp4","ext":"mp4"}]}, "facebook")
check("mixed: g: + i: both present", [m["kind"] for m in r_mix["media"]] == ["g:0", "i:0"])

check("version current", app.APP_VERSION.startswith("YoinkT v") and "always-mp4" in app.APP_VERSION)

# ---- v37: gallery-dl module detection + filename scheme ----
import time as _t
check("gdl cmd is module form", app._gdl_cmd()[:2] == [app.sys.executable, "-m"] or app._gdl_cmd() == ["gallery-dl"])
check("gdl available bool", isinstance(app._gdl_available(), bool))

nm = app._social_name("instagram", {"uploader": "jane doe", "id": "ABC123"})
check("social name uploader_id_date", nm.startswith("jane_doe_ABC123_") and _t.strftime("%Y-%m-%d") in nm)
check("social name no spaces", " " not in nm)
nm2 = app._social_name("tiktok", {})
check("social name fallback", nm2.startswith("tiktok_") and _t.strftime("%Y-%m-%d") in nm2)
check("social name id-only ok", app._social_name("x", {"id": "999"}).startswith("x_999_"))

check("version current", app.APP_VERSION.startswith("YoinkT v") and "always-mp4" in app.APP_VERSION)

# ---- v39: config isolation, stealth profiles, engine provenance ----

# config isolation must be present on EVERY subprocess path, and first —
# a later flag can't undo an ambient config that was already merged.
for _site in ("youtube", "x", "instagram"):
    bf = app._base_flags(_site)
    check(f"config isolated ({_site})",
          bf[0] == "--ignore-config" and "--no-config-locations" in bf)
    check(f"config isolation survives _common_flags ({_site})",
          "--ignore-config" in app._common_flags(_site))

# stealth profile policy: social sites armed, youtube deliberately not
check("stealth default youtube off", app._STEALTH_DEFAULTS["youtube"] is None)
check("stealth default social on",
      all(app._STEALTH_DEFAULTS[s] for s in ("x", "facebook", "instagram", "tiktok")))
check("stealth env off kills all",
      all(v is None for v in app._parse_stealth("off").values()))
check("stealth env override", app._parse_stealth("x=safari")["x"] == "safari")
check("stealth env override leaves others at default",
      app._parse_stealth("x=safari")["instagram"] == app._STEALTH_DEFAULTS["instagram"])
check("stealth env ignores unknown site", "bogus" not in app._parse_stealth("bogus=chrome"))

# transport is optional: with it absent, no flag is emitted anywhere
check("stealth availability is bool", isinstance(app._stealth_available(), bool))
if not app._stealth_available():
    check("no stealth flag when transport missing",
          all("--impersonate" not in app._base_flags(s) for s in app._STEALTH_DEFAULTS))
    check("no impersonate opt when transport missing",
          "impersonate" not in app._ydl_opts("x"))
    check("_stealth_for returns None when transport missing",
          app._stealth_for("x") is None)

# block detection drives the one-shot stealth retry — narrow on purpose
check("block err: bot wall", app._is_block_error(Exception("Sign in to confirm you're not a bot")))
check("block err: rate limit", app._is_block_error(Exception("rate-limit reached")))
check("block err: checkpoint", app._is_block_error(Exception("Checkpoint required")))
check("block err: private post is NOT a block",
      not app._is_block_error(Exception("This post is private; login required")))
check("block err: plain 404 is NOT a block",
      not app._is_block_error(Exception("HTTP Error 404: Not Found")))

# engine provenance never raises, even with the engine absent (sandbox case)
_eng = app._engine_info()
check("engine info is dict", isinstance(_eng, dict))
check("engine info has keys",
      {"version", "age_days", "stale", "channel"} <= set(_eng))
check("engine stale threshold sane", app.ENGINE_STALE_DAYS > 0)

# photo-resolver config flag is probed, not guessed
check("gdl config flag is list", isinstance(app._gdl_config_flag(), list))
check("gdl config flag empty or known",
      app._gdl_config_flag() in ([], ["--ignore-config"], ["--config-ignore"]))

check("version v44", "v44" in app.APP_VERSION and "always-mp4" in app.APP_VERSION)


# ---- v39: engine pin must not drift across the four places it appears ----
# ./ENGINE_VERSION is the source of truth. Dockerfile ARG default and the CI
# env are fallbacks for when run-local.sh isn't the entry point, so they have
# to agree or Docker and native silently run different engines — exactly the
# floating variable that makes a speed regression impossible to attribute.
import re as _re, pathlib as _pl
_root = _pl.Path(__file__).resolve().parent.parent
_pin = (_root / "ENGINE_VERSION").read_text().strip()
check("ENGINE_VERSION file looks like a build", bool(_re.fullmatch(r"\d{4}\.\d{1,2}\.\d{1,2}(\.\d+)?", _pin)))

_df = _re.search(r"ARG ENGINE_VERSION=(\S*)", (_root / "Dockerfile").read_text())
check("Dockerfile pin matches ENGINE_VERSION", _df and _df.group(1).strip('"') == _pin)

_ci = _re.search(r'ENGINE_VERSION:\s*"([^"]*)"', (_root / ".github/workflows/build.yml").read_text())
check("CI pin matches ENGINE_VERSION", _ci and _ci.group(1) == _pin)

_compose = (_root / "docker-compose.yml").read_text()
_cp = _re.search(r"ENGINE_VERSION:\s*\"\$\{ENGINE_VERSION:-([^}]*)\}\"", _compose)
check("compose fallback matches ENGINE_VERSION", _cp and _cp.group(1) == _pin)

_rl = (_root / "run-local.sh").read_text()
check("run-local reads the pin file", "< ENGINE_VERSION" in _rl)
_rl_code = "\n".join(l for l in _rl.splitlines() if not l.strip().startswith("#"))
check("run-local no longer blind-upgrades the engine",
      "--upgrade yt-dlp" not in _rl_code.replace("--pre --upgrade yt-dlp", ""))


import pathlib
def _raises(fn):
    try:
        fn()
    except Exception:
        return True
    return False


# ---- v40: egress guard (SSRF), lock leak, zip byte budget ----
# NOTE: these run without network. Literal-IP URLs skip DNS entirely, and the
# bypass paths are pure logic. A "resolves fine" positive case is deliberately
# NOT asserted here — it would need DNS and would go red on an offline runner.

import urllib.request as _ur

# scheme allowlist — the file:// hole was the sharp one: urllib's default
# opener would have read and streamed a local file
for _bad, _label in [
    ("file:///etc/passwd", "file"),
    ("ftp://example.com/x", "ftp"),
    ("data:text/plain,hi", "data"),
    ("gopher://example.com/", "gopher"),
]:
    check(f"egress rejects {_label} scheme", app._egress_reject_reason(_bad) is not None)

# destination denylist — literal IPs, no resolution needed
for _bad, _label in [
    ("http://127.0.0.1:8000/api/health", "loopback v4"),
    ("http://[::1]/", "loopback v6"),
    ("http://169.254.169.254/latest/meta-data/", "cloud metadata"),
    ("http://10.1.2.3/", "RFC1918 10/8"),
    ("http://192.168.1.1/", "RFC1918 192.168/16"),
    ("http://172.16.0.1/", "RFC1918 172.16/12"),
    ("http://0.0.0.0/", "unspecified"),
]:
    _r = app._egress_reject_reason(_bad)
    check(f"egress rejects {_label}", _r is not None and "non-public" in _r)

check("egress rejects hostless URL", app._egress_reject_reason("http://") is not None)
check("egress raises HTTPException", _raises(lambda: app._validate_egress("http://127.0.0.1/")))

# documented bypasses
_saved_allow, _saved_proxy = app.ALLOW_PRIVATE_EGRESS, app.PROXY_URL
app.ALLOW_PRIVATE_EGRESS = True
check("ALLOW_PRIVATE_EGRESS lets loopback through",
      app._egress_reject_reason("http://127.0.0.1/") is None)
check("ALLOW_PRIVATE_EGRESS still blocks file://",
      app._egress_reject_reason("file:///etc/passwd") is not None)
app.ALLOW_PRIVATE_EGRESS = False
app.PROXY_URL = "http://proxy:3128"
check("proxy mode skips local IP checks (proxy resolves, not us)",
      app._egress_reject_reason("http://10.0.0.1/") is None)
check("proxy mode still blocks file://",
      app._egress_reject_reason("file:///etc/passwd") is not None)
app.ALLOW_PRIVATE_EGRESS, app.PROXY_URL = _saved_allow, _saved_proxy

# the opener itself must not be able to speak file:// or ftp:// at all
_op = app._egress_opener()
_handlers = [type(h).__name__ for h in _op.handlers]
check("opener has no FileHandler", "FileHandler" not in _handlers)
check("opener has no FTPHandler", not any("FTP" in h for h in _handlers))
check("opener has no DataHandler", "DataHandler" not in _handlers)
check("opener speaks http+https", "HTTPHandler" in _handlers and "HTTPSHandler" in _handlers)
check("opener revalidates redirects",
      any("GuardedRedirect" in h for h in _handlers))
check("opener redirect handler subclasses urllib's",
      any(isinstance(h, _ur.HTTPRedirectHandler) for h in _op.handlers))

# a redirect hop to a blocked destination must abort the chain
_gr = [h for h in _op.handlers if "GuardedRedirect" in type(h).__name__][0]
check("redirect to metadata IP is blocked mid-chain",
      _raises(lambda: _gr.redirect_request(None, None, 302, "Found", {},
                                           "http://169.254.169.254/")))

# ---- info-lock leak on failed extraction ----
import asyncio as _aio
app._INFO_CACHE.clear(); app._info_locks.clear()

def _boom(_u):
    raise RuntimeError("extract failed")

_saved_extract = app._extract
app._extract = _boom
try:
    for _i in range(25):
        try:
            _aio.run(app._extract_cached(f"https://youtube.com/watch?v=dead{_i}"))
        except Exception:
            pass
finally:
    app._extract = _saved_extract
check("failed extractions leave no orphan locks", len(app._info_locks) == 0)
check("failed extractions cache nothing", len(app._INFO_CACHE) == 0)
check("failed extractions leave no orphan refcounts", len(app._info_lock_users) == 0)
app._INFO_CACHE.clear(); app._info_locks.clear(); app._info_lock_users.clear()

# REGRESSION (v40 self-inflicted): the first cut of the leak fix reclaimed the
# lock when `not lock.locked()`. asyncio.Lock.release() clears _locked BEFORE
# waking the next waiter, so that is True while a waiter is still queued --
# the lock got popped mid-handoff and the NEXT caller setdefault()d a SECOND
# lock for the same URL, running a concurrent extraction. Assert one identity.
# Reproducing the race needs STAGGERED arrival, not just concurrency: callers
# that all arrive up front share one lock object regardless of the bug, because
# every setdefault happens before the erroneous pop. The failure only shows
# when a caller arrives AFTER the holder popped the lock while a waiter is
# still mid-extraction -- that caller creates a second lock for the same URL
# and extracts concurrently with the waiter.
class _SpyLocks(dict):
    def __init__(self):
        super().__init__()
        self.handed_out = []
    def setdefault(self, k, v):
        got = super().setdefault(k, v)
        self.handed_out.append((k, id(got)))
        return got

_url = "https://youtube.com/watch?v=raceme"

def _slow_boom(_u):
    import time as _tt
    _tt.sleep(0.05)
    raise RuntimeError("extract failed")

async def _race():
    async def one():
        try:
            await app._extract_cached(_url)
        except Exception:
            pass
    a = _aio.create_task(one())          # acquires, extracts, fails, releases
    b = _aio.create_task(one())          # queues as waiter behind a
    await _aio.sleep(0.08)               # a has failed+popped; b now extracting
    c = _aio.create_task(one())          # arrives in the window -> new lock?
    await _aio.gather(a, b, c)

_saved_extract, _saved_locks = app._extract, app._info_locks
_spy = _SpyLocks()
app._extract = _slow_boom
app._info_locks = _spy
try:
    _aio.run(_race())
finally:
    app._extract, app._info_locks = _saved_extract, _saved_locks

_ids = {i for k, i in _spy.handed_out if k == _url}
check("late caller never gets a second lock for an in-flight URL", len(_ids) == 1)
check("race run leaves no orphan locks", len(app._info_locks) == 0)
check("race run leaves no orphan refcounts", len(app._info_lock_users) == 0)

check("race run leaves no orphan refcounts", len(app._info_lock_users) == 0)
app._INFO_CACHE.clear(); app._info_locks.clear(); app._info_lock_users.clear()

# the lock-handoff window itself, asserted directly so the reasoning above
# doesn't silently rot if CPython changes Lock internals
async def _window():
    lk = _aio.Lock()
    await lk.acquire()
    t = _aio.create_task((lambda: _hold(lk))())
    await _aio.sleep(0)
    lk.release()
    observed = lk.locked()
    await t
    return observed

async def _hold(lk):
    async with lk:
        pass

check("locked() alone is NOT a safe 'unused' signal", _aio.run(_window()) is False)

# ---- zip byte budget ----
check("ZIP_MAX_BYTES default is finite",
      int(os.environ.get("ZIP_MAX_BYTES", str(256 * 1024 * 1024))) > 0)
_zsrc = pathlib.Path("backend/app.py").read_text()
check("zip enforces a byte budget", "ZIP_MAX_BYTES" in _zsrc and "budget - used + 1" in _zsrc)
check("zip drops over-budget image whole (no truncated file)",
      "truncated = True" in _zsrc and "TRUNCATED.txt" in _zsrc)
check("zip validates each photo URL", "_validate_egress(p[\"url\"])" in _zsrc)


# ---- v41: buffered path (turbo / SponsorBlock) parity with streaming ----

# BUG: c: and m: produced the SAME selector, so the buffered path silently
# dropped the H.264 constraint. `c:` exists only to guarantee a
# QuickTime-playable file; turbo used to hand back VP9/AV1 with no warning.
_c = app._sb_selector("c:1080", {}, "")
_m = app._sb_selector("m:1080", {}, "")
check("compat and merge selectors are no longer identical", _c != _m)
check("c: constrains EVERY branch to avc1",
      all("vcodec^=avc1" in b for b in _c.split("/")))
check("c: has no bare quality fallback (streaming path 404s there)",
      "best[height<=1080]" not in [b.strip() for b in _c.split("/")])
check("c: prefers AAC audio for a pure-copy mux", "acodec^=mp4a" in _c)

# codec=h264 (the QuickTime UI toggle) must reach the buffered path too
_mq = app._sb_selector("m:1080", {}, "h264")
check("codec=h264 makes m: prefer avc1", _mq.split("/")[0].count("vcodec^=avc1") == 1)
check("m:+h264 still falls back to best quality (matches streaming branch)",
      any("vcodec^=avc1" not in b for b in _mq.split("/")))
check("plain m: is quality-over-codec (no avc1 constraint)",
      "vcodec^=avc1" not in _m)

# exact height first, so buffered doesn't silently hand back a lower res
for _k, _c2 in [("c:1080", ""), ("m:1080", "h264"), ("m:720", "")]:
    check(f"{_k} tries exact height first",
          app._sb_selector(_k, {}, _c2).split("/")[0].count("height=") == 1)

check("p: passes the format id straight through", app._sb_selector("p:137", {}, "") == "137")
check("audio kinds unaffected", app._sb_selector("mp3", {}, "h264") == "bestaudio")

# codec must actually be threaded through the dispatch
import inspect as _insp
_sig = _insp.signature(app._sb_selector).parameters
check("_sb_selector accepts codec", "codec" in _sig)
_disp = _insp.getsource(app.api_download)
check("dispatch passes codec to the selector", "_sb_selector(kind, info, codec)" in _disp)

# buffered failures must be real HTTP errors, not an empty 200
_fd = _insp.getsource(app._file_download)
check("_file_download is async (prepares before responding)",
      _insp.iscoroutinefunction(app._file_download))
check("buffered failure raises instead of ending the generator",
      "raise HTTPException(502" in _fd)
check("prepare happens outside the response generator",
      _fd.index("create_subprocess_exec") < _fd.index("async def gen()"))
check("temp dir cleaned up when prepare fails",
      "except BaseException" in _fd and "rmtree" in _fd)

# turbo/sb + clip is now explicit, not silently dropped
check("clip + buffered rejected explicitly",
      "cannot be combined with clipping" in _disp)
_ui = pathlib.Path("backend/static/index.html").read_text()
check("UI stops sending turbo/sb when clipping",
      "const bufQS = clip ?" in _ui)


# ---- v42: turbo connection budget + scratch storage ----

# THE BUG: aria2c ran -x16 -j16 per download with nothing coordinating lanes,
# so 3 batch lanes = 48 connections to one CDN and 6 = 96. Since each
# connection is throttled ~2-4 MB/s, 16 already saturates a typical link on ONE
# video -- the extra connections bought no throughput and just multiplied
# block risk. The budget is now shared, so total stays ~constant.
_totals = []
for _n in range(1, 8):
    _c = app._turbo_conns(_n)
    _totals.append(_c * min(_n, app.TURBO_LANES))
check("total turbo connections stay bounded regardless of lanes",
      max(_totals) <= app.TURBO_CONN_BUDGET + app.TURBO_MIN_CONNS)
check("single turbo download still gets the full budget",
      app._turbo_conns(1) == app.TURBO_CONN_BUDGET)
check("connections shrink as lanes grow",
      app._turbo_conns(1) > app._turbo_conns(2) > app._turbo_conns(4) - 1)
check("never drops below the useful floor",
      all(app._turbo_conns(n) >= app.TURBO_MIN_CONNS for n in range(1, 50)))
check("lane cap keeps every lane above the floor",
      app.TURBO_CONN_BUDGET // max(1, app.TURBO_LANES) >= app.TURBO_MIN_CONNS)
check("turbo lanes are capped independently of MAX_CONCURRENCY",
      app.turbo_sem._value == app.TURBO_LANES)

# connection count must reach the aria2c args, not be hardcoded
import inspect as _i2
_fdsrc = _i2.getsource(app._file_download)
check("aria2c args are built from the budget, not hardcoded 16",
      "-x{conns}" in _fdsrc and "-x16" not in _fdsrc)
check("turbo slot acquired before the main slot (consistent lock order)",
      _fdsrc.index("async with turbo_sem") < _fdsrc.index("await _prepare()"))
check("active counter decremented in finally",
      "_turbo_active -= 1" in _fdsrc and "finally:" in _fdsrc)

# scratch storage must be separate from the RAM tmpfs holding merge FIFOs
check("buffered downloads stage in the scratch root, not the RAM tmpfs",
      "_scratch_root()" in _fdsrc and "mkdtemp(prefix=\"ytgrab-dl-\", dir=root)" in _fdsrc)
check("_scratch_root returns a writable dir", os.access(app._scratch_root(), os.W_OK))
check("scratch falls back rather than raising", isinstance(app._scratch_root(), str))
check("out-of-space is reported as 507, not a generic failure",
      "507" in _fdsrc and "ENOSPC" in _fdsrc)

_k8s = pathlib.Path("k8s/deployment.yaml").read_text()
check("k8s mounts a separate scratch volume", "name: scratch" in _k8s)
check("k8s scratch is DISK-backed (no medium: Memory)",
      "emptyDir: { sizeLimit: 8Gi }" in _k8s)
check("k8s sets SCRATCH_DIR", "SCRATCH_DIR" in _k8s)
check("k8s /tmp stays small and RAM-backed for FIFOs",
      "medium: Memory, sizeLimit: 16Mi" in _k8s)

# zip budget must fit under the shipped pod memory limit
_zdefault = 64 * 1024 * 1024
check("zip RAM budget default lowered to fit a 512Mi pod",
      f"str({64} * 1024 * 1024)" in pathlib.Path("backend/app.py").read_text())
check("k8s pins ZIP_MAX_BYTES explicitly", "ZIP_MAX_BYTES" in _k8s)

check("health reports the turbo budget", "turbo_budget" in _i2.getsource(app.health))
check("health reports the scratch dir", '"scratch"' in _i2.getsource(app.health))


# ---- v43: error taxonomy, audio passthrough, sb=mark, scratch budget ----

# 1. error taxonomy — every rule must produce an ACTION, not just a restatement
_cases = [
    ("Sign in to confirm you're not a bot", 403, "cookie"),
    ("Sign in to confirm your age", 403, "cookies"),
    ("This video is private. Login required", 403, "cookies"),
    ("HTTP Error 429: Too Many Requests", 429, "wait"),
    ("Video not available in your country", 451, "PROXY_URL"),
    ("Video unavailable", 404, "unavailable"),
    ("Unsupported URL: https://example.com/x", 400, "supported site"),
    ("This video is available to members-only", 402, "members-only"),
    ("nsig extraction failed", 502, "ENGINE_VERSION"),
    ("No space left on device", 507, "scratch"),
]
for _msg, _want, _needle in _cases:
    _exc = app._classify_error(Exception(_msg), "youtube")
    check(f"taxonomy: {_msg[:34]!r} -> {_want}", _exc.status_code == _want)
    check(f"taxonomy: {_msg[:24]!r} is actionable",
          _needle.lower() in _exc.detail["hint"].lower())
_unknown = app._classify_error(Exception("kwyjibo"), "x")
check("unknown errors admit they're unknown", _unknown.status_code == 502
      and "unrecognised" in _unknown.detail["hint"])
check("raw engine text is preserved for bug reports",
      "kwyjibo" in _unknown.detail["raw"])
check("site name is humanised in hints that name a site",
      app._classify_error(Exception("HTTP Error 429"), "x").detail["hint"].startswith("X ")
      and app._classify_error(Exception("HTTP Error 429"), "instagram"
                              ).detail["hint"].startswith("Instagram "))
check("no raw 'extract failed:' strings remain",
      'f"extract failed: {e}"' not in pathlib.Path("backend/app.py").read_text())

# 2. audio passthrough — the best audio must be offered UNTOUCHED
_info = {"formats": [
    {"format_id": "251", "vcodec": "none", "acodec": "opus", "abr": 160, "ext": "webm",
     "filesize": 5_000_000},
    {"format_id": "140", "vcodec": "none", "acodec": "mp4a.40.2", "abr": 128, "ext": "m4a",
     "filesize": 4_000_000},
    {"format_id": "137", "vcodec": "avc1.640028", "acodec": "none", "height": 1080,
     "ext": "mp4", "filesize": 50_000_000, "tbr": 4000},
]}
_out = app._curate_formats(_info)
_akinds = [f["kind"] for f in _out if f["height"] == 0]
check("best audio offered as passthrough", "a:audio" in _akinds)
check("second codec offered as passthrough too", "a:140" in _akinds)
check("mp3 still offered for old players", "mp3" in _akinds)
_best = next(f for f in _out if f["kind"] == "a:audio")
check("passthrough labelled with real codec", "Opus" in _best["label"])
check("passthrough says no re-encode", "no re-encode" in _best["note"])
check("passthrough is flagged", _best.get("lossless_passthrough") is True)
check("mp3 is marked as re-encoded", 
      next(f for f in _out if f["kind"] == "mp3")["note"].startswith("re-encoded"))
check("explicit audio format id is a distinct codec",
      next(f for f in _out if f["kind"] == "a:140")["label"].endswith("AAC"))

# 3. sb=mark — chapters, no buffering
_segs = [{"start": 10.0, "end": 30.0, "category": "sponsor"},
         {"start": 100.0, "end": 110.0, "category": "outro"}]
_ch = [{"start_time": 0.0, "end_time": 60.0, "title": "Part 1"},
       {"start_time": 60.0, "end_time": 120.0, "title": "Part 2"}]
_merged = app._merge_sb_chapters(_ch, _segs)
check("sb marks become chapters", any("[Sponsor]" == c["title"] for c in _merged))
check("category labels are humanised", any("[Outro]" == c["title"] for c in _merged))
check("chapters stay sorted",
      [c["start_time"] for c in _merged] == sorted(c["start_time"] for c in _merged))
check("no chapter overlaps a mark (ffmpeg requires this)",
      all(a["end_time"] <= b["start_time"] + 1e-9
          for a, b in zip(_merged, _merged[1:])))
check("no marks -> chapters untouched", app._merge_sb_chapters(_ch, []) == _ch)
check("segments fetched over the guarded opener",
      "_egress_opener()" in _i2.getsource(app._sb_segments))
check("sponsorblock failure never breaks a download",
      "return []" in _i2.getsource(app._sb_segments))
_dsrc = _i2.getsource(app.api_download)
check("sb=mark stays on the STREAMING path (no _file_download)",
      _dsrc.index("if sb_mark:") > _dsrc.index("if sb_remove or turbo:"))
check("sb=remove still forces the buffered path", "sb_remove or turbo" in _dsrc)

# 4. scratch budget
_est = app._estimate_bytes(_info, "m:1080")
check("merge estimate covers both streams plus output", _est > 54_000_000)
check("estimate falls back when sizes are missing",
      app._estimate_bytes({"formats": [], "duration": 600}, "m:1080") > 0)
check("estimate never returns zero", app._estimate_bytes({}, "m:1080") > 0)
_fd2 = _i2.getsource(app._file_download)
check("budget checked before staging", _fd2.index("SCRATCH_BUDGET") < _fd2.index("mkdtemp"))
check("free disk checked too", "disk_usage" in _fd2)
check("oversized single download -> 507", "507" in _fd2)
check("budget-full -> 503 retryable", "503" in _fd2)
check("reservation released on failure",
      _fd2.count("_scratch_reserved -= est_bytes") >= 2)


# ---- v44: rich video metadata ----
_full = {"view_count": 1234567, "like_count": 89000, "comment_count": 432,
         "upload_date": "20260115", "duration": 215, "channel": "Foo",
         "channel_id": "UC1", "channel_follower_count": 50000, "age_limit": 18,
         "tags": [str(i) for i in range(40)], "categories": ["Music"],
         "description": "x" * 6000, "is_live": False}
_m = app._video_meta(_full)
check("meta surfaces views", _m["views"] == 1234567)
check("meta surfaces likes", _m["likes"] == 89000)
check("meta surfaces comments", _m["comments"] == 432)
check("upload_date normalised to ISO", _m["upload_date"] == "2026-01-15")
check("meta carries duration", _m["duration"] == 215)
check("meta carries follower count", _m["channel_followers"] == 50000)
check("meta carries age limit", _m["age_limit"] == 18)
check("tags capped at 20", len(_m["tags"]) == 20)
check("description capped at 5000", len(_m["description"]) == 5000)
check("truncation flagged", _m["description_truncated"] is True)

# stable shape: missing fields are null, never absent
_empty = app._video_meta({})
_keys = {"views","likes","comments","upload_date","duration","channel",
         "channel_id","channel_url","channel_followers","is_live","was_live",
         "age_limit","categories","tags","description","description_truncated"}
check("meta shape stable when everything is missing", set(_empty) == _keys)
check("missing counts are null not zero", _empty["views"] is None and _empty["likes"] is None)
check("missing age_limit defaults to 0", _empty["age_limit"] == 0)
check("bad upload_date -> null", app._video_meta({"upload_date": "garbage"})["upload_date"] is None)
check("release_date used as date fallback",
      app._video_meta({"release_date": "20251231"})["upload_date"] == "2025-12-31")
check("no description -> null, not truncated",
      _empty["description"] is None and _empty["description_truncated"] is False)

# wired into both info endpoints
import inspect as _i3
check("YouTube /api/info returns meta", '"meta": _video_meta(info)' in _i3.getsource(app.api_info))
check("social /api/info returns meta", "_video_meta(" in _i3.getsource(app._social_api_info))

# UI renders it without crashing on null-heavy payloads
_ui = pathlib.Path("backend/static/index.html").read_text()
check("UI has a stat renderer", "function renderStats" in _ui)
check("UI formats big counts (K/M/B)", "const fcount" in _ui)
check("UI formats dates", "const fdate" in _ui)
check("UI shows a description box", 'id="descbox"' in _ui and 'id="desctext"' in _ui)
check("UI calls renderStats on load", "renderStats(d);" in _ui)
check("stat row hidden when no chips", "row.style.display = C.length" in _ui)

print(f"\nTOTAL {ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
