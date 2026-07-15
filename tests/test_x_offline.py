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
print(f"\nTOTAL {ok} passed, {fail} failed")
sys.exit(1 if fail else 0)
