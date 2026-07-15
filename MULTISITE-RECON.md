# YoinkT multi-site recon — X, Facebook, Instagram (video + photos)

Recon date: 2026-07-14 (against v29). Goal: extend YoinkT from YouTube-only to a multi-purpose video **and photo** grabber for X/Twitter, Facebook, and Instagram, without breaking the settled stateless-streaming architecture.

## 1. Per-platform recon

### X / Twitter
- **Video**: yt-dlp has an active `twitter` extractor. Public tweets with video generally extract **without login**; NSFW/age-gated and protected accounts need cookies. Known quirk: passing cookies has sometimes *broken* extraction that worked anonymously ([yt-dlp #12549](https://github.com/yt-dlp/yt-dlp/issues/12549)) — so cookies must be **opt-in per request/site**, not always-on.
- **Formats**: X serves pre-merged MP4 (H.264+AAC) at a few bitrates, plus HLS (m3u8). No DASH split like YouTube → **no FIFO merge needed**. Progressive MP4s can go through the existing `_range_proxy` (resumable). HLS variants need `ffmpeg -c copy` remux to fMP4 — a *single-input* variant of the existing merge plumbing.
- **Photos**: yt-dlp does NOT handle image-only tweets. Options: (a) gallery-dl (`twitter` extractor, mature — photos + videos from multi-media tweets); (b) parse via yt-dlp's tweet metadata when present; (c) X syndication/CDN: image URLs are plain `pbs.twimg.com/media/...?name=orig` — once extracted, they're unauthenticated CDN fetches, perfect for `_range_proxy`.
- **Auth churn**: X cookies expire fast (days). Expect a re-export loop; surface cookie staleness in `/api/health`.

### Facebook
- **Video**: yt-dlp supports watch/share/reel URLs (`facebook.com/reel/` path distinct from `/watch/`). Public videos work without login; private/group/age-gated need cookies. FB changes delivery often — same "update yt-dlp first" maintenance rule as YouTube ([yt-dlp #14896](https://github.com/yt-dlp/yt-dlp/issues/14896): some reels fail while others work — usually staleness).
- **Formats**: mostly pre-merged MP4 (SD/HD) + DASH for higher qualities on some videos — curation must handle both progressive and merged kinds, but merges are rarer than YouTube.
- **Photos**: weakest link. yt-dlp: no. gallery-dl has a `facebook` extractor (photos, albums) but it's newer and login-sensitive — **verify against current gallery-dl before committing**. Cobalt, notably, ships FB *video only* — a signal that FB photos are hard/brittle.
- **Blocking**: datacenter IPs get checkpointed like YouTube; residential IP or proxy assumption carries over.

### Instagram
- **The hard one.** Since ~2023 Instagram requires login for nearly everything; yt-dlp's IG extractor has a long trail of "rate-limit reached or login required" issues ([#11166](https://github.com/yt-dlp/yt-dlp/issues/11166), [#13626](https://github.com/yt-dlp/yt-dlp/issues/13626)). yt-dlp 2026.07.04 reworked the extractor and now detects invalidated cookies — pin ≥ that version.
- **Practical stance**: treat cookies as **mandatory** for IG. Anonymous extraction (embed-page fallback) works occasionally for public reels — offer it, expect failures.
- **Formats**: pre-merged MP4 only (reels/posts/stories). No merge path needed.
- **Photos + carousels**: core IG use case. gallery-dl's `instagram` extractor handles posts/profiles/stories/highlights with cookies. Carousel posts mix photos + videos → info response must model **multi-media posts**.
- **Rate limiting**: aggressive; accounts doing bulk fetches get soft-banned or forced through checkpoints. Batch lanes must be capped to 1 for IG and requests spaced (3–5 s). Warn users their *own account* carries the risk.
- **Legal note**: Meta actively litigates scraping. Personal-use warning gets more teeth here; never default-enable IG on a public deployment.

### Cross-platform prior art (cobalt)
Cobalt supports exactly this trio: IG (reels/photos/videos, pick-from-carousel), X (pick-what-to-save from multi-media posts, "not 100% reliable"), FB (public videos only). Its UX pattern — **one URL box, auto-detect site, media picker for multi-media posts** — is the right model for YoinkT's UI.

## 2. What changes in YoinkT (design)

### 2.1 Site registry replaces the YouTube regex (SSRF guard preserved)
```python
SITES = {
  "youtube":   {"re": r"^https?://(www\.|m\.|music\.)?(youtube\.com|youtu\.be)/", "engine": "ytdlp", "merge": True,  "photos": False},
  "x":         {"re": r"^https?://(www\.|mobile\.)?(x\.com|twitter\.com)/\w+/status/\d+", "engine": "ytdlp+gdl", "merge": False, "photos": True},
  "facebook":  {"re": r"^https?://(www\.|m\.|web\.)?(facebook\.com|fb\.watch)/", "engine": "ytdlp", "merge": False, "photos": "verify"},
  "instagram": {"re": r"^https?://(www\.)?instagram\.com/(p|reel|reels|stories|tv)/", "engine": "ytdlp+gdl", "merge": False, "photos": True, "cookies": "required"},
}
```
- Still a strict allowlist (never a generic "any URL" — the SSRF guard is a fixed design decision).
- `ENABLED_SITES` env (default `youtube`) gates rollout; `/api/health` reports per-site: enabled, cookies present, engine available.

### 2.2 `/api/info` grows a media model (backward-compatible for YouTube)
Add top-level `site` and `media[]` for multi-media posts:
```json
{"site":"x", "title":"…", "uploader":"…",
 "media":[
   {"type":"video","index":0,"formats":[{"kind":"p:http-2176","label":"720p", "resumable":true}]},
   {"type":"photo","index":1,"kind":"i:1","ext":"jpg","width":4096,"filesize":null,"thumbnail":"…"}
 ]}
```
YouTube keeps its current flat `formats` shape (frontend contract unchanged); new sites use `media[]`. UI renders a cobalt-style picker with per-item checkboxes + "grab all".

### 2.3 New download kinds
| kind | path | notes |
|---|---|---|
| `i:<index>` | `_range_proxy` of image CDN URL | resumable, zero subprocess — photos are the *cheapest* media YoinkT will ever serve |
| `h:<format_id>` | single `yt-dlp -o -` (HLS) or `ffmpeg -i m3u8 -c copy` → fMP4 | reuse pipe-safe fMP4 flags; NO FIFOs (single input) |
| `z:all` | streaming ZIP of a carousel/multi-photo post | see 2.5 |
| existing `p:/a:/mp3` | unchanged | pre-merged MP4s on X/FB/IG map to `p:` semantics |
| existing `m:/c:` | **YouTube-only** | gate by `site.merge` |

### 2.4 Engine strategy: yt-dlp stays primary, gallery-dl added for images
- yt-dlp for all video on all four sites (one maintenance story, already containerized).
- gallery-dl (pip-installable, same image) invoked in **URL-resolution mode** (`gallery-dl -g <url>` + `--dump-json`) for image posts on X/IG — YoinkT then `_range_proxy`s the returned CDN URLs itself. This keeps the stateless/no-disk property: gallery-dl never writes media, it only resolves.
- Cache resolution results in `_extract_cached` (keyed url+engine); image CDN URLs are long-lived, TTL can stay 300s.
- Add `GALLERY_DL_COOKIES_*` plumbing mirroring yt-dlp's.

### 2.5 Streaming ZIP for carousels ("grab all")
Multi-photo posts want one click → one file. A ZIP with **stored (uncompressed) entries + data descriptors** can be written strictly sequentially to a non-seekable pipe (same trick as fMP4). Use `stream-zip` (pip) or hand-rolled store-only writer; images are already compressed, so store-only loses nothing. Stateless preserved: fetch each image → yield into zip stream → done. Cap total size / count.

### 2.6 Per-site cookies (not one global cookie jar)
- `COOKIES_FILE` → `COOKIES_FILE_YOUTUBE / _X / _FACEBOOK / _INSTAGRAM` (k8s: one Secret, multiple keys). Global var kept as fallback.
- WHY per-site: the X issue above (cookies *breaking* anonymous-working extraction) means cookies must attach only to the site they belong to; also blast-radius — an invalidated IG session shouldn't degrade YouTube.
- Cookies wizard in UI gains a site tab; `/api/health` reports per-site cookie presence + (for IG) validity if yt-dlp exposes it.

### 2.7 Politeness / concurrency per site
- Per-site semaphore weights: IG lanes hard-capped at 1, X/FB at 2, YouTube keeps MAX_CONCURRENCY.
- IG batch: enforce 3–5 s spacing between requests (recon: rate-limit bans).
- Batch UI reads per-site caps from `/api/health` (extends the existing lanes-from-health pattern).

### 2.8 What does NOT carry over (gate by site)
- SponsorBlock, chapters, PO tokens/Deno n-challenge, playlist endpoint, QuickTime toggle mostly moot (X/FB/IG are already H.264) — hide these controls when site ≠ youtube.
- Clip (start/end) can work on X/FB/IG progressive MP4s via existing ffmpeg `-ss` path — cheap win, keep it.

### 2.9 UI
- URL box auto-detects site → site badge (𝕏 / f / IG / ▶) on the hero card.
- Multi-media posts: thumbnail grid with checkboxes, per-item download + "grab all (zip)". Photos use native `<a download>` (resumable range-proxy).
- Recents store site; settings memory unchanged.
- Footer version bump + `always-mp4` tag no longer universally true → rename stamp to `YoinkT vNN-multisite`.

## 3. Phasing (risk-ordered)

1. **Phase 1 — X** (lowest risk): site registry + `media[]` + `i:` photo kind + range-proxy images + picker UI. No cookies needed for the happy path. Proves the multi-site plumbing.
2. **Phase 2 — Facebook video** (reels + watch): yt-dlp only, progressive kinds, cookies optional. Skip FB photos until gallery-dl support is verified.
3. **Phase 3 — Instagram**: cookies-required flow, gallery-dl resolution for photos/carousels, ZIP path, per-site politeness. Hardest, ships last.
4. **Phase 4 (optional)**: profile/bulk via gallery-dl (X media timeline, IG profile) — reuses playlist batch UI, but rate-limit exposure is highest; keep behind a flag.

## 4. Risks & open items

- **gallery-dl FB extractor maturity** — verify live before Phase 2 includes photos.
- **IG extractor volatility** — pin yt-dlp ≥ 2026.07.04; weekly image rebuild becomes even more important (three more moving targets).
- **Cookie churn** (X: days; IG: weeks) — health-endpoint staleness signal + UI warning is a must, or users hit silent failures (lesson of bug #9: invisible failures are the real enemy).
- **Legal**: all three platforms' ToS prohibit downloading/scraping; Meta litigates. Keep personal-use scope, never default-enable non-YouTube sites in a public deployment, and repeat the warning in README.
- **Naming**: "YoinkT" codename stays; YoinkT tagline "grab it and go" now covers photos too.

## Sources
- [yt-dlp supported extractors](https://github.com/yt-dlp/yt-dlp/wiki/extractors)
- [yt-dlp #12549 — Twitter cookies break extraction](https://github.com/yt-dlp/yt-dlp/issues/12549)
- [yt-dlp #11166 — Instagram rate-limit/login errors](https://github.com/yt-dlp/yt-dlp/issues/11166)
- [yt-dlp #13626 — Instagram metadata extraction failed](https://github.com/yt-dlp/yt-dlp/issues/13626)
- [yt-dlp 2026.07.04 release (IG rework)](https://github.com/yt-dlp/yt-dlp/releases/tag/2026.07.04)
- [yt-dlp #14896 — some FB reels fail](https://github.com/yt-dlp/yt-dlp/issues/14896)
- [gallery-dl (PyPI)](https://pypi.org/project/gallery-dl/)
- [cobalt supported services](https://github.com/imputnet/cobalt/blob/main/api/README.md)
