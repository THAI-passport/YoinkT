import { useEffect, useState } from "react";

type Fmt = {
  kind: string; label: string; height: number; ext: string;
  fps: number | null; filesize: number | null; note: string;
};
type Info = {
  id: string; title: string; channel: string | null; duration: number | null;
  thumbnail: string | null; formats: Fmt[];
};

const mb = (n: number | null) =>
  n ? (n / 1048576 >= 1024 ? (n / 1073741824).toFixed(2) + " GB" : (n / 1048576).toFixed(1) + " MB") : "size unknown";
const dur = (s: number | null) => {
  if (!s) return "";
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = s % 60;
  return (h ? h + ":" + String(m).padStart(2, "0") : m) + ":" + String(x).padStart(2, "0");
};
const YT_RE = /^https?:\/\/(www\.|m\.|music\.)?(youtube\.com|youtu\.be|youtubekids\.com)\//i;

function Badge({ f }: { f: Fmt }) {
  if (f.kind.startsWith("a:"))
    return <span className="rounded bg-sky-500/15 px-1.5 text-[10.5px] font-bold uppercase tracking-wide text-sky-300">audio</span>;
  if (f.note === "progressive")
    return <span className="rounded bg-green-500/15 px-1.5 text-[10.5px] font-bold uppercase tracking-wide text-green-400">direct</span>;
  return <span className="rounded bg-orange-500/15 px-1.5 text-[10.5px] font-bold uppercase tracking-wide text-orange-300">{f.note.split("· ")[1] || "merged"}</span>;
}

export default function App() {
  const [url, setUrl] = useState("");
  const [info, setInfo] = useState<Info | null>(null);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [ver, setVer] = useState("");
  const [started, setStarted] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/health").then(r => r.json()).then(h => setVer(h.version || "OLD BUILD")).catch(() => {});
    const onPaste = (e: ClipboardEvent) => {
      const t = e.clipboardData?.getData("text").trim() || "";
      if (YT_RE.test(t)) { setUrl(t); fetchInfo(t); e.preventDefault(); }
    };
    document.addEventListener("paste", onPaste);
    return () => document.removeEventListener("paste", onPaste);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function fetchInfo(u: string) {
    setErr(""); setInfo(null); setBusy(true);
    try {
      const r = await fetch("/api/info?url=" + encodeURIComponent(u));
      if (!r.ok) throw new Error((await r.json()).detail || r.statusText);
      setInfo(await r.json());
    } catch (ex) {
      setErr(ex instanceof Error ? ex.message : String(ex));
    } finally { setBusy(false); }
  }

  const dl = (kind: string) =>
    "/api/download?url=" + encodeURIComponent(url.trim()) + "&kind=" + encodeURIComponent(kind);

  return (
    <div className="min-h-screen bg-[#0c0e10] text-[#ece9e4]"
      style={{ background: "radial-gradient(1200px 500px at 70% -10%, rgba(224,93,56,.09), transparent 60%), radial-gradient(900px 400px at 10% 110%, rgba(92,141,181,.06), transparent 60%), #0c0e10" }}>
      <div className="mx-auto max-w-[820px] px-5 pb-8 pt-12">
        <header className="mb-7 flex items-baseline gap-3.5">
          <div className="text-[26px] font-extrabold tracking-tight">yt-<b className="text-orange-500">grab</b></div>
          <small className="text-neutral-400">paste a link · pick a quality · done</small>
        </header>

        <form className="flex gap-2.5" onSubmit={e => { e.preventDefault(); url.trim() && fetchInfo(url.trim()); }}>
          <input type="url" required autoFocus value={url} onChange={e => setUrl(e.target.value)}
            placeholder="https://www.youtube.com/watch?v=..."
            className="flex-1 rounded-[10px] border border-[#2a2f36] bg-[#16191d] px-4 py-3.5 outline-none transition focus:border-orange-500 focus:ring-[3px] focus:ring-orange-500/20" />
          <button disabled={busy}
            className="rounded-[10px] bg-orange-600 px-6 py-3.5 font-bold transition hover:bg-orange-500 active:scale-95 disabled:opacity-50">
            {busy ? "Fetching…" : "Fetch"}
          </button>
        </form>
        <div className="mt-2.5 text-[13px] text-neutral-500">
          Tip: paste a YouTube URL anywhere on this page — it fetches automatically.
        </div>

        {err && <div className="mt-4 whitespace-pre-wrap rounded-[10px] border border-red-400/35 bg-red-400/10 px-4 py-3 text-sm text-red-300">{err}</div>}

        {busy && (
          <div className="mt-6 overflow-hidden rounded-[14px] border border-[#2a2f36]">
            <div className="h-60 animate-pulse bg-[#16191d]" />
            <div className="h-16 animate-pulse border-t border-[#2a2f36] bg-[#16191d]" />
          </div>
        )}

        {info && !busy && (
          <div className="mt-6 overflow-hidden rounded-[14px] border border-[#2a2f36] bg-[#16191d]">
            <div className="relative aspect-[16/7] overflow-hidden bg-black">
              {info.thumbnail && <img src={info.thumbnail} alt="" className="h-full w-full object-cover" />}
              <div className="absolute inset-0 bg-gradient-to-b from-transparent via-transparent to-[#0a0b0d]/95" />
              {info.duration ? (
                <div className="absolute right-3 top-3 rounded-md bg-black/70 px-2 py-0.5 font-mono text-[12.5px] font-semibold">{dur(info.duration)}</div>
              ) : null}
              <div className="absolute inset-x-5 bottom-3.5">
                <h2 className="text-[19px] font-semibold leading-snug [text-shadow:0_1px_8px_rgba(0,0,0,.7)]">{info.title}</h2>
                <p className="mt-0.5 text-[13.5px] text-neutral-300 [text-shadow:0_1px_6px_rgba(0,0,0,.7)]">{info.channel}</p>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-2.5 p-4 sm:grid-cols-[repeat(auto-fill,minmax(180px,1fr))]">
              {info.formats.map(f => (
                <a key={f.kind} href={dl(f.kind)} download
                  onClick={() => { setStarted(f.kind); setTimeout(() => setStarted(null), 4000); }}
                  className={"flex items-center justify-between gap-2 rounded-[10px] border bg-[#22262b] px-3.5 py-3 transition hover:border-orange-500 hover:bg-[#2a2015] active:scale-[.98] " +
                    (started === f.kind ? "border-green-500" : "border-[#2a2f36]")}>
                  <span className="text-[17px] font-extrabold">
                    {f.label}{f.fps && f.fps > 30 && <small className="ml-1 text-xs font-semibold text-neutral-400">{f.fps}fps</small>}
                    {started === f.kind && <span className="text-green-400"> ✓</span>}
                  </span>
                  <span className="text-right text-xs leading-relaxed text-neutral-400">
                    <b className="font-semibold text-[#ece9e4]">{f.ext.toUpperCase()}</b> · {mb(f.filesize)}<br /><Badge f={f} />
                  </span>
                </a>
              ))}
            </div>
          </div>
        )}

        <footer className="mt-9 flex justify-between gap-3 text-[13px] text-neutral-500">
          <span>Streams pipe straight through the server — nothing stored. High-res merges land as MP4.</span>
          <span className="font-mono text-xs font-semibold text-neutral-600">{ver}</span>
        </footer>
      </div>
    </div>
  );
}
