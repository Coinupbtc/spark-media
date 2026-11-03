#!/usr/bin/env python3
"""Spark Media — the house library of every photo and video the DGX Sparks make.

Self-sufficient on purpose (2026-11-03: Discord is down and may not come back): one Python file, stdlib only
(+ Pillow for image thumbnails, ffmpeg for video), SQLite, files hard-linked (or copied) into ~/media-library so the
library survives a project folder being deleted. Serves a phone-friendly gallery with byte-range video playback.

  spark_media.py serve                      # HTTP on $SPARK_MEDIA_PORT (default 8120), scans roots every 15 min
  spark_media.py scan [--full]              # ingest new files from config.json roots (--full re-reads sidecars)
  spark_media.py ingest FILE [--caption ..] [--prompt ..] [--project ..] [--tier ..] [--tags a,b]
  spark_media.py import-discord-log         # captions/prompts from ~/logs/discord/discord-media-send.log
  spark_media.py stats
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOME = Path.home()
APP_DIR = Path(__file__).resolve().parent
CONFIG = Path(os.environ.get("SPARK_MEDIA_CONFIG", APP_DIR / "config.json"))
LIB = Path(os.environ.get("SPARK_MEDIA_LIBRARY", HOME / "media-library"))
DB_PATH = LIB / "library.sqlite"
STORE, THUMBS, STRIPS, LOGF = LIB / "store", LIB / "thumbs", LIB / "strips", HOME / "logs" / "spark-media.log"
VIDEO_EXT = {".mp4", ".webm", ".mov", ".mkv", ".m4v"}
IMAGE_EXT = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
SCAN_EVERY_S = 15 * 60
REF_PROMPT = "Macro soldering a PCB under warm bench light, soft room tone."
_log_lock = threading.Lock()
_FTS_OK = False


def log(msg: str) -> None:
    line = f"[{dt.datetime.now().isoformat(timespec='seconds')}] {msg}"
    with _log_lock:
        print(line, flush=True)
        try:
            LOGF.parent.mkdir(parents=True, exist_ok=True)
            with LOGF.open("a") as f:
                f.write(line + "\n")
        except OSError:
            pass


# ----------------------------------------------------------------------------- storage
SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY, sha256 TEXT UNIQUE NOT NULL, sha12 TEXT NOT NULL, kind TEXT NOT NULL, ext TEXT NOT NULL,
  store_path TEXT NOT NULL, orig_path TEXT NOT NULL, also_paths TEXT DEFAULT '', project TEXT DEFAULT '', tier TEXT DEFAULT '',
  tags TEXT DEFAULT '', prompt TEXT DEFAULT '', prompt_source TEXT DEFAULT '', caption TEXT DEFAULT '', seed TEXT DEFAULT '',
  steps INTEGER, width INTEGER, height INTEGER, duration REAL, fps REAL, bytes INTEGER, created_at TEXT, ingested_at TEXT,
  elapsed_s REAL, model TEXT DEFAULT '', meta TEXT DEFAULT '{}', hidden INTEGER DEFAULT 0);
CREATE INDEX IF NOT EXISTS items_created ON items(created_at DESC);
CREATE INDEX IF NOT EXISTS items_project ON items(project);
CREATE TABLE IF NOT EXISTS seen (path TEXT PRIMARY KEY, size INTEGER, mtime REAL, sha256 TEXT);
"""


def db() -> sqlite3.Connection:
    LIB.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(SCHEMA)
    try:
        c.execute("CREATE VIRTUAL TABLE IF NOT EXISTS items_fts USING fts5(prompt, caption, orig_path, project, tags, content='items', content_rowid='id')")
        c.executescript("""
        CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items BEGIN
          INSERT INTO items_fts(rowid, prompt, caption, orig_path, project, tags) VALUES (new.id, new.prompt, new.caption, new.orig_path, new.project, new.tags); END;
        CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items BEGIN
          INSERT INTO items_fts(items_fts, rowid, prompt, caption, orig_path, project, tags) VALUES ('delete', old.id, old.prompt, old.caption, old.orig_path, old.project, old.tags); END;
        CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items BEGIN
          INSERT INTO items_fts(items_fts, rowid, prompt, caption, orig_path, project, tags) VALUES ('delete', old.id, old.prompt, old.caption, old.orig_path, old.project, old.tags);
          INSERT INTO items_fts(rowid, prompt, caption, orig_path, project, tags) VALUES (new.id, new.prompt, new.caption, new.orig_path, new.project, new.tags); END;
        """)
        global _FTS_OK
        _FTS_OK = True
    except sqlite3.OperationalError:
        _FTS_OK = False
    return c


def load_config() -> dict:
    if CONFIG.exists():
        return json.loads(CONFIG.read_text())
    return {"roots": []}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ffprobe(path: Path) -> dict:
    try:
        out = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                              "stream=width,height,r_frame_rate,nb_frames", "-show_entries", "format=duration",
                              "-of", "json", str(path)], capture_output=True, text=True, timeout=60).stdout
        d = json.loads(out or "{}")
        st = (d.get("streams") or [{}])[0]
        fr = st.get("r_frame_rate", "0/1")
        num, den = (fr.split("/") + ["1"])[:2]
        fps = float(num) / float(den) if float(den) else 0.0
        return {"width": st.get("width"), "height": st.get("height"), "fps": round(fps, 3),
                "duration": round(float(d.get("format", {}).get("duration", 0) or 0), 3)}
    except (subprocess.SubprocessError, ValueError, json.JSONDecodeError):
        return {}


def image_size(path: Path) -> dict:
    try:
        from PIL import Image
        with Image.open(path) as im:
            return {"width": im.width, "height": im.height}
    except Exception:  # noqa: BLE001
        return {}


def make_thumbs(kind: str, src: Path, sha12: str, duration: float | None) -> None:
    THUMBS.mkdir(parents=True, exist_ok=True)
    STRIPS.mkdir(parents=True, exist_ok=True)
    thumb, strip = THUMBS / f"{sha12}.jpg", STRIPS / f"{sha12}.jpg"
    if kind == "video":
        d = duration or 0
        if not thumb.exists():
            subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", f"{max(0.0, d * 0.25):.2f}", "-i", str(src), "-frames:v", "1",
                            "-vf", "scale=480:-2", "-q:v", "4", str(thumb)], capture_output=True, timeout=120)
        if not strip.exists() and d > 0:
            sel = "+".join(f"gte(t\\,{d * f:.2f})*lt(t\\,{d * f + 0.5:.2f})" for f in (0.05, 0.3, 0.5, 0.7, 0.95))
            subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", str(src), "-vf",
                            f"select='{sel}',scale=320:-2,tile=5x1", "-frames:v", "1", "-vsync", "vfr", "-q:v", "5", str(strip)],
                           capture_output=True, timeout=180)
    else:
        if not thumb.exists():
            try:
                from PIL import Image
                with Image.open(src) as im:
                    im = im.convert("RGB")
                    im.thumbnail((480, 480))
                    im.save(thumb, "JPEG", quality=85)
            except Exception as e:  # noqa: BLE001
                log(f"thumb failed for {src}: {e}")


FNAME_RE = re.compile(r"^h3-(\d{8})_(\d{6})-(.+?)-\d+$")


def created_from_name(path: Path) -> tuple[str | None, str | None]:
    m = FNAME_RE.match(path.stem)
    if not m:
        return None, None
    d, t, pfx = m.groups()
    try:
        ts = dt.datetime.strptime(d + t, "%Y%m%d%H%M%S").isoformat(timespec="seconds")
    except ValueError:
        ts = None
    return ts, pfx.replace("_", " ").strip()


def read_sidecars(path: Path) -> dict:
    """Prompt/params from the files the house tools leave next to a clip."""
    meta: dict = {}
    sc = path.with_suffix(path.suffix + ".json")
    if sc.exists():
        try:
            j = json.loads(sc.read_text())
            meta.update({k: j[k] for k in ("prompt", "seed", "steps", "elapsed_s", "model", "tier", "caption", "width", "height", "duration", "fps", "api_base", "flow_shift", "turbo") if k in j})
            meta["prompt_source"] = "sidecar"
        except (json.JSONDecodeError, OSError):
            pass
    txt = path.with_suffix(".txt")
    if "prompt" not in meta and txt.exists() and txt.stat().st_size < 4000:
        kv = dict(line.split("=", 1) for line in txt.read_text().splitlines() if "=" in line and not line.startswith("ffprobe"))
        if "sha256" in kv and "label" in kv:
            meta.update({"prompt": REF_PROMPT, "prompt_source": "twin-test", "caption": f"{kv.get('label', '')}: {kv.get('verdict', '')}".strip(": "),
                         "tier": "proof", "seed": "42"})
            el = re.match(r"([\d.]+)", kv.get("client_elapsed_s", ""))
            if el:
                meta["elapsed_s"] = float(el.group(1))
    d = path.parent
    if "prompt" not in meta and (d / "prompt.txt").exists():
        meta["prompt"], meta["prompt_source"] = (d / "prompt.txt").read_text().strip(), "prompt.txt"
    if "prompt" not in meta and (d / "gen.log").exists():
        for line in (d / "gen.log").read_text(errors="replace").splitlines():
            if line.startswith("Prompt: "):
                meta["prompt"], meta["prompt_source"] = line[len("Prompt: "):].strip(), "gen.log"
                break
    for p in (d / "plan.txt", d.parent / "plan.txt"):
        if p.exists() and p.stat().st_size < 4000:
            meta.setdefault("caption", p.read_text().strip()[:500])
            break
    return meta


def guess_tier(path: str, tier: str) -> str:
    if tier:
        return tier
    s = path.lower()
    if "/twin/" in s or "proof" in s:
        return "proof"
    if "draft" in s:
        return "draft"
    if "/blind/" in s or "/ab-" in s or "/renders/" in s:
        return "ab"
    if "/h3-chain-" in s or "/chain" in s:
        return "chain"
    return "full"


def ingest_file(c: sqlite3.Connection, path: Path, project: str = "", tier: str = "", tags: str = "", caption: str = "",
                prompt: str = "", force_meta: bool = False) -> int | None:
    path = path.resolve()
    if not path.is_file() or ".partial." in path.name:
        return None
    ext = path.suffix.lower()
    kind = "video" if ext in VIDEO_EXT else "image" if ext in IMAGE_EXT else None
    if kind is None:
        return None
    st = path.stat()
    row = c.execute("SELECT sha256 FROM seen WHERE path=? AND size=? AND mtime=?", (str(path), st.st_size, st.st_mtime)).fetchone()
    sha = row["sha256"] if row else sha256_of(path)
    c.execute("INSERT OR REPLACE INTO seen(path,size,mtime,sha256) VALUES (?,?,?,?)", (str(path), st.st_size, st.st_mtime, sha))
    sha12 = sha[:12]
    existing = c.execute("SELECT id, orig_path, also_paths, prompt, caption FROM items WHERE sha256=?", (sha,)).fetchone()
    meta = read_sidecars(path)
    created_name, pfx = created_from_name(path)
    if existing:
        updates = {}
        if str(path) != existing["orig_path"] and str(path) not in (existing["also_paths"] or ""):
            updates["also_paths"] = ((existing["also_paths"] or "") + "\n" + str(path)).strip()
        if (prompt or meta.get("prompt")) and (force_meta or not existing["prompt"] or existing["prompt"] == pfx):
            updates["prompt"] = prompt or meta.get("prompt")
            updates["prompt_source"] = "arg" if prompt else meta.get("prompt_source", "")
        if caption and (force_meta or not existing["caption"]):
            updates["caption"] = caption
        if updates:
            c.execute("UPDATE items SET " + ", ".join(f"{k}=?" for k in updates) + " WHERE id=?", (*updates.values(), existing["id"]))
        return existing["id"]
    created = created_name or dt.datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
    sub = STORE / created[:4] / created[5:7]
    sub.mkdir(parents=True, exist_ok=True)
    store = sub / f"{sha12}-{path.name}"
    if not store.exists():
        try:
            os.link(path, store)
        except OSError:
            shutil.copy2(path, store)
    info = ffprobe(store) if kind == "video" else image_size(store)
    make_thumbs(kind, store, sha12, info.get("duration"))
    final_prompt = prompt or meta.get("prompt") or pfx or ""
    psrc = "arg" if prompt else meta.get("prompt_source") or ("filename-prefix" if pfx else "")
    cur = c.execute(
        "INSERT INTO items(sha256,sha12,kind,ext,store_path,orig_path,project,tier,tags,prompt,prompt_source,caption,seed,steps,width,height,"
        "duration,fps,bytes,created_at,ingested_at,elapsed_s,model,meta) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sha, sha12, kind, ext, str(store), str(path), project, guess_tier(str(path), tier or meta.get("tier", "")), tags,
         final_prompt, psrc, caption or meta.get("caption", ""), str(meta.get("seed", "") or ""), meta.get("steps"),
         info.get("width") or meta.get("width"), info.get("height") or meta.get("height"), info.get("duration"), info.get("fps"),
         st.st_size, created, dt.datetime.now().isoformat(timespec="seconds"), meta.get("elapsed_s"),
         meta.get("model", "minimax-h3" if kind == "video" else ""), json.dumps({k: v for k, v in meta.items() if k not in ("prompt", "caption")})))
    log(f"ingested {kind} {sha12} {path}")
    return cur.lastrowid


def scan(full: bool = False) -> dict:
    cfg = load_config()
    c = db()
    n_new = n_seen = 0
    for root in cfg.get("roots", []):
        base = Path(os.path.expanduser(root["path"]))
        if not base.exists():
            continue
        kinds = set(root.get("kinds", ["video"]))
        exts = (VIDEO_EXT if "video" in kinds else set()) | (IMAGE_EXT if "image" in kinds else set())
        excl = [re.compile(x) for x in root.get("exclude", [])]
        for p in base.rglob("*"):
            if not p.is_file() or p.suffix.lower() not in exts or ".partial." in p.name:
                continue
            sp = str(p)
            if any(x.search(sp) for x in excl) or "/.git/" in sp or "/node_modules/" in sp:
                continue
            if p.stat().st_size < root.get("min_bytes", 20000):
                continue
            n_seen += 1
            before = c.execute("SELECT 1 FROM seen WHERE path=? AND size=? AND mtime=?", (sp, p.stat().st_size, p.stat().st_mtime)).fetchone()
            if before and not full:
                continue
            try:
                if ingest_file(c, p, project=root.get("label", base.name), tier=root.get("tier", ""), tags=",".join(root.get("tags", [])), force_meta=full) and not before:
                    n_new += 1
            except Exception as e:  # noqa: BLE001
                log(f"scan error {p}: {e}")
            c.commit()
    c.commit()
    total = c.execute("SELECT count(*) FROM items").fetchone()[0]
    c.close()
    log(f"scan done: {n_seen} files seen, {n_new} new, {total} items in the library")
    return {"seen": n_seen, "new": n_new, "total": total}


def import_discord_log(path: Path = HOME / "logs" / "discord" / "discord-media-send.log") -> int:
    """Captions (and prompts inside them) for every file ever delivered to Discord #media."""
    if not path.exists():
        return 0
    c = db()
    n = 0
    rx = re.compile(r"^\[(?P<ts>[^\]]+)\] channel=(?P<ch>\S+) file=(?P<file>\S+) msg=(?P<msg>.*)$")
    for line in path.read_text(errors="replace").splitlines():
        m = rx.match(line)
        if not m:
            continue
        f = Path(m.group("file"))
        msg = m.group("msg").strip()
        pm = re.search(r"Prompt: (.+)$", msg)
        prompt = pm.group(1).strip() if pm else ""
        if f.exists():
            try:
                if ingest_file(c, f, project="discord-#" + m.group("ch"), caption=msg, prompt=prompt):
                    n += 1
            except Exception as e:  # noqa: BLE001
                log(f"discord import error {f}: {e}")
        else:
            row = c.execute("SELECT id, caption, prompt FROM items WHERE orig_path=? OR also_paths LIKE ?", (str(f), f"%{f}%")).fetchone()
            if row:
                c.execute("UPDATE items SET caption=COALESCE(NULLIF(caption,''),?), prompt=CASE WHEN prompt='' OR prompt_source='filename-prefix' THEN COALESCE(NULLIF(?,''),prompt) ELSE prompt END WHERE id=?",
                          (msg, prompt, row["id"]))
                n += 1
    c.commit()
    c.close()
    log(f"discord log import: {n} items touched")
    return n


# ----------------------------------------------------------------------------- web
CSS = """
:root{color-scheme:dark;--bg:#0f1115;--card:#171a21;--fg:#e8e8ea;--mut:#9aa0ad;--acc:#4cc2ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.4 -apple-system,system-ui,Segoe UI,Roboto,sans-serif}
header{position:sticky;top:0;background:#0f1115ee;backdrop-filter:blur(8px);padding:10px 14px;border-bottom:1px solid #222;display:flex;gap:10px;align-items:center;flex-wrap:wrap}
header h1{font-size:17px;margin:0 8px 0 0}header a{color:var(--fg);text-decoration:none}
input[type=search]{flex:1;min-width:160px;background:#0b0d11;border:1px solid #2a2f3a;color:var(--fg);padding:8px 10px;border-radius:8px;font-size:16px}
.chips{display:flex;gap:6px;flex-wrap:wrap}.chip{padding:5px 10px;border-radius:999px;background:#1d2130;color:var(--mut);font-size:13px;text-decoration:none}
.chip.on{background:var(--acc);color:#000}select{background:#0b0d11;color:var(--fg);border:1px solid #2a2f3a;border-radius:8px;padding:6px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:8px;padding:10px}
.card{background:var(--card);border-radius:10px;overflow:hidden;position:relative;display:block;color:var(--fg);text-decoration:none}
.card img{width:100%;aspect-ratio:16/9;object-fit:cover;display:block;background:#000}.card .b{position:absolute;top:6px;right:6px;background:#000a;padding:2px 6px;border-radius:6px;font-size:12px}
.card .t{position:absolute;top:6px;left:6px;background:#000a;padding:2px 6px;border-radius:6px;font-size:11px;color:var(--acc)}
.card .m{padding:7px 9px;font-size:12px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.card .p{color:var(--fg);font-size:13px;height:2.8em;overflow:hidden;padding:0 9px 8px}
.detail{max-width:1100px;margin:0 auto;padding:12px}.detail video,.detail img.full{width:100%;max-height:72vh;background:#000;border-radius:10px}
table.kv{width:100%;border-collapse:collapse;margin-top:10px;font-size:14px}table.kv td{padding:6px 8px;border-bottom:1px solid #22262f;vertical-align:top}table.kv td:first-child{color:var(--mut);width:120px;white-space:nowrap}
.btn{display:inline-block;padding:8px 12px;border-radius:8px;background:#1d2130;color:var(--fg);text-decoration:none;margin:6px 6px 0 0}
.nav{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 10px}.nav .btn{margin:0;font-size:15px;padding:10px 14px}.chip.home{background:#1d2130;color:var(--fg);font-weight:600}
.strip{width:100%;border-radius:8px;margin-top:10px}.pager{display:flex;justify-content:center;gap:10px;padding:16px}.stats{color:var(--mut);font-size:13px;padding:0 14px}
pre.prompt{white-space:pre-wrap;background:#0b0d11;padding:10px;border-radius:8px;font:14px/1.45 ui-monospace,Menlo,monospace}
"""


def page(title: str, body: str, q: str = "", kind: str = "", project: str = "", tier: str = "", projects: list[str] | None = None, stats: str = "") -> str:
    qs = lambda **kw: "?" + urllib.parse.urlencode({k: v for k, v in {"q": q, "kind": kind, "project": project, "tier": tier, **kw}.items() if v})  # noqa: E731
    chips = "".join(f'<a class="chip{" on" if kind == k else ""}" href="{qs(kind=k)}">{lbl}</a>' for k, lbl in (("", "All"), ("video", "Video"), ("image", "Photos")))
    chips += "".join(f'<a class="chip{" on" if tier == t else ""}" href="{qs(tier=t)}">{t}</a>' for t in ("full", "draft", "chain", "ab", "proof"))
    opts = "".join(f'<option value="{html.escape(p)}"{" selected" if p == project else ""}>{html.escape(p)}</option>' for p in (projects or []))
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>{html.escape(title)}</title><style>{CSS}</style></head><body>
<header><h1><a href="/">Spark Media</a></h1><a class="chip home" href="/">⌂ Home</a><form method="get" action="/" style="display:contents">
<input type="search" name="q" placeholder="search prompts, captions, paths" value="{html.escape(q)}">
<select name="project" onchange="this.form.submit()"><option value="">all projects</option>{opts}</select>
<input type="hidden" name="kind" value="{html.escape(kind)}"><input type="hidden" name="tier" value="{html.escape(tier)}"></form>
<div class="chips">{chips}</div></header><div class="stats">{stats}</div>{body}</body></html>"""


def fmt_dur(d: float | None) -> str:
    if not d:
        return ""
    return f"{int(d // 60)}:{int(d % 60):02d}" if d >= 60 else f"{d:.1f}s"


def render_gallery(c: sqlite3.Connection, q: str, kind: str, project: str, tier: str, pg: int) -> str:
    per = 60
    where, args = ["hidden=0"], []
    if kind:
        where.append("kind=?"); args.append(kind)
    if project:
        where.append("project=?"); args.append(project)
    if tier:
        where.append("tier=?"); args.append(tier)
    if q:
        if _FTS_OK:
            fq = " ".join(f'"{t}"' for t in re.findall(r"[\w\-]+", q))
            where.append("id IN (SELECT rowid FROM items_fts WHERE items_fts MATCH ?)"); args.append(fq or '""')
        else:
            where.append("(prompt LIKE ? OR caption LIKE ? OR orig_path LIKE ?)"); args += [f"%{q}%"] * 3
    w = " AND ".join(where)
    total = c.execute(f"SELECT count(*) FROM items WHERE {w}", args).fetchone()[0]
    rows = c.execute(f"SELECT * FROM items WHERE {w} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?", (*args, per, (pg - 1) * per)).fetchall()
    projects = [r[0] for r in c.execute("SELECT DISTINCT project FROM items WHERE hidden=0 ORDER BY project")]
    cards = []
    for r in rows:
        badge = fmt_dur(r["duration"]) if r["kind"] == "video" else f'{r["width"] or "?"}×{r["height"] or "?"}'
        cards.append(f'<a class="card" href="/item/{r["id"]}"><img loading="lazy" src="/thumb/{r["sha12"]}.jpg" alt=""><span class="b">{badge}</span>'
                     f'<span class="t">{html.escape(r["tier"] or "")}</span><div class="m">{html.escape((r["created_at"] or "")[:16].replace("T", " "))} · {html.escape(r["project"] or "")}</div>'
                     f'<div class="p">{html.escape((r["prompt"] or r["caption"] or r["orig_path"].rsplit("/", 1)[-1])[:140])}</div></a>')
    base = urllib.parse.urlencode({k: v for k, v in {"q": q, "kind": kind, "project": project, "tier": tier}.items() if v})
    pager = f'<div class="pager">{f"<a class=btn href=\"/?{base}&page={pg-1}\">‹ newer</a>" if pg > 1 else ""}{f"<a class=btn href=\"/?{base}&page={pg+1}\">older ›</a>" if pg * per < total else ""}</div>'
    stats = f"{total} items" + (f" matching “{html.escape(q)}”" if q else "") + f" · page {pg} of {max(1, -(-total // per))}"
    return page("Spark Media", f'<div class="grid">{"".join(cards) or "<p style=padding:14px>Nothing here yet.</p>"}</div>{pager}', q, kind, project, tier, projects, stats)


def render_item(c: sqlite3.Connection, item_id: int) -> str | None:
    r = c.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
    if not r:
        return None
    src = f"/media/{r['sha12']}{r['ext']}"
    media = (f'<video controls playsinline preload="metadata" poster="/thumb/{r["sha12"]}.jpg" src="{src}"></video>' if r["kind"] == "video"
             else f'<img class="full" src="{src}" alt="">')
    meta = json.loads(r["meta"] or "{}")
    kv = [("created", (r["created_at"] or "").replace("T", " ")), ("project", r["project"]), ("tier", r["tier"]), ("tags", r["tags"]),
          ("size", f'{r["width"]}×{r["height"]}' + (f' · {fmt_dur(r["duration"])} · {r["fps"]:g} fps' if r["kind"] == "video" else "")),
          ("seed / steps", f'{r["seed"] or "—"} / {r["steps"] or "—"}'), ("render time", f'{r["elapsed_s"]:.0f} s' if r["elapsed_s"] else "—"),
          ("model", r["model"]), ("file", f'{r["bytes"] / 1e6:.1f} MB · sha {r["sha12"]}'), ("original", r["orig_path"]),
          ("also at", r["also_paths"].replace("\n", "<br>") if r["also_paths"] else "—"), ("caption", r["caption"] or "—"),
          ("prompt source", r["prompt_source"] or "—"), ("meta", html.escape(json.dumps(meta)) if meta else "—")]
    rows = "".join(f"<tr><td>{html.escape(k)}</td><td>{v if k in ('also at', 'meta') else html.escape(str(v))}</td></tr>" for k, v in kv)
    strip = f'<img class="strip" src="/strip/{r["sha12"]}.jpg" alt="" onerror="this.remove()">' if r["kind"] == "video" else ""
    body = (f'<div class="detail"><div class="nav"><a class="btn" href="/" onclick="if(history.length>1){{history.back();return false}}">‹ Back</a>'
            f'<a class="btn" href="/">⌂ Home</a><a class="btn" href="/?project={urllib.parse.quote(r["project"] or "")}">{html.escape(r["project"] or "project")}</a></div>'
            f'{media}{strip}<div><a class="btn" href="{src}" download>Download</a>'
            f'<a class="btn" href="javascript:navigator.clipboard.writeText(document.getElementById(\'pr\').innerText)">Copy prompt</a>'
            f'<a class="btn" href="/?project={urllib.parse.quote(r["project"] or "")}">More from {html.escape(r["project"] or "project")}</a></div>'
            f'<pre class="prompt" id="pr">{html.escape(r["prompt"] or "(no prompt recorded)")}</pre><table class="kv">{rows}</table></div>')
    return page(f"Spark Media · {r['sha12']}", body)


class Handler(BaseHTTPRequestHandler):
    server_version = "SparkMedia/1.0"

    def log_message(self, fmt, *args):  # quieter access log
        if "/thumb/" in (args[0] if args else "") or "/strip/" in (args[0] if args else ""):
            return
        log("%s %s" % (self.address_string(), fmt % args))

    def _send(self, code: int, body: bytes, ctype: str = "text/html; charset=utf-8", extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_file(self, path: Path, ctype: str, cache: bool = True):
        if not path.is_file():
            return self._send(404, b"not found", "text/plain")
        size = path.stat().st_size
        rng = self.headers.get("Range")
        start, end = 0, size - 1
        code = 200
        if rng and rng.startswith("bytes="):
            a, _, b = rng[6:].partition("-")
            try:
                start = int(a) if a else max(0, size - int(b))
                end = int(b) if (a and b) else end
            except ValueError:
                return self._send(416, b"", "text/plain", {"Content-Range": f"bytes */{size}"})
            if start >= size or start > end:
                return self._send(416, b"", "text/plain", {"Content-Range": f"bytes */{size}"})
            end = min(end, size - 1)
            code = 206
        length = end - start + 1
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        if cache:
            self.send_header("Cache-Control", "public, max-age=86400")
        if code == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if self.command == "HEAD":
            return
        with path.open("rb") as f:
            f.seek(start)
            left = length
            while left > 0:
                chunk = f.read(min(1 << 20, left))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    return
                left -= len(chunk)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        qs = urllib.parse.parse_qs(u.query)
        g = lambda k, d="": (qs.get(k) or [d])[0]  # noqa: E731
        p = u.path
        try:
            if p == "/health":
                return self._send(200, b"ok", "text/plain")
            if p.startswith("/thumb/") or p.startswith("/strip/"):
                name = os.path.basename(p)
                if not re.fullmatch(r"[0-9a-f]{12}\.jpg", name):
                    return self._send(404, b"", "text/plain")
                return self._send_file((THUMBS if p.startswith("/thumb/") else STRIPS) / name, "image/jpeg")
            if p.startswith("/media/"):
                name = os.path.basename(p)
                m = re.fullmatch(r"([0-9a-f]{12})(\.[a-z0-9]+)", name)
                if not m:
                    return self._send(404, b"", "text/plain")
                c = db()
                r = c.execute("SELECT store_path, ext FROM items WHERE sha12=?", (m.group(1),)).fetchone()
                c.close()
                if not r:
                    return self._send(404, b"", "text/plain")
                return self._send_file(Path(r["store_path"]), mimetypes.guess_type(r["store_path"])[0] or "application/octet-stream")
            c = db()
            try:
                if p == "/":
                    pg = max(1, int(g("page", "1") or 1))
                    return self._send(200, render_gallery(c, g("q"), g("kind"), g("project"), g("tier"), pg).encode())
                if p.startswith("/item/"):
                    out = render_item(c, int(p[6:]))
                    return self._send(200, out.encode()) if out else self._send(404, b"no such item", "text/plain")
                if p == "/api/items.json":
                    rows = [dict(r) for r in c.execute("SELECT id,sha12,kind,ext,project,tier,tags,prompt,caption,seed,steps,width,height,duration,created_at,elapsed_s,orig_path FROM items WHERE hidden=0 ORDER BY created_at DESC LIMIT ?", (int(g("limit", "200")),))]
                    return self._send(200, json.dumps(rows).encode(), "application/json")
                if p == "/api/stats":
                    st = {k: v for k, v in c.execute("SELECT kind, count(*) FROM items GROUP BY kind")}
                    st["bytes"] = c.execute("SELECT coalesce(sum(bytes),0) FROM items").fetchone()[0]
                    st["projects"] = c.execute("SELECT count(DISTINCT project) FROM items").fetchone()[0]
                    return self._send(200, json.dumps(st).encode(), "application/json")
            finally:
                c.close()
            return self._send(404, b"not found", "text/plain")
        except Exception as e:  # noqa: BLE001
            log(f"500 {p}: {e!r}")
            return self._send(500, f"error: {e!r}".encode(), "text/plain")

    def do_POST(self):
        u = urllib.parse.urlsplit(self.path)
        if self.client_address[0] not in ("127.0.0.1", "::1"):
            return self._send(403, b"local only", "text/plain")
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        try:
            if u.path == "/api/ingest":
                c = db()
                iid = ingest_file(c, Path(body["path"]), project=body.get("project", ""), tier=body.get("tier", ""), tags=body.get("tags", ""),
                                  caption=body.get("caption", ""), prompt=body.get("prompt", ""), force_meta=bool(body.get("force")))
                c.commit(); c.close()
                return self._send(200, json.dumps({"id": iid}).encode(), "application/json")
            if u.path == "/api/scan":
                return self._send(200, json.dumps(scan(full=bool(body.get("full")))).encode(), "application/json")
        except Exception as e:  # noqa: BLE001
            log(f"500 POST {u.path}: {e!r}")
            return self._send(500, f"error: {e!r}".encode(), "text/plain")
        return self._send(404, b"not found", "text/plain")


def serve(port: int) -> None:
    def loop():
        while True:
            try:
                scan()
            except Exception as e:  # noqa: BLE001
                log(f"periodic scan failed: {e!r}")
            time.sleep(SCAN_EVERY_S)
    threading.Thread(target=loop, daemon=True, name="scan").start()
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    srv.daemon_threads = True
    log(f"serving on :{port}, library {LIB}")
    srv.serve_forever()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve")
    s = sub.add_parser("scan"); s.add_argument("--full", action="store_true")
    i = sub.add_parser("ingest"); i.add_argument("file"); i.add_argument("--caption", default=""); i.add_argument("--prompt", default="")
    i.add_argument("--project", default=""); i.add_argument("--tier", default=""); i.add_argument("--tags", default=""); i.add_argument("--force", action="store_true")
    sub.add_parser("import-discord-log"); sub.add_parser("stats")
    a = ap.parse_args()
    if a.cmd == "serve":
        serve(int(os.environ.get("SPARK_MEDIA_PORT", "8120")))
    elif a.cmd == "scan":
        print(json.dumps(scan(full=a.full)))
    elif a.cmd == "ingest":
        c = db()
        iid = ingest_file(c, Path(a.file), project=a.project, tier=a.tier, tags=a.tags, caption=a.caption, prompt=a.prompt, force_meta=a.force)
        c.commit(); c.close()
        print(json.dumps({"id": iid}))
    elif a.cmd == "import-discord-log":
        print(json.dumps({"touched": import_discord_log()}))
    elif a.cmd == "stats":
        c = db()
        print(json.dumps({k: v for k, v in c.execute("SELECT kind, count(*) FROM items GROUP BY kind")} | {"bytes": c.execute("SELECT coalesce(sum(bytes),0) FROM items").fetchone()[0]}))


if __name__ == "__main__":
    main()
