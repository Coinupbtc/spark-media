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
CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, created_at TEXT, status TEXT, progress REAL DEFAULT 0, spec TEXT, output TEXT DEFAULT '', item_id INTEGER, log TEXT DEFAULT '', error TEXT DEFAULT '');
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


# ----------------------------------------------------------------------------- studio: trim / order / stitch
COMPILES = LIB / "compiles"
_job_wake = threading.Event()


def has_audio(path: Path) -> bool:
    out = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "a:0", "-show_entries", "stream=codec_type", "-of", "csv=p=0", str(path)],
                         capture_output=True, text=True, timeout=30).stdout
    return "audio" in out


def _ff(cmd: list[str], timeout: int = 1800) -> None:
    r = subprocess.run(["ffmpeg", "-hide_banner", "-y", "-v", "error", *cmd], capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "ffmpeg failed").strip()[-800:])


def _ff_progress(cmd: list[str], total: float, cb, timeout: int = 3600) -> None:
    proc = subprocess.Popen(["ffmpeg", "-hide_banner", "-y", "-v", "error", "-progress", "pipe:1", "-nostats", *cmd],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    t0 = time.time()
    assert proc.stdout is not None
    for line in proc.stdout:
        if line.startswith("out_time_us=") or line.startswith("out_time_ms="):
            try:
                us = int(line.split("=", 1)[1])
                if total > 0:
                    cb(min(0.99, us / 1e6 / total))
            except ValueError:
                pass
        if time.time() - t0 > timeout:
            proc.kill()
            raise RuntimeError("ffmpeg timed out")
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr.read() if proc.stderr else "ffmpeg failed").strip()[-800:])


def compile_spec(jid: int, spec: dict, cb) -> tuple[Path, float, dict]:
    """spec = {title, clips:[{id, in, out, still}], transition: cut|fade, xfade, fade_ends, size: auto|WxH, mute}"""
    c = db()
    rows = {}
    for cl in spec["clips"]:
        r = c.execute("SELECT * FROM items WHERE id=?", (int(cl["id"]),)).fetchone()
        if not r:
            raise ValueError(f"item {cl['id']} not in the library")
        rows[int(cl["id"])] = r
    c.close()
    if not rows:
        raise ValueError("no clips")
    vids = [r for r in rows.values() if r["kind"] == "video" and r["width"]]
    if spec.get("size") and spec["size"] != "auto":
        W, H = (int(x) for x in spec["size"].lower().split("x"))
    elif vids:
        big = max(vids, key=lambda r: r["width"] * (r["height"] or 0))
        W, H = big["width"], big["height"]
    else:
        W, H = 1280, 704
    W, H = W - W % 2, H - H % 2
    fps = 24
    mute = bool(spec.get("mute"))
    tmp = COMPILES / f"job-{jid}"
    tmp.mkdir(parents=True, exist_ok=True)
    vf_norm = f"scale={W}:{H}:force_original_aspect_ratio=decrease,pad={W}:{H}:(ow-iw)/2:(oh-ih)/2,fps={fps},format=yuv420p"
    enc = ["-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2"]
    segs: list[tuple[Path, float]] = []
    n = len(spec["clips"])
    sources = []
    for i, cl in enumerate(spec["clips"]):
        r = rows[int(cl["id"])]
        src = Path(r["store_path"])
        seg = tmp / f"seg{i:02d}.mp4"
        if r["kind"] == "video":
            total = float(r["duration"] or 0)
            a = max(0.0, float(cl.get("in") or 0))
            b = float(cl.get("out") or total) if cl.get("out") else total
            b = min(b, total) if total else b
            if b - a < 0.2:
                raise ValueError(f"clip {r['id']}: out must be at least 0.2 s after in")
            dur = b - a
            vf = f"trim=start={a:.3f}:end={b:.3f},setpts=PTS-STARTPTS,{vf_norm}"
            if has_audio(src) and not mute:
                af = f"atrim=start={a:.3f}:end={b:.3f},asetpts=PTS-STARTPTS,aformat=sample_rates=48000:channel_layouts=stereo"
                _ff(["-i", str(src), "-filter_complex", f"[0:v]{vf}[v];[0:a]{af}[a]", "-map", "[v]", "-map", "[a]", "-t", f"{dur:.3f}", *enc, str(seg)])
            else:
                _ff(["-i", str(src), "-f", "lavfi", "-t", f"{dur:.3f}", "-i", "anullsrc=r=48000:cl=stereo", "-filter_complex", f"[0:v]{vf}[v]",
                     "-map", "[v]", "-map", "1:a", "-t", f"{dur:.3f}", *enc, str(seg)])
            sources.append({"id": r["id"], "sha12": r["sha12"], "in": a, "out": b, "orig_path": r["orig_path"]})
        else:
            dur = max(0.5, float(cl.get("still") or 3.0))
            _ff(["-loop", "1", "-t", f"{dur:.3f}", "-i", str(src), "-f", "lavfi", "-t", f"{dur:.3f}", "-i", "anullsrc=r=48000:cl=stereo",
                 "-filter_complex", f"[0:v]{vf_norm}[v]", "-map", "[v]", "-map", "1:a", "-t", f"{dur:.3f}", *enc, str(seg)])
            sources.append({"id": r["id"], "sha12": r["sha12"], "still": dur, "orig_path": r["orig_path"]})
        segs.append((seg, dur))
        cb(0.05 + 0.45 * (i + 1) / n)
    durs = [d for _, d in segs]
    fade = max(0.0, float(spec.get("xfade") or 0.4)) if spec.get("transition") == "fade" and len(segs) > 1 else 0.0
    fade = min(fade, min(durs) / 2.5)
    ends = max(0.0, float(spec.get("fade_ends") or 0))
    inputs = []
    for seg, _ in segs:
        inputs += ["-i", str(seg)]
    if fade > 0:
        total = sum(durs) - fade * (len(segs) - 1)
        vparts, aparts, acc = [], [], durs[0]
        prev_v, prev_a = "[0:v]", "[0:a]"
        for k in range(1, len(segs)):
            off = acc - fade
            vparts.append(f"{prev_v}[{k}:v]xfade=transition=fade:duration={fade:.3f}:offset={off:.3f}[v{k}]")
            aparts.append(f"{prev_a}[{k}:a]acrossfade=d={fade:.3f}[a{k}]")
            prev_v, prev_a = f"[v{k}]", f"[a{k}]"
            acc = acc + durs[k] - fade
        vout, aout = prev_v, prev_a
        graph = ";".join(vparts + aparts)
    else:
        total = sum(durs)
        if len(segs) > 1:
            graph = "".join(f"[{k}:v][{k}:a]" for k in range(len(segs))) + f"concat=n={len(segs)}:v=1:a=1[vc][ac]"
            vout, aout = "[vc]", "[ac]"
        else:
            graph, vout, aout = "", "[0:v]", "[0:a]"
    ends = min(ends, total / 3)
    if ends > 0:
        tail = f"{vout}fade=t=in:st=0:d={ends:.3f},fade=t=out:st={max(0.0, total - ends):.3f}:d={ends:.3f}[vf];{aout}afade=t=in:st=0:d={ends:.3f},afade=t=out:st={max(0.0, total - ends):.3f}:d={ends:.3f}[af]"
        graph = (graph + ";" if graph else "") + tail
        vout, aout = "[vf]", "[af]"
    if graph == "":
        graph = f"{vout}null[vf];{aout}anull[af]"
        vout, aout = "[vf]", "[af]"
    title = (spec.get("title") or "compile").strip()[:80]
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", title).strip("_") or "compile"
    out = COMPILES / f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}-{slug}.mp4"
    _ff_progress([*inputs, "-filter_complex", graph, "-map", vout, "-map", aout, "-c:v", "libx264", "-preset", "medium", "-crf", "18",
                  "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", str(out)], total, lambda f: cb(0.5 + 0.5 * f))
    shutil.rmtree(tmp, ignore_errors=True)
    side = {"tool": "spark-media studio", "tier": "compile", "title": title, "spec": spec, "sources": sources, "duration": round(total, 3),
            "width": W, "height": H, "fps": fps, "created": dt.datetime.now().isoformat(timespec="seconds"),
            "prompt": f"Compiled from {len(sources)} clips: " + " + ".join(f"#{x['id']}" for x in sources)}
    (out.parent / (out.name + ".json")).write_text(json.dumps(side, indent=1))
    return out, total, side


def jobs_loop() -> None:
    while True:
        _job_wake.wait(timeout=2.0)
        _job_wake.clear()
        c = db()
        job = c.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
        c.close()
        if not job:
            continue
        jid = job["id"]

        def cb(f: float, jid=jid):
            cj = db()
            cj.execute("UPDATE jobs SET progress=? WHERE id=?", (round(float(f), 3), jid))
            cj.commit(); cj.close()

        c = db(); c.execute("UPDATE jobs SET status='running', progress=0.02 WHERE id=?", (jid,)); c.commit(); c.close()
        try:
            spec = json.loads(job["spec"])
            out, total, side = compile_spec(jid, spec, cb)
            c = db()
            iid = ingest_file(c, out, project="studio", tier="compile", caption=side["title"], prompt=side["prompt"])
            c.execute("UPDATE jobs SET status='done', progress=1, output=?, item_id=? WHERE id=?", (str(out), iid, jid))
            c.commit(); c.close()
            log(f"job {jid} done: {out} ({total:.1f} s) -> item {iid}")
        except Exception as e:  # noqa: BLE001
            log(f"job {jid} failed: {e!r}")
            c = db(); c.execute("UPDATE jobs SET status='failed', error=? WHERE id=?", (str(e)[-800:], jid)); c.commit(); c.close()


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
.card.sel{outline:3px solid var(--acc)}.card .add{position:absolute;bottom:50px;right:8px;width:38px;height:38px;border-radius:50%;background:#000c;border:2px solid #fff;color:#fff;font-size:22px;line-height:1;display:flex;align-items:center;justify-content:center;cursor:pointer;padding:0}.card.sel .add{background:var(--acc);color:#000;border-color:var(--acc);font-weight:700;font-size:18px}
#tray .film{display:flex;gap:6px;overflow-x:auto;padding-bottom:8px}#tray .film img{width:72px;aspect-ratio:16/9;object-fit:cover;border-radius:6px;border:2px solid var(--acc)}#tray .film .f{position:relative;flex:none}#tray .film .f b{position:absolute;top:2px;left:4px;background:var(--acc);color:#000;border-radius:6px;padding:0 5px;font-size:12px}#tray .film .f .x{position:absolute;top:-6px;right:-6px;background:#000;border:1px solid #fff;color:#fff;border-radius:50%;width:20px;height:20px;font-size:12px;display:flex;align-items:center;justify-content:center}
#tray .tb{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.btn.big{font-size:18px;padding:12px 18px}
#tray{position:fixed;left:0;right:0;bottom:0;background:#171a21f2;border-top:1px solid #2a2f3a;padding:10px 14px calc(10px + env(safe-area-inset-bottom));display:none;flex-direction:column;z-index:9}#tray.on{display:flex}#tray .n{flex:1;color:var(--mut);font-size:13px}body.hastray .grid{padding-bottom:170px}
.btn.primary{background:var(--acc);color:#000;font-weight:600}.btn.danger{background:#3a1d1d}
.rows{display:flex;flex-direction:column;gap:8px;padding:0 0 100px}.row{display:grid;grid-template-columns:96px 1fr;gap:10px;background:var(--card);border-radius:10px;padding:8px}.row img{width:96px;aspect-ratio:16/9;object-fit:cover;border-radius:6px;background:#000}
.row .ttl{font-size:13px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.row .ctl{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-top:6px}.row input[type=number]{width:72px;background:#0b0d11;color:var(--fg);border:1px solid #2a2f3a;border-radius:6px;padding:6px;font-size:15px}
.sbtn{padding:7px 10px;border-radius:8px;background:#1d2130;color:var(--fg);border:0;font-size:14px}.sbtn:active{background:#2a3044}
.opts{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;background:var(--card);border-radius:10px;padding:12px;margin:10px 0}.opts label{display:flex;flex-direction:column;gap:4px;font-size:13px;color:var(--mut)}.opts input,.opts select{background:#0b0d11;color:var(--fg);border:1px solid #2a2f3a;border-radius:6px;padding:8px;font-size:15px}
.bar{height:14px;background:#0b0d11;border-radius:999px;overflow:hidden;margin:10px 0}.bar i{display:block;height:100%;background:var(--acc);width:0;transition:width .4s}
#pv{width:100%;max-height:50vh;background:#000;border-radius:10px;margin-bottom:10px}
"""


TRAY_JS = r"""
const SM={key:'sm.film',get(){try{const a=JSON.parse(localStorage.getItem(this.key)||'[]');return a.filter(x=>x&&x.id)}catch(e){return[]}},set(a){localStorage.setItem(this.key,JSON.stringify(a));this.paint()},
 has(id){return this.get().some(x=>x.id===id)},add(id,sha){if(!this.has(id)){const a=this.get();a.push({id,sha});this.set(a)}},rm(id){this.set(this.get().filter(x=>x.id!==id))},
 toggle(id,sha){this.has(id)?this.rm(id):this.add(id,sha)},ids(){return this.get().map(x=>x.id)},
 paint(){const a=this.get();const t=document.getElementById('tray');if(!t)return;document.getElementById('trayN').textContent=a.length;t.classList.toggle('on',a.length>0);document.body.classList.toggle('hastray',a.length>0);
  document.getElementById('trayGo').href='/studio?ids='+this.ids().join(',');
  const f=document.getElementById('film');f.innerHTML=a.map((x,i)=>`<div class="f" data-id="${x.id}"><img src="/thumb/${x.sha}.jpg" alt=""><b>${i+1}</b><span class="x">✕</span></div>`).join('');
  f.querySelectorAll('.f').forEach(el=>el.onclick=()=>SM.rm(+el.dataset.id));
  document.querySelectorAll('.card[data-id]').forEach(c=>{const id=+c.dataset.id;const i=a.findIndex(x=>x.id===id);c.classList.toggle('sel',i>=0);const b=c.querySelector('.add');if(b)b.textContent=i>=0?String(i+1):'＋'})}};
async function makeFilm(){const ids=SM.ids();if(!ids.length)return;const d=new Date();const title='Film '+d.toISOString().slice(0,10)+' '+d.toTimeString().slice(0,5);
 const spec={title,transition:'fade',xfade:0.4,fade_ends:0.3,size:'auto',mute:false,clips:ids.map(id=>({id}))};const b=document.getElementById('trayMake');b.textContent='…';
 try{const r=await fetch('/api/compile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(spec)});const j=await r.json();if(j.job){SM.set([]);location.href='/job/'+j.job}else{alert(j.error||'failed');b.textContent='▶ Make film'}}catch(e){alert('failed: '+e);b.textContent='▶ Make film'}}
document.addEventListener('DOMContentLoaded',()=>{SM.paint();const tc=document.getElementById('trayClear');if(tc)tc.onclick=e=>{e.preventDefault();SM.set([])};const mk=document.getElementById('trayMake');if(mk)mk.onclick=e=>{e.preventDefault();makeFilm()};
 document.querySelectorAll('.card[data-id] .add').forEach(b=>b.addEventListener('click',e=>{e.preventDefault();e.stopPropagation();const c=b.closest('.card');SM.toggle(+c.dataset.id,c.dataset.sha)}));
 document.querySelectorAll('[data-addsel]').forEach(b=>b.addEventListener('click',e=>{e.preventDefault();const id=+b.dataset.addsel;SM.add(id,b.dataset.sha);b.textContent='✓ In your film ('+SM.get().length+')'}));});
"""


def page(title: str, body: str, q: str = "", kind: str = "", project: str = "", tier: str = "", projects: list[str] | None = None, stats: str = "") -> str:
    qs = lambda **kw: "?" + urllib.parse.urlencode({k: v for k, v in {"q": q, "kind": kind, "project": project, "tier": tier, **kw}.items() if v})  # noqa: E731
    chips = "".join(f'<a class="chip{" on" if kind == k else ""}" href="{qs(kind=k)}">{lbl}</a>' for k, lbl in (("", "All"), ("video", "Video"), ("image", "Photos")))
    chips += "".join(f'<a class="chip{" on" if tier == t else ""}" href="{qs(tier=t)}">{t}</a>' for t in ("full", "draft", "chain", "ab", "proof", "compile"))
    chips += '<a class="chip" href="/studio" title="trim and fine-tune">✂ Fine-tune</a>'
    opts = "".join(f'<option value="{html.escape(p)}"{" selected" if p == project else ""}>{html.escape(p)}</option>' for p in (projects or []))
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>{html.escape(title)}</title><style>{CSS}</style></head><body>
<header><h1><a href="/">Spark Media</a></h1><a class="chip home" href="/">⌂ Home</a><form method="get" action="/" style="display:contents">
<input type="search" name="q" placeholder="search prompts, captions, paths" value="{html.escape(q)}">
<select name="project" onchange="this.form.submit()"><option value="">all projects</option>{opts}</select>
<input type="hidden" name="kind" value="{html.escape(kind)}"><input type="hidden" name="tier" value="{html.escape(tier)}"></form>
<div class="chips">{chips}</div></header><div class="stats">{stats}</div>{body}<div id="tray"><div class="film" id="film"></div><div class="tb"><span class="n">Your film: <b id="trayN">0</b> clips, in the order you tapped</span><a class="btn primary big" id="trayMake" href="#">▶ Make film</a><a class="btn" id="trayGo" href="/studio">Fine-tune</a><a class="btn" href="#" id="trayClear">Clear</a></div></div><script>{TRAY_JS}</script></body></html>"""


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
        cards.append(f'<a class="card" data-id="{r["id"]}" data-sha="{r["sha12"]}" href="/item/{r["id"]}"><img loading="lazy" src="/thumb/{r["sha12"]}.jpg" alt=""><span class="b">{badge}</span><button class="add" type="button" aria-label="add to film">＋</button>'
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
            f'<a class="btn" href="/?project={urllib.parse.quote(r["project"] or "")}">More from {html.escape(r["project"] or "project")}</a>'
            f'<a class="btn primary" href="#" data-addsel="{r["id"]}" data-sha="{r["sha12"]}">＋ Add to film</a>'
            + (f'<a class="btn" href="/studio?from={r["id"]}">✂ Re-edit this compile</a>' if r["tier"] == "compile" and meta.get("spec") else "") + '</div>'
            f'<pre class="prompt" id="pr">{html.escape(r["prompt"] or "(no prompt recorded)")}</pre><table class="kv">{rows}</table></div>')
    return page(f"Spark Media · {r['sha12']}", body)


STUDIO_JS = r"""
const q=new URLSearchParams(location.search);let clips=[];let opts={title:'',transition:'fade',xfade:0.4,fade_ends:0.3,size:'auto',mute:false};
async function load(){let ids=(q.get('ids')||'').split(',').filter(Boolean).map(Number);const from=q.get('from');
 if(from){const r=await fetch('/api/items/by-ids?ids='+from);const it=(await r.json())[0];const sp=it&&it.meta&&it.meta.spec;if(sp){clips=sp.clips.map(c=>({...c}));Object.assign(opts,{title:sp.title||'',transition:sp.transition||'fade',xfade:sp.xfade||0.4,fade_ends:sp.fade_ends||0,size:sp.size||'auto',mute:!!sp.mute});ids=clips.map(c=>c.id)}}
 if(!ids.length)ids=SM.ids();if(!clips.length)clips=ids.map(id=>({id}));
 if(!ids.length){document.getElementById('rows').innerHTML='<p>No clips yet. Go <a href="/">Home</a>, tap <b>＋</b> on clips, then <b>▶ Make film</b> — or <b>Fine-tune</b> to come here.</p>';return}
 const r=await fetch('/api/items/by-ids?ids='+ids.join(','));const items=await r.json();const by={};items.forEach(i=>by[i.id]=i);clips=clips.filter(c=>by[c.id]).map(c=>({...c,item:by[c.id]}));paint()}
function fmt(x){return x==null||x===''?'':(+x).toFixed(1)}
function paint(){const o=document.getElementById('rows');o.innerHTML='';clips.forEach((c,i)=>{const it=c.item;const vid=it.kind==='video';const d=document.createElement('div');d.className='row';
 d.innerHTML=`<img src="/thumb/${it.sha12}.jpg"><div><div class="ttl">#${it.id} · ${vid?(it.duration||0).toFixed(1)+' s':'photo'} · ${(it.prompt||it.caption||'').slice(0,80)}</div>
 <div class="ctl">${vid?`in <input type=number step=0.1 min=0 value="${fmt(c.in)}" data-k=in placeholder=0> out <input type=number step=0.1 min=0 value="${fmt(c.out)}" data-k=out placeholder="${(it.duration||0).toFixed(1)}">
 <button class=sbtn data-a=pv>▶ preview</button><button class=sbtn data-a=setin>set in</button><button class=sbtn data-a=setout>set out</button>`:`still <input type=number step=0.5 min=0.5 value="${fmt(c.still||3)}" data-k=still> s`}
 <button class=sbtn data-a=up>↑</button><button class=sbtn data-a=down>↓</button><button class=sbtn data-a=rm>✕</button></div></div>`;
 d.querySelectorAll('input').forEach(inp=>inp.onchange=()=>{c[inp.dataset.k]=inp.value===''?null:+inp.value});
 d.querySelectorAll('button').forEach(b=>b.onclick=()=>{const a=b.dataset.a;const pv=document.getElementById('pv');
  if(a==='up'&&i>0){[clips[i-1],clips[i]]=[clips[i],clips[i-1]];paint()}else if(a==='down'&&i<clips.length-1){[clips[i+1],clips[i]]=[clips[i],clips[i+1]];paint()}
  else if(a==='rm'){clips.splice(i,1);paint()}else if(a==='pv'){pv.src='/media/'+it.sha12+it.ext;pv.currentTime=c.in||0;pv.play();pv.dataset.i=i}
  else if(a==='setin'||a==='setout'){if(+pv.dataset.i!==i){pv.src='/media/'+it.sha12+it.ext;pv.dataset.i=i;return}c[a==='setin'?'in':'out']=+pv.currentTime.toFixed(2);paint()}});
 o.appendChild(d)});
 const sum=clips.reduce((s,c)=>s+(c.item.kind==='video'?Math.max(0,(c.out||c.item.duration||0)-(c.in||0)):(c.still||3)),0);
 document.getElementById('total').textContent=clips.length+' clips · ~'+sum.toFixed(1)+' s'+(opts.transition==='fade'&&clips.length>1?' (minus '+((clips.length-1)*opts.xfade).toFixed(1)+' s of crossfades)':'');
 ['title','transition','xfade','fade_ends','size'].forEach(k=>{const e=document.getElementById('o_'+k);if(e)e.value=opts[k]});document.getElementById('o_mute').checked=opts.mute}
document.addEventListener('DOMContentLoaded',()=>{load();['title','transition','xfade','fade_ends','size'].forEach(k=>document.getElementById('o_'+k).onchange=e=>{opts[k]=e.target.value;if(k!=='title'&&k!=='size'&&k!=='transition')opts[k]=+opts[k];paint()});
 document.getElementById('o_mute').onchange=e=>opts.mute=e.target.checked;
 document.getElementById('go').onclick=async()=>{if(!clips.length)return;const spec={...opts,clips:clips.map(c=>({id:c.id,in:c.in||null,out:c.out||null,still:c.still||null}))};
  const r=await fetch('/api/compile',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(spec)});const j=await r.json();if(j.job){SM.set([]);location.href='/job/'+j.job}else alert(j.error||'failed')}});
"""

JOB_JS = r"""
const jid=location.pathname.split('/').pop();async function tick(){const r=await fetch('/api/job/'+jid);const j=await r.json();
 document.getElementById('st').textContent=j.status+(j.error?' — '+j.error:'');document.querySelector('.bar i').style.width=Math.round((j.progress||0)*100)+'%';
 document.getElementById('pct').textContent=Math.round((j.progress||0)*100)+'%';if(j.status==='done'&&j.item_id){document.getElementById('done').innerHTML='<a class="btn primary big" href="/item/'+j.item_id+'">▶ Play your film</a> <a class="btn" href="/">＋ Make another</a> <a class="btn" href="/studio?from='+j.item_id+'">✂ Fine-tune</a>';return}
 if(j.status==='failed')return;setTimeout(tick,1000)}document.addEventListener('DOMContentLoaded',tick);
"""


def render_studio() -> str:
    body = ('<div class="detail"><div class="nav"><a class="btn" href="/">⌂ Home</a><span class="btn" id="total">…</span></div>'
            '<video id="pv" controls playsinline preload="metadata"></video><div class="rows" id="rows">loading…</div>'
            '<div class="opts"><label>Title<input id="o_title" placeholder="my cut"></label>'
            '<label>Transition<select id="o_transition"><option value="fade">crossfade</option><option value="cut">hard cut</option></select></label>'
            '<label>Crossfade (s)<input id="o_xfade" type="number" step="0.1" min="0.1" max="2"></label>'
            '<label>Fade in/out at ends (s)<input id="o_fade_ends" type="number" step="0.1" min="0" max="3"></label>'
            '<label>Output size<select id="o_size"><option value="auto">auto (largest clip)</option><option value="768x448">768×448</option><option value="1280x704">1280×704</option><option value="1920x1080">1920×1080</option></select></label>'
            '<label>Audio<span><input id="o_mute" type="checkbox"> mute all</span></label></div>'
            '<div style="padding-bottom:90px"><a class="btn primary" href="#" id="go" style="font-size:17px;padding:12px 18px">⚙ Compile</a> '
            '<span style="color:var(--mut);font-size:13px">clips are trimmed, scaled to one size at 24 fps, joined, and the result lands in the library (tier compile)</span></div></div>'
            f"<script>{STUDIO_JS}</script>")
    return page("Spark Media · Studio", body)


def render_job(c: sqlite3.Connection, jid: int) -> str | None:
    j = c.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if not j:
        return None
    spec = json.loads(j["spec"] or "{}")
    body = (f'<div class="detail"><div class="nav"><a class="btn" href="/">⌂ Home</a><a class="btn" href="/studio">✂ Studio</a></div>'
            f'<h2 style="margin:6px 0">Making your film…</h2><p style="color:var(--mut)">{html.escape(spec.get("title") or "")}</p><p>status: <b id="st">{html.escape(j["status"])}</b> · <span id="pct">0%</span></p>'
            f'<div class="bar"><i></i></div><div id="done"></div><p style="color:var(--mut);font-size:13px">{len(spec.get("clips", []))} clips · {html.escape(spec.get("transition", ""))}</p></div>'
            f"<script>{JOB_JS}</script>")
    return page(f"Spark Media · compile #{jid}", body)


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
                if p == "/studio":
                    return self._send(200, render_studio().encode())
                if p.startswith("/job/"):
                    out = render_job(c, int(p[5:]))
                    return self._send(200, out.encode()) if out else self._send(404, b"no such job", "text/plain")
                if p == "/api/job":
                    pass
                if p.startswith("/api/job/"):
                    j = c.execute("SELECT id,status,progress,output,item_id,error,created_at FROM jobs WHERE id=?", (int(p[9:]),)).fetchone()
                    return self._send(200, json.dumps(dict(j) if j else {"error": "no such job"}).encode(), "application/json")
                if p == "/api/items/by-ids":
                    ids = [int(x) for x in re.findall(r"\d+", g("ids"))][:200]
                    rows = []
                    for i in ids:
                        r = c.execute("SELECT id,sha12,kind,ext,project,tier,prompt,caption,width,height,duration,created_at,meta FROM items WHERE id=?", (i,)).fetchone()
                        if r:
                            d = dict(r); d["meta"] = json.loads(d.get("meta") or "{}"); rows.append(d)
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
        n = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(min(n, 1 << 20)) or b"{}") if n else {}
        except json.JSONDecodeError:
            return self._send(400, b"bad json", "text/plain")
        if u.path == "/api/compile":
            try:
                clips = [c for c in body.get("clips", []) if isinstance(c, dict) and str(c.get("id", "")).isdigit()]
                if not clips or len(clips) > 60:
                    return self._send(400, json.dumps({"error": "1-60 clips"}).encode(), "application/json")
                spec = {"title": str(body.get("title") or "")[:80], "clips": [{"id": int(c["id"]), "in": c.get("in"), "out": c.get("out"), "still": c.get("still")} for c in clips],
                        "transition": "fade" if body.get("transition") == "fade" else "cut", "xfade": float(body.get("xfade") or 0.4),
                        "fade_ends": float(body.get("fade_ends") or 0), "size": str(body.get("size") or "auto")[:12], "mute": bool(body.get("mute"))}
                c = db()
                cur = c.execute("INSERT INTO jobs(created_at,status,spec) VALUES (?,?,?)", (dt.datetime.now().isoformat(timespec="seconds"), "queued", json.dumps(spec)))
                c.commit(); jid = cur.lastrowid; c.close()
                _job_wake.set()
                return self._send(200, json.dumps({"job": jid}).encode(), "application/json")
            except Exception as e:  # noqa: BLE001
                return self._send(500, json.dumps({"error": repr(e)}).encode(), "application/json")
        if self.client_address[0] not in ("127.0.0.1", "::1"):
            return self._send(403, b"local only", "text/plain")
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
    threading.Thread(target=jobs_loop, daemon=True, name="jobs").start()
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
