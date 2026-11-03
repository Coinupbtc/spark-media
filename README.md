# Spark Media — the house library of everything the DGX Sparks make

Discord was the archive for every H3 clip until 2026-11-03, when it went down and may not come back. This is the
self-sufficient replacement: one Python file, no third-party packages (Pillow for image thumbnails, ffmpeg for video),
SQLite, and a gallery that works on a phone.

- **Gallery:** http://sparkmax-10ef:8120 (tailnet/LAN; also on the Spark Console as *Spark Media*). Search prompts,
  captions and paths (SQLite FTS5); filter by Video / Photos, project, tier (`full`, `draft`, `chain`, `ab`, `proof`);
  item page with a byte-range video player, 5-frame strip, prompt (copy button), seed/steps/size/render time, and the
  original path(s).
- **Library on disk:** `~/media-library/` — `store/YYYY/MM/<sha12>-<name>` (hard-linked when on the same filesystem,
  else copied, so a deleted project folder no longer loses the clip), `thumbs/`, `strips/`, `library.sqlite`.
  Duplicates collapse by SHA-256; every path a file was seen at is kept.
- **Ingest paths:** (1) the scanner walks the roots in `config.json` every 15 min (and `spark_media.py scan`);
  (2) `~/.hermes/scripts/h3-video-gen.sh` writes a `<clip>.mp4.json` sidecar (prompt, size, duration, steps, seed,
  flow_shift, render time, tier) and ingests the clip the moment it is rendered; (3) `h3-deliver-clip.sh` ingests with
  the delivery caption **before** trying Discord/Telegram (which may fail without consequence);
  (4) `spark_media.py import-discord-log` recovers captions and prompts from `~/logs/discord/discord-media-send.log`
  for everything that was ever posted; (5) `curl -X POST localhost:8120/api/ingest -d '{"path":...}'` (loopback only).
- **Service:** `spark-media.service` (user systemd, `Restart=always`, `MemoryMax=2G`), `OnFailure` → `spark-media-alert.service`
  (dedupes 1 h, posts to #system-status). Log: `~/logs/spark-media.log`.

```bash
python3 -I spark_media.py serve            # the service
python3 -I spark_media.py scan [--full]    # ingest new files (--full re-reads sidecars/captions)
python3 -I spark_media.py ingest FILE --caption "..." --prompt "..." --project x --tier draft
python3 -I spark_media.py import-discord-log
python3 -I spark_media.py stats
```

## Studio — trim, order, stitch (2026-11-03)

Tap **☑ Select** in the gallery, tap clips (or photos), then **Open Studio** (`/studio`); or **＋ Add to Studio** on any item page.
In the Studio: reorder (↑ ↓), remove, set in/out points per clip (type seconds, or **▶ preview** and **set in / set out** from
the player's current time), photos become stills (default 3 s). Options: crossfade (default 0.4 s) or hard cut, fade in/out
at the ends, output size (auto = largest clip; everything is scaled/letterboxed to one size at 24 fps), mute. **⚙ Compile**
queues a job: each clip is normalised (trim → scale/pad → 24 fps → stereo 48 kHz), then joined with `xfade`/`acrossfade` or
`concat`, encoded with libx264 (CRF 18, faststart); the job page shows live progress; the result is ingested as a library item
(tier `compile`, project `studio`) with a JSON sidecar holding the full recipe, so **✂ Re-edit this compile** reopens it in the
Studio. Clips longer than 15 s still come from chaining at render time (`h3-long-chain.sh`); the Studio stitches anything after.

Add a root: append `{"path": "~/...", "label": "name", "kinds": ["video","image"], "exclude": ["regex"]}` to
`config.json`; the next scan picks it up. Metadata precedence per clip: sidecar JSON → twin-test `.txt` → `prompt.txt` →
`gen.log` → Discord caption → filename prefix.
