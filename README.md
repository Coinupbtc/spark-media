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

## Make a film (2026-11-03)

Tap **＋** on clips (or photos) in the gallery — each one shows its number in your film (1, 2, 3, 4…) — then press
**▶ Make film** in the bar at the bottom. That's it: the clips are joined in the order you tapped with short crossfades,
a progress bar runs, then **▶ Play your film**. Tap a numbered clip (or its thumbnail in the bar) to take it out; the order
is the order you tap. **Fine-tune** (`/studio`) is there when you want trims (set in/out from the player), hard cuts,
end fades, a fixed output size, or mute; a finished film carries its recipe, so **✂ Fine-tune** on it reopens the edit.
Under the hood: each clip is normalised (trim → scale/letterbox to one size → 24 fps → stereo 48 kHz), joined with
`xfade`/`acrossfade` or `concat`, encoded with libx264 (CRF 18, faststart), and ingested as a library item (tier `compile`).
Clips longer than 15 s still come from chaining at render time (`h3-long-chain.sh`); films are stitched after.

Add a root: append `{"path": "~/...", "label": "name", "kinds": ["video","image"], "exclude": ["regex"]}` to
`config.json`; the next scan picks it up. Metadata precedence per clip: sidecar JSON → twin-test `.txt` → `prompt.txt` →
`gen.log` → Discord caption → filename prefix.

## Save to Photos on iPhone (2026-11-03)

Every item page has **📲 Save to Photos**: tap it, the iOS share sheet opens, tap **Save Video** / **Save Image**.
A web page can't write to the Photos library directly; the share sheet (`navigator.share` with the file) is the only way,
and Safari only allows it on a secure page. So serve the gallery over HTTPS as well and set `https_base` in `config.json`:

```bash
tailscale serve --bg --https=8121 http://127.0.0.1:8120   # tailnet only; undo: tailscale serve --https=8121 off
```

On the plain-http page the button opens the same item on `https_base`. The file is fetched when the page loads, so the
tap goes straight to the share sheet. Browsers that can't share files get a normal download. Photos takes the H.264 mp4s;
`.webm` and audio go to Files. The HTTPS proxy connects from 127.0.0.1, so the local-only endpoints (`/api/ingest`,
`/api/scan`) also refuse any request carrying `X-Forwarded-For` or `Tailscale-User-Login`.

## Cast — the same character, the same voice, every time (2026-11-03)

Prompt-only series drift: two "same cartoon reggae singer" clips rendered minutes apart came out as two different people
(different shirt, cap and stage). **Cast** (`/cast`) fixes that with two locks:

- **Look lock.** A character is created from one library clip (or photo): its middle frame becomes the character's
  reference frame. **🎥 Render shot** sends that frame as the first frame of a new MiniMax H3 render
  (`h3-image2video.sh`, first-frame I2V on the maximus pair), so the face, outfit and stage carry over. Proof: a 4 s shot
  from the singer's frame kept the same singer in every frame; render 136 s at 768×448.
- **Voice lock.** Each character gets a Fish Audio Drama 3 voice (plotarmordev/drama-tensorfold on node4, started on
  demand by `~/scripts/dgx/drama3.sh`, stopped after 5 idle minutes). The voice is bootstrapped from the model's own
  built-in voice with a seed, then encoded into the catalog (`drama3-voice-encode.sh`, provenance `model-invented`), so
  every later line is the same timbre. **🗣 Speak** makes a line (library item, tier `voice`, with a waveform thumbnail and
  player); **🎬 Put it on the clip** lays a line over a clip at a chosen second: the line is loudness-normalised to −16 LUFS,
  the clip's own sound ducks under it (sidechain compression), the video stream is copied byte-for-byte, and the result is
  a library item (tier `voiced`).
- **Every take is checked.** Drama's built-in voice sometimes returns half a second of sound and then silence (seed 7 and
  seed 23 did, reproducibly); a voice encoded from such a take is silent forever. `take_quality()` measures voiced seconds,
  the longest gap and the voiced share of every take (ffmpeg `silencedetect`); a rejected take is retried on the next seed
  (8 tries for a voice, 4 for a line) and the seed that worked is stored on the character. **↻ re-make the voice** on a
  character page rebuilds the voice from a new seed.

Everything runs as background jobs (`jobs.kind` = `voice-create` / `say` / `mix` / `shot`), with the same progress page as
films. API: `POST /api/cast {name, seed, look, ref_text}` · `POST /api/cast/<id>/say {text, seed}` ·
`POST /api/cast/<id>/mix {line_id, clip_id, at, lufs}` · `POST /api/cast/<id>/shot {prompt, duration, seed, width, height}` ·
`POST /api/cast/<id>/revoice {seed}`. Files: `~/media-library/cast/` (reference frames and takes, `lines/<name>/`,
`mixed/`, `shots/<name>/`); voices live on node4 in `~/Documents/projects/drama-tensorfold/voices/`.
Drama 3 weights are under the Fish Audio Community License (personal / small-commercial use; not a hosted service for
third parties).
