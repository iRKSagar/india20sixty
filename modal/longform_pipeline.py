import modal
import os
import uuid
import traceback
import requests
import subprocess
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP — LONGFORM PIPELINE
# Web endpoints called by api.worker.js.
# Orchestrates: script → images → voice → render → publish
#
# Endpoints:
#   POST /generate-script         — step 1: create segment scripts
#   POST /generate-segment-voice  — generate AI voice for one segment
#   POST /generate-segment-images — generate images for one segment
#   POST /render-full             — final render + publish
# ==========================================

app = modal.App("india20sixty-longform")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR = "/tmp/india20sixty-longform"

# Cross-app function references — resolved at runtime via Function.lookup
# This avoids Modal trying to find .app on imported modules at deploy time.
def _lf_script():
    return modal.Function.lookup("india20sixty-longform-scriptwriter", "generate_longform_script")

def _lf_renderer():
    return modal.Function.lookup("india20sixty-longform-renderer", "render_longform")

def _image_gen():
    return modal.Function.lookup("india20sixty-images", "generate_single_image")

def _voice_gen():
    return modal.Function.lookup("india20sixty-voice", "generate_voice")

def _r2_upload():
    return modal.Function.lookup("india20sixty-publisher", "upload_to_r2")

def _yt_upload():
    return modal.Function.lookup("india20sixty-publisher", "upload_to_youtube")

def _title_gen():
    return modal.Function.lookup("india20sixty-publisher", "generate_title")

def _research():
    return modal.Function.lookup("india20sixty-research", "run_research")


# ── HELPER ─────────────────────────────────────────────────────
def _sb(env_key="SUPABASE_URL"):
    return os.environ[env_key]

def _sbh():
    return {
        "apikey":        os.environ["SUPABASE_ANON_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_ANON_KEY']}",
        "Content-Type":  "application/json",
    }

def sb_get(ep):
    r = requests.get(f"{_sb()}/rest/v1/{ep}", headers=_sbh(), timeout=10)
    return r.json() if r.ok else []

def sb_patch(ep, data):
    requests.patch(f"{_sb()}/rest/v1/{ep}", headers={**_sbh(),"Prefer":"return=minimal"},
                   json=data, timeout=10)

def sb_insert(table, data):
    r = requests.post(f"{_sb()}/rest/v1/{table}",
                      headers={**_sbh(),"Prefer":"return=representation"},
                      json=data, timeout=10)
    if not r.ok: raise Exception(f"INSERT {r.status_code}: {r.text[:200]}")
    return r.json()[0]

def log(job_id, msg):
    print(f"[longform] {msg}")
    try:
        requests.post(f"{_sb()}/rest/v1/render_logs", headers=_sbh(),
                      json={"job_id": job_id, "message": str(msg)[:500]}, timeout=5)
    except Exception:
        pass

def ping_worker(job_id, segment_idx, event, payload):
    """Send event back to Cloudflare Worker via /longform/webhook."""
    worker_url = os.environ.get("WORKER_URL", "")
    if not worker_url:
        return
    try:
        requests.post(
            worker_url.rstrip("/") + "/longform/webhook",
            headers={"Content-Type": "application/json"},
            json={"job_id": job_id, "segment_idx": segment_idx, "event": event, "payload": payload},
            timeout=10,
        )
    except Exception as e:
        print(f"  Webhook ping failed (non-fatal): {e}")


# ==========================================
# ENDPOINT 1: GENERATE SCRIPT
# Called by Cloudflare when a long-form job is created.
# Runs research + scriptwriter, creates segment rows.
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=180)
@modal.fastapi_endpoint(method="POST")
def generate_script(data: dict):
    job_id   = data.get("job_id")
    topic    = data.get("topic", "Future India")
    cluster  = data.get("cluster", "Space")
    dur_secs = int(data.get("target_duration", 420))

    if not job_id:
        return {"status": "error", "message": "Missing job_id"}

    print(f"\n[LF Generate Script] job={job_id} topic={topic[:60]}")
    sb_patch(f"longform_jobs?id=eq.{job_id}",
             {"status": "scripting", "updated_at": datetime.utcnow().isoformat()})

    try:
        # Research for fact anchor
        fact_package = _research().remote(job_id, topic)

        # Generate structured script
        result = _lf_script().remote(
            job_id=job_id, topic=topic, cluster=cluster,
            target_duration=dur_secs, fact_package=fact_package,
        )

        # Save mood to job
        sb_patch(f"longform_jobs?id=eq.{job_id}", {
            "mood": result["mood"], "updated_at": datetime.utcnow().isoformat()
        })

        # Ping worker to create segment rows
        ping_worker(job_id, None, "script_ready", {
            "segments": [{
                "segment_idx":    s["segment_idx"],
                "type":           s["type"],
                "label":          s["label"],
                "script":         s["script"],
                "duration_target":s["duration_target"],
                "image_prompts":  s["image_prompts"],
            } for s in result["segments"]],
        })

        log(job_id, f"Script done: {len(result['segments'])} segments, mood={result['mood']}")
        return {"status": "script_ready", "job_id": job_id, "segments": len(result["segments"])}

    except Exception as e:
        msg = str(e)
        print(f"  Script failed: {msg}")
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status": "failed", "error": msg[:400],
                  "updated_at": datetime.utcnow().isoformat()})
        return {"status": "error", "message": msg}


# ==========================================
# ENDPOINT 2: GENERATE SEGMENT AI VOICE
# Called per segment when AI voice is requested.
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=120)
@modal.fastapi_endpoint(method="POST")
def generate_segment_voice(data: dict):
    job_id      = data.get("job_id")
    segment_idx = data.get("segment_idx")

    if not job_id or segment_idx is None:
        return {"status": "error", "message": "Missing job_id or segment_idx"}

    print(f"\n[LF Segment Voice] job={job_id} seg={segment_idx}")

    try:
        segs = sb_get(f"longform_segments?job_id=eq.{job_id}&segment_idx=eq.{segment_idx}&select=script,duration_target")
        if not segs or not segs[0].get("script"):
            return {"status": "error", "message": "No script for this segment"}

        script = segs[0]["script"]

        # Generate voice via voice worker
        voice_result = _voice_gen().remote(
            job_id=f"{job_id}_seg{segment_idx}",
            reviewed_script=script,
        )
        audio_path = voice_result["audio_path"]
        audio_dur  = voice_result["duration"]

        # Upload audio to R2
        r2_key     = f"longform/{job_id}/seg{segment_idx}_voice.mp3"
        voice_url  = _r2_upload().remote(audio_path, r2_key)

        try: os.remove(audio_path)
        except Exception: pass

        log(job_id, f"Seg {segment_idx} voice: {audio_dur:.1f}s → {r2_key}")

        # Ping worker
        ping_worker(job_id, segment_idx, "segment_voice_ready", {
            "voice_r2_url":  r2_key,
            "voice_pub_url": voice_url,
            "duration":      audio_dur,
        })

        return {"status": "voice_ready", "job_id": job_id, "segment_idx": segment_idx,
                "duration": audio_dur, "r2_key": r2_key}

    except Exception as e:
        msg = str(e)[:400]
        print(f"  Segment voice failed: {msg}")
        sb_patch(f"longform_segments?job_id=eq.{job_id}&segment_idx=eq.{segment_idx}",
                 {"status": "voice_failed", "updated_at": datetime.utcnow().isoformat()})
        return {"status": "error", "message": msg}


# ==========================================
# ENDPOINT 3: GENERATE SEGMENT IMAGES
# Triggers parallel image generation for one segment.
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=300)
@modal.fastapi_endpoint(method="POST")
def generate_segment_images(data: dict):
    job_id      = data.get("job_id")
    segment_idx = data.get("segment_idx")

    if not job_id or segment_idx is None:
        return {"status": "error", "message": "Missing fields"}

    print(f"\n[LF Segment Images] job={job_id} seg={segment_idx}")

    try:
        segs = sb_get(
            f"longform_segments?job_id=eq.{job_id}&segment_idx=eq.{segment_idx}"
            "&select=script,image_prompts,duration_target"
        )
        if not segs:
            return {"status": "error", "message": "Segment not found"}

        seg           = segs[0]
        image_prompts = seg.get("image_prompts") or []
        if not image_prompts:
            return {"status": "error", "message": "No image prompts — run script generation first"}

        # Generate images in parallel
        futures = [
            _image_gen().spawn(
                prompt=prompt, scene_idx=i,
                job_id=f"{job_id}_seg{segment_idx}",
            )
            for i, prompt in enumerate(image_prompts)
        ]
        results = [f.get() for f in futures]

        # Upload successful images to R2
        media = []
        for res in results:
            if res["success"] and res["local_path"]:
                r2_key    = f"longform/{job_id}/seg{segment_idx}_img{res['scene_idx']}.png"
                public_url = _r2_upload().remote(res["local_path"], r2_key)
                media.append({
                    "type":       "image",
                    "r2_url":     r2_key,
                    "public_url": public_url,
                    "order":      res["scene_idx"],
                })
                try: os.remove(res["local_path"])
                except Exception: pass

        log(job_id, f"Seg {segment_idx} images: {len(media)}/{len(image_prompts)} uploaded")

        # Ping worker
        ping_worker(job_id, segment_idx, "segment_images_ready", {"media": media})

        return {"status": "images_ready", "job_id": job_id, "segment_idx": segment_idx,
                "images_count": len(media)}

    except Exception as e:
        msg = str(e)[:400]
        sb_patch(f"longform_segments?job_id=eq.{job_id}&segment_idx=eq.{segment_idx}",
                 {"status": "image_failed", "updated_at": datetime.utcnow().isoformat()})
        return {"status": "error", "message": msg}


# ==========================================
# ENDPOINT 4: RENDER FULL VIDEO
# Called when all segments are ready.
# Downloads all media + audio, renders, uploads, publishes.
# ==========================================

@app.function(image=image, secrets=secrets, cpu=4.0, memory=8192, timeout=1800)
@modal.fastapi_endpoint(method="POST")
def render_full(data: dict):
    job_id     = data.get("job_id")
    publish_at = data.get("publish_at")

    if not job_id:
        return {"status": "error", "message": "Missing job_id"}

    WORKER_URL  = os.environ.get("WORKER_URL", "")
    TEST_MODE   = os.environ.get("TEST_MODE", "true").lower() == "true"
    R2_BASE_URL = os.environ.get("R2_BASE_URL", "").rstrip("/")

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    print(f"\n[LF Render Full] job={job_id}")
    sb_patch(f"longform_jobs?id=eq.{job_id}",
             {"status": "rendering", "updated_at": datetime.utcnow().isoformat()})

    try:
        # Fetch job + segments
        jobs = sb_get(f"longform_jobs?id=eq.{job_id}&select=id,topic,cluster,mood,target_duration")
        if not jobs:
            raise Exception("Job not found")
        job = jobs[0]

        segments_data = sb_get(
            f"longform_segments?job_id=eq.{job_id}&order=segment_idx.asc"
            "&select=segment_idx,type,label,script,media,voice_r2_url,voice_source,"
            "caption_style,transition_out,duration_target"
        )
        if not segments_data:
            raise Exception("No segments found")

        mood = job.get("mood", "hopeful_future")

        # Build segments list with downloaded local paths
        render_segments = []
        for seg in segments_data:
            seg_idx = seg["segment_idx"]
            log(job_id, f"Preparing seg {seg_idx}")

            # Download audio
            voice_r2 = seg.get("voice_r2_url", "")
            if not voice_r2:
                raise Exception(f"Segment {seg_idx} has no voice — cannot render")
            voice_url    = voice_r2 if voice_r2.startswith("http") else f"{R2_BASE_URL}/{voice_r2}"
            audio_local  = f"{TMP_DIR}/{job_id}_seg{seg_idx}_audio.mp3"
            _download(voice_url, audio_local)
            audio_dur    = _get_duration(audio_local)

            # Download media (images or video clips)
            media_local = []
            for m in (seg.get("media") or []):
                url = m.get("public_url") or (f"{R2_BASE_URL}/{m['r2_url']}" if m.get("r2_url") else "")
                if not url:
                    continue
                ext  = ".mp4" if m.get("type") == "video" else ".png"
                path = f"{TMP_DIR}/{job_id}_seg{seg_idx}_m{m.get('order',0)}{ext}"
                _download(url, path)
                media_local.append({"type": m.get("type","image"), "local_path": path})

            render_segments.append({
                "segment_idx":    seg_idx,
                "type":           seg.get("type","context"),
                "label":          seg.get("label",""),
                "audio_path":     audio_local,
                "audio_dur":      audio_dur,
                "media":          media_local,
                "caption_style":  seg.get("caption_style","subtitle"),
                "transition_out": seg.get("transition_out","dissolve"),
            })

        log(job_id, f"Rendering {len(render_segments)} segments")

        # Render via longform renderer
        final_video = _lf_renderer().remote(
            job_id=job_id,
            segments=render_segments,
            mood=mood,
        )

        log(job_id, f"Rendered: {os.path.getsize(final_video)//1024}KB")

        if TEST_MODE:
            log(job_id, "TEST_MODE — skipping YouTube upload")
            sb_patch(f"longform_jobs?id=eq.{job_id}",
                     {"status": "complete", "updated_at": datetime.utcnow().isoformat()})
            ping_worker(job_id, None, "render_complete",
                        {"youtube_id": "TEST_LONGFORM", "video_r2_url": ""})
            return {"status": "test_complete", "job_id": job_id}

        # Upload to YouTube
        title = _title_gen().remote(
            job.get("topic","India Future"), "", "Revelation: Nobody Talks About This"
        )
        def sanitize_for_youtube(t):
            import re
            if not t: return ""
            for bad,good in [("\u2019","'"),("\u2013","-"),("\u2014","-"),("\u20b9","Rs."),("\u00a0"," ")]:
                t=t.replace(bad,good)
            t=re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>","",t)
            t=re.sub(r"[\U0001F000-\U0001FFFF]","",t)
            t=re.sub(r"[\u0900-\u097F]","",t)
            return t.strip()
        full_script = " ".join(s.get("script","") for s in segments_data)
        description = (
            f"{sanitize_for_youtube(full_script[:3000])}\n\n"
            "India20Sixty - India's near future, explained in depth.\n\n"
            "#IndiaFuture #FutureTech #India #ISRO #Technology #Documentary"
        )
        video_id = _yt_upload().remote(
            video_path=final_video, title=title, description=description,
            tags=["Future India","India innovation","ISRO","Technology","Documentary"],
            publish_at=publish_at,
        )
        log(job_id, f"PUBLISHED: https://youtube.com/watch?v={video_id}")

        sb_patch(f"longform_jobs?id=eq.{job_id}", {
            "status": "complete", "youtube_id": video_id,
            "updated_at": datetime.utcnow().isoformat()
        })
        ping_worker(job_id, None, "render_complete",
                    {"youtube_id": video_id, "video_r2_url": ""})

        try: os.remove(final_video)
        except Exception: pass

        return {"status": "complete", "job_id": job_id, "youtube_id": video_id,
                "url": f"https://youtube.com/watch?v={video_id}"}

    except Exception as e:
        msg = str(e)
        print(f"\nLF RENDER FAILED: {msg}\n{traceback.format_exc()}")
        log(job_id, f"FAILED: {msg[:400]}")
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status": "failed", "error": msg[:400],
                  "updated_at": datetime.utcnow().isoformat()})
        ping_worker(job_id, None, "render_failed", {"error": msg[:400]})
        return {"status": "error", "message": msg}


# ==========================================
# HEALTH
# ==========================================

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="GET")
def health():
    return {
        "status": "healthy",
        "service": "india20sixty-longform",
        "version": "1.0",
        "endpoints": ["/generate-script", "/generate-segment-voice",
                      "/generate-segment-images", "/render-full"],
    }


# ==========================================
# UTILITIES
# ==========================================

def _download(url: str, path: str):
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    print(f"  Downloaded {path}: {os.path.getsize(path)//1024}KB")


def _get_duration(path: str) -> float:
    try:
        result = subprocess.run(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1",path],
            capture_output=True, text=True, timeout=10,
        )
        return float(result.stdout.strip())
    except Exception:
        return 30.0


@app.local_entrypoint()
def main():
    print("longform_pipeline.py health check")
    print(health.remote())