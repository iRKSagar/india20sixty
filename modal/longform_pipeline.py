import modal
import os
import traceback
import requests
import subprocess
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP — LONGFORM PIPELINE
#
# ONE web endpoint: /dispatch
# Action field routes to the right handler.
# This keeps us under Modal free plan's 8
# web endpoint limit.
#
# api.worker.js sends:
#   POST /dispatch  { action: "generate-script",         job_id, topic, cluster, target_duration }
#   POST /dispatch  { action: "generate-segment-voice",  job_id, segment_idx }
#   POST /dispatch  { action: "generate-segment-images", job_id, segment_idx }
#   POST /dispatch  { action: "render-full",             job_id, publish_at }
# ==========================================

app = modal.App("india20sixty-longform")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]
TMP_DIR = "/tmp/india20sixty-longform"

# Cross-app function references
def _lf_script():
    return modal.Function.from_name("india20sixty-longform-scriptwriter", "generate_longform_script")

def _lf_renderer():
    return modal.Function.from_name("india20sixty-longform-renderer", "render_longform")

def _image_gen():
    return modal.Function.from_name("india20sixty-images", "generate_single_image")

def _voice_gen():
    return modal.Function.from_name("india20sixty-voice", "generate_voice")

def _r2_upload():
    return modal.Function.from_name("india20sixty-publisher", "upload_to_r2")

def _yt_upload():
    return modal.Function.from_name("india20sixty-publisher", "upload_to_youtube")

def _title_gen():
    return modal.Function.from_name("india20sixty-publisher", "generate_title")

def _research():
    return modal.Function.from_name("india20sixty-research", "run_research")


# ── SUPABASE HELPERS ──────────────────────────────────────────

def _hdrs():
    return {
        "apikey":        os.environ["SUPABASE_ANON_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_ANON_KEY']}",
        "Content-Type":  "application/json",
    }

def sb_get(ep):
    r = requests.get(f"{os.environ['SUPABASE_URL']}/rest/v1/{ep}", headers=_hdrs(), timeout=10)
    return r.json() if r.ok else []

def sb_patch(ep, data):
    requests.patch(f"{os.environ['SUPABASE_URL']}/rest/v1/{ep}",
                   headers={**_hdrs(), "Prefer": "return=minimal"}, json=data, timeout=10)

def sb_insert(table, data):
    r = requests.post(f"{os.environ['SUPABASE_URL']}/rest/v1/{table}",
                      headers={**_hdrs(), "Prefer": "return=representation"}, json=data, timeout=10)
    if not r.ok: raise Exception(f"INSERT {r.status_code}: {r.text[:200]}")
    return r.json()[0]

def log(job_id, msg):
    print(f"[longform] {msg}")
    try:
        requests.post(f"{os.environ['SUPABASE_URL']}/rest/v1/render_logs",
                      headers=_hdrs(),
                      json={"job_id": job_id, "message": str(msg)[:500]}, timeout=5)
    except Exception:
        pass

def ping_worker(job_id, segment_idx, event, payload):
    worker_url = os.environ.get("WORKER_URL", "")
    if not worker_url:
        return
    try:
        requests.post(
            worker_url.rstrip("/") + "/longform/webhook",
            headers={"Content-Type": "application/json"},
            json={"job_id": job_id, "segment_idx": segment_idx,
                  "event": event, "payload": payload},
            timeout=10,
        )
    except Exception as e:
        print(f"  Webhook ping failed (non-fatal): {e}")


# ==========================================
# SINGLE DISPATCHER ENDPOINT
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=60)
@modal.fastapi_endpoint(method="POST")
def dispatch(data: dict):
    """
    Single entry point for all longform operations.
    Spawns the appropriate handler function async.
    Returns immediately with { status: started }.
    """
    action = data.get("action", "")
    job_id = data.get("job_id", "")

    if not action:
        return {"status": "error", "message": "Missing action"}
    if not job_id:
        return {"status": "error", "message": "Missing job_id"}

    print(f"[dispatch] action={action} job={job_id}")

    if action == "generate-script":
        _handle_generate_script.spawn(data)
    elif action == "generate-segment-voice":
        _handle_segment_voice.spawn(data)
    elif action == "generate-segment-images":
        _handle_segment_images.spawn(data)
    elif action == "render-full":
        _handle_render_full.spawn(data)
    else:
        return {"status": "error", "message": f"Unknown action: {action}"}

    return {"status": "started", "action": action, "job_id": job_id}


# ==========================================
# HANDLER: GENERATE SCRIPT
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=180)
def _handle_generate_script(data: dict):
    job_id   = data["job_id"]
    topic    = data.get("topic", "Future India")
    cluster  = data.get("cluster", "Space")
    dur_secs = int(data.get("target_duration", 420))

    print(f"\n[LF Generate Script] job={job_id} topic={topic[:60]}")
    sb_patch(f"longform_jobs?id=eq.{job_id}",
             {"status": "scripting", "updated_at": datetime.utcnow().isoformat()})
    try:
        fact_package = _research().remote(job_id, topic)
        result = _lf_script().remote(
            job_id=job_id, topic=topic, cluster=cluster,
            target_duration=dur_secs, fact_package=fact_package,
        )
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"mood": result["mood"], "updated_at": datetime.utcnow().isoformat()})
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
    except Exception as e:
        msg = str(e)[:400]
        print(f"  Script failed: {msg}")
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status": "failed", "error": msg, "updated_at": datetime.utcnow().isoformat()})


# ==========================================
# HANDLER: GENERATE SEGMENT VOICE
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=120)
def _handle_segment_voice(data: dict):
    job_id      = data["job_id"]
    segment_idx = data.get("segment_idx")
    if segment_idx is None:
        return

    print(f"\n[LF Segment Voice] job={job_id} seg={segment_idx}")
    try:
        segs = sb_get(
            f"longform_segments?job_id=eq.{job_id}"
            f"&segment_idx=eq.{segment_idx}&select=script,duration_target"
        )
        if not segs or not segs[0].get("script"):
            print("  No script for this segment")
            return

        voice_result = _voice_gen().remote(
            job_id=f"{job_id}_seg{segment_idx}",
            reviewed_script=segs[0]["script"],
        )
        audio_path = voice_result["audio_path"]
        audio_dur  = voice_result["duration"]

        r2_key    = f"longform/{job_id}/seg{segment_idx}_voice.mp3"
        voice_url = _r2_upload().remote(audio_path, r2_key)
        try: os.remove(audio_path)
        except Exception: pass

        log(job_id, f"Seg {segment_idx} voice: {audio_dur:.1f}s")
        ping_worker(job_id, segment_idx, "segment_voice_ready", {
            "voice_r2_url": r2_key, "voice_pub_url": voice_url, "duration": audio_dur,
        })
    except Exception as e:
        msg = str(e)[:400]
        print(f"  Segment voice failed: {msg}")
        sb_patch(
            f"longform_segments?job_id=eq.{job_id}&segment_idx=eq.{segment_idx}",
            {"status": "voice_failed", "updated_at": datetime.utcnow().isoformat()}
        )


# ==========================================
# HANDLER: GENERATE SEGMENT IMAGES
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=300)
def _handle_segment_images(data: dict):
    job_id      = data["job_id"]
    segment_idx = data.get("segment_idx")
    if segment_idx is None:
        return

    print(f"\n[LF Segment Images] job={job_id} seg={segment_idx}")
    try:
        segs = sb_get(
            f"longform_segments?job_id=eq.{job_id}"
            f"&segment_idx=eq.{segment_idx}&select=script,image_prompts"
        )
        if not segs:
            return
        image_prompts = segs[0].get("image_prompts") or []
        if not image_prompts:
            print("  No image prompts")
            return

        futures = [
            _image_gen().spawn(
                prompt=prompt, scene_idx=i,
                job_id=f"{job_id}_seg{segment_idx}",
            )
            for i, prompt in enumerate(image_prompts)
        ]
        results = [f.get() for f in futures]

        media = []
        for res in results:
            if res["success"] and res["local_path"]:
                r2_key     = f"longform/{job_id}/seg{segment_idx}_img{res['scene_idx']}.png"
                public_url = _r2_upload().remote(res["local_path"], r2_key)
                media.append({"type":"image","r2_url":r2_key,"public_url":public_url,"order":res["scene_idx"]})
                try: os.remove(res["local_path"])
                except Exception: pass

        log(job_id, f"Seg {segment_idx} images: {len(media)}/{len(image_prompts)}")
        ping_worker(job_id, segment_idx, "segment_images_ready", {"media": media})
    except Exception as e:
        msg = str(e)[:400]
        sb_patch(
            f"longform_segments?job_id=eq.{job_id}&segment_idx=eq.{segment_idx}",
            {"status": "image_failed", "updated_at": datetime.utcnow().isoformat()}
        )


# ==========================================
# HANDLER: RENDER FULL VIDEO
# ==========================================

@app.function(image=image, secrets=secrets, cpu=4.0, memory=8192, timeout=1800)
def _handle_render_full(data: dict):
    job_id     = data["job_id"]
    publish_at = data.get("publish_at")
    TEST_MODE  = os.environ.get("TEST_MODE", "true").lower() == "true"
    R2_BASE    = os.environ.get("R2_BASE_URL", "").rstrip("/")

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    print(f"\n[LF Render Full] job={job_id}")
    sb_patch(f"longform_jobs?id=eq.{job_id}",
             {"status": "rendering", "updated_at": datetime.utcnow().isoformat()})

    try:
        jobs = sb_get(f"longform_jobs?id=eq.{job_id}&select=id,topic,cluster,mood,target_duration")
        if not jobs: raise Exception("Job not found")
        job = jobs[0]

        segments_data = sb_get(
            f"longform_segments?job_id=eq.{job_id}&order=segment_idx.asc"
            "&select=segment_idx,type,label,script,media,voice_r2_url,"
            "caption_style,transition_out,duration_target"
        )
        if not segments_data: raise Exception("No segments found")

        mood = job.get("mood", "hopeful_future")
        render_segments = []

        for seg in segments_data:
            seg_idx  = seg["segment_idx"]
            voice_r2 = seg.get("voice_r2_url", "")
            if not voice_r2:
                raise Exception(f"Segment {seg_idx} has no voice")

            voice_url   = voice_r2 if voice_r2.startswith("http") else f"{R2_BASE}/{voice_r2}"
            audio_local = f"{TMP_DIR}/{job_id}_seg{seg_idx}_audio.mp3"
            _download(voice_url, audio_local)
            audio_dur = _get_duration(audio_local)

            media_local = []
            for m in (seg.get("media") or []):
                url = m.get("public_url") or (f"{R2_BASE}/{m['r2_url']}" if m.get("r2_url") else "")
                if not url: continue
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
        final_video = _lf_renderer().remote(
            job_id=job_id, segments=render_segments, mood=mood,
        )
        log(job_id, f"Rendered: {os.path.getsize(final_video)//1024}KB")

        if TEST_MODE:
            log(job_id, "TEST_MODE — skipping upload")
            sb_patch(f"longform_jobs?id=eq.{job_id}",
                     {"status": "complete", "updated_at": datetime.utcnow().isoformat()})
            ping_worker(job_id, None, "render_complete",
                        {"youtube_id": "TEST_LONGFORM", "video_r2_url": ""})
            return

        title = _title_gen().remote(job.get("topic","India Future"), "")

        def sanitize(t):
            import re
            if not t: return ""
            for bad,good in [("\u2019","'"),("\u2013","-"),("\u2014","-"),("\u20b9","Rs.")]:
                t=t.replace(bad,good)
            t=re.sub(r"[\U0001F000-\U0001FFFF]","",t)
            t=re.sub(r"[\u0900-\u097F]","",t)
            return t.strip()

        full_script = " ".join(s.get("script","") for s in segments_data)
        description = (
            f"{sanitize(full_script[:3000])}\n\n"
            "India20Sixty - India's near future, explained in depth.\n\n"
            "#IndiaFuture #FutureTech #India #ISRO #Technology #Documentary"
        )
        video_id = _yt_upload().remote(
            video_path=final_video, title=title, description=description,
            tags=["Future India","India innovation","ISRO","Technology","Documentary"],
            publish_at=publish_at,
        )
        log(job_id, f"PUBLISHED: https://youtube.com/watch?v={video_id}")
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status":"complete","youtube_id":video_id,"updated_at":datetime.utcnow().isoformat()})
        ping_worker(job_id, None, "render_complete",
                    {"youtube_id": video_id, "video_r2_url": ""})
        try: os.remove(final_video)
        except Exception: pass

    except Exception as e:
        msg = str(e)
        print(f"\nRENDER FAILED: {msg}\n{traceback.format_exc()}")
        log(job_id, f"FAILED: {msg[:400]}")
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status":"failed","error":msg[:400],"updated_at":datetime.utcnow().isoformat()})
        ping_worker(job_id, None, "render_failed", {"error": msg[:400]})


# ==========================================
# UTILITIES
# ==========================================

def _download(url: str, path: str):
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192): f.write(chunk)
    print(f"  Downloaded {os.path.basename(path)}: {os.path.getsize(path)//1024}KB")

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
    print("longform_pipeline.py — 1 web endpoint: /dispatch")
    print("Actions: generate-script, generate-segment-voice, generate-segment-images, render-full")