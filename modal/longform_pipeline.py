import modal
import os
import traceback
import requests
import subprocess
from pathlib import Path
from datetime import datetime
from fastapi import Request

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
        print(f"  ping_worker SKIP — WORKER_URL not set (event={event})")
        return
    try:
        r = requests.post(
            worker_url.rstrip("/") + "/longform/webhook",
            headers={"Content-Type": "application/json"},
            json={"job_id": job_id, "segment_idx": segment_idx,
                  "event": event, "payload": payload},
            timeout=15,
        )
        print(f"  ping_worker {event} → {r.status_code}")
        if not r.ok:
            print(f"  ping_worker error body: {r.text[:200]}")
    except Exception as e:
        print(f"  ping_worker FAILED (event={event}): {e}")


# ==========================================
# SINGLE DISPATCHER ENDPOINT
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=60)
@modal.fastapi_endpoint(method="POST")
async def dispatch(request: Request):
    """
    Single entry point for all longform operations.
    Spawns the appropriate handler function async.
    Returns immediately with { status: started }.
    """
    data = {}
    try:
        data = await request.json()
    except Exception:
        pass

    action = data.get("action", "")
    job_id = data.get("job_id", "")

    if not action:
        return {"status": "error", "message": "Missing action"}
    if not job_id:
        return {"status": "error", "message": "Missing job_id"}

    print(f"[dispatch] action={action} job={job_id}")

    if action == "generate-script":
        await _handle_generate_script.spawn.aio(data)
    elif action == "generate-segment-voice":
        await _handle_segment_voice.spawn.aio(data)
    elif action == "generate-segment-images":
        await _handle_segment_images.spawn.aio(data)
    elif action == "render-full":
        await _handle_render_full.spawn.aio(data)
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

        # Insert segments directly — more reliable than webhook ping
        print(f"  Inserting {len(result['segments'])} segments into Supabase...")
        for s in result["segments"]:
            try:
                sb_insert("longform_segments", {
                    "job_id":         job_id,
                    "segment_idx":    s["segment_idx"],
                    "segment_type":   s["type"],
                    "label":          s["label"],
                    "script":         s["script"],
                    "duration_target":s["duration_target"],
                    "image_prompts":  s.get("image_prompts", []),
                    "caption_style":  s.get("caption_style", "subtitle"),
                    "transition_out": s.get("transition_out", "dissolve"),
                    "media":          [],
                    "voice_r2_url":   None,
                    "voice_source":   None,
                    "status":         "has_script",
                    "created_at":     datetime.utcnow().isoformat(),
                })
                print(f"    Segment {s['segment_idx']} [{s['label']}] inserted")
            except Exception as se:
                print(f"    Segment {s['segment_idx']} insert failed: {se}")

        # Update job status to media_collecting
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status": "media_collecting", "updated_at": datetime.utcnow().isoformat()})

        # Also ping worker as backup (non-critical)
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

        # AUTO MODE — spawn voice + images for all segments
        # Stagger to avoid hitting GPU limit (10 GPUs on Starter plan)
        if auto_mode:
            log(job_id, "Auto mode: spawning voice + images for all segments")
            import time
            for s in result["segments"]:
                try:
                    _handle_segment_voice.spawn({"job_id": job_id, "segment_idx": s["segment_idx"]})
                    _handle_segment_images.spawn({"job_id": job_id, "segment_idx": s["segment_idx"]})
                    print(f"  Spawned voice+images for seg {s['segment_idx']}")
                    time.sleep(2)  # 2s stagger between segments to avoid GPU stampede
                except Exception as ae:
                    print(f"  Auto spawn failed seg {s['segment_idx']}: {ae}")
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
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
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
        # Voice ran in separate container — use bytes not path
        audio_bytes = voice_result.get("audio_bytes")
        audio_dur   = voice_result["duration"]

        audio_local = f"{TMP_DIR}/{job_id}_seg{segment_idx}_voice.mp3"
        if audio_bytes:
            with open(audio_local, "wb") as f:
                f.write(audio_bytes)
        else:
            raise Exception("Voice worker returned no audio bytes")

        r2_key    = f"longform/{job_id}/seg{segment_idx}_voice.mp3"
        voice_url = _r2_upload().remote(audio_local, r2_key)
        try: os.remove(audio_local)
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
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
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
            if res["success"] and res.get("image_bytes"):
                r2_key = f"longform/{job_id}/seg{segment_idx}_img{res['scene_idx']}.png"
                # Write bytes to this container's /tmp/ then upload to R2
                local_p = f"{TMP_DIR}/{job_id}_seg{segment_idx}_img{res['scene_idx']}.png"
                with open(local_p, "wb") as f:
                    f.write(res["image_bytes"])
                public_url = _r2_upload().remote(local_p, r2_key)
                media.append({"type":"image","r2_url":r2_key,"public_url":public_url,"order":res["scene_idx"]})
                try: os.remove(local_p)
                except Exception: pass
            elif res.get("r2_url"):
                # Already on R2 from auto-save
                media.append({"type":"image","r2_url":res["r2_url"],"public_url":res["r2_url"],"order":res["scene_idx"]})

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

            # Pass R2 URLs directly — renderer downloads in its own container
            voice_url = voice_r2 if voice_r2.startswith("http") else f"{R2_BASE}/{voice_r2}"

            media_urls = []
            for m in (seg.get("media") or []):
                url = m.get("public_url") or (f"{R2_BASE}/{m['r2_url']}" if m.get("r2_url") else "")
                if url:
                    media_urls.append({"type": m.get("type","image"), "url": url, "order": m.get("order",0)})

            render_segments.append({
                "segment_idx":    seg_idx,
                "type":           seg.get("type","context"),
                "label":          seg.get("label",""),
                "voice_url":      voice_url,      # R2 URL — renderer downloads
                "media_urls":     media_urls,      # R2 URLs — renderer downloads
                "caption_style":  seg.get("caption_style","subtitle"),
                "transition_out": seg.get("transition_out","dissolve"),
            })

        log(job_id, f"Rendering {len(render_segments)} segments")
        video_bytes = _lf_renderer().remote(
            job_id=job_id, segments=render_segments, mood=mood,
        )
        log(job_id, f"Rendered: {len(video_bytes)//1024}KB")

        # Upload video bytes to R2
        r2_key = f"longform/{job_id}/final.mp4"
        video_r2_url = None
        try:
            import boto3, hashlib, hmac as hmac_lib, urllib.parse
            from datetime import datetime as dt
            R2_ACCT = os.environ.get("R2_ACCOUNT_ID","")
            R2_KEY  = os.environ.get("R2_ACCESS_KEY_ID","")
            R2_SEC  = os.environ.get("R2_SECRET_ACCESS_KEY","")
            R2_BKT  = os.environ.get("R2_BUCKET","india20sixty-videos")
            if R2_ACCT and R2_KEY:
                import requests as req
                now = dt.utcnow()
                date_str = now.strftime("%Y%m%d")
                time_str = now.strftime("%Y%m%dT%H%M%SZ")
                payload_hash = hashlib.sha256(video_bytes).hexdigest()
                endpoint = f"https://{R2_ACCT}.r2.cloudflarestorage.com"
                url = f"{endpoint}/{R2_BKT}/{r2_key}"
                signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
                canonical = "\n".join(["PUT", f"/{R2_BKT}/{urllib.parse.quote(r2_key, safe='/')}","",
                    f"content-type:video/mp4",f"host:{R2_ACCT}.r2.cloudflarestorage.com",
                    f"x-amz-content-sha256:{payload_hash}",f"x-amz-date:{time_str}","",
                    signed_headers,payload_hash])
                cred_scope = f"{date_str}/auto/s3/aws4_request"
                string_to_sign = "\n".join(["AWS4-HMAC-SHA256",time_str,cred_scope,
                    hashlib.sha256(canonical.encode()).hexdigest()])
                def sign(key, msg): return hmac_lib.new(key, msg.encode(), hashlib.sha256).digest()
                sk = sign(sign(sign(sign(f"AWS4{R2_SEC}".encode(),date_str),"auto"),"s3"),"aws4_request")
                sig = hmac_lib.new(sk, string_to_sign.encode(), hashlib.sha256).hexdigest()
                r = req.put(url, data=video_bytes, headers={
                    "content-type":"video/mp4","x-amz-content-sha256":payload_hash,
                    "x-amz-date":time_str,
                    "Authorization":f"AWS4-HMAC-SHA256 Credential={R2_KEY}/{cred_scope},SignedHeaders={signed_headers},Signature={sig}",
                }, timeout=120)
                if r.ok:
                    video_r2_url = f"{R2_BASE}/{r2_key}"
                    log(job_id, f"R2: {r2_key}")
        except Exception as e:
            log(job_id, f"R2 upload error: {e}")

        if TEST_MODE:
            log(job_id, "TEST_MODE — skipping upload")
            sb_patch(f"longform_jobs?id=eq.{job_id}",
                     {"status": "complete", "updated_at": datetime.utcnow().isoformat()})
            ping_worker(job_id, None, "render_complete",
                        {"youtube_id": "TEST_LONGFORM", "video_r2_url": video_r2_url or ""})
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
            video_path="", title=title, description=description,
            tags=["Future India","India innovation","ISRO","Technology","Documentary"],
            publish_at=publish_at,
            video_bytes=video_bytes,
        )
        log(job_id, f"PUBLISHED: https://youtube.com/watch?v={video_id}")
        sb_patch(f"longform_jobs?id=eq.{job_id}",
                 {"status":"complete","youtube_id":video_id,
                  "video_r2_url":video_r2_url or "",
                  "updated_at":datetime.utcnow().isoformat()})
        ping_worker(job_id, None, "render_complete",
                    {"youtube_id": video_id, "video_r2_url": video_r2_url or ""})
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