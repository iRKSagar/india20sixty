import modal
import os
import uuid
import traceback
import subprocess
import requests
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP — PIPELINE ORCHESTRATOR v5.0
#
# NO direct imports of other Modal apps.
# All cross-app calls use modal.Function.lookup()
# so Modal doesn't try to find .app on imports.
# ==========================================

app = modal.App("india20sixty")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]
TMP_DIR = "/tmp/india20sixty"

# ── CROSS-APP FUNCTION REFERENCES ─────────────────────────────
# These are resolved at runtime, not import time.
# This is the correct pattern for calling across Modal apps.

def _research():
    return modal.Function.lookup("india20sixty-research", "run_research")

def _scriptwriter():
    return modal.Function.lookup("india20sixty-scriptwriter", "run_scriptwriter")

def _image_gen():
    return modal.Function.lookup("india20sixty-images", "generate_single_image")

def _voice_gen():
    return modal.Function.lookup("india20sixty-voice", "generate_voice")

def _render_audio():
    return modal.Function.lookup("india20sixty-renderer", "render_with_audio")

def _render_silent():
    return modal.Function.lookup("india20sixty-renderer", "render_silent")

def _r2_upload():
    return modal.Function.lookup("india20sixty-publisher", "upload_to_r2")

def _yt_upload():
    return modal.Function.lookup("india20sixty-publisher", "upload_to_youtube")

def _title_gen():
    return modal.Function.lookup("india20sixty-publisher", "generate_title")


# ==========================================
# WEB ENDPOINTS
# ==========================================

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="POST")
def trigger(data: dict):
    job_id      = data.get("job_id") or str(uuid.uuid4())
    topic       = data.get("topic", "Future India")
    webhook_url = data.get("webhook_url", "")
    image_urls  = data.get("image_urls") or []
    print(f"Trigger: {job_id} | {topic}")
    run_pipeline.spawn(job_id=job_id, topic=topic,
                       webhook_url=webhook_url, image_urls=image_urls)
    return {"status": "started", "job_id": job_id, "topic": topic}


@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="GET")
def health():
    voice_mode = "unknown"
    try:
        SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
        SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
        if SUPABASE_URL:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/system_state?id=eq.main&select=voice_mode",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                timeout=3,
            )
            if r.ok and r.json():
                voice_mode = r.json()[0].get("voice_mode", "ai")
    except Exception:
        pass
    return {"status": "healthy", "platform": "modal", "version": "5.0",
            "voice_mode": voice_mode}


# ==========================================
# MAIN PIPELINE
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=900)
def run_pipeline(job_id: str, topic: str, webhook_url: str = "", image_urls: list = None):

    SUPABASE_URL      = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
    TEST_MODE         = os.environ.get("TEST_MODE", "true").lower() == "true"
    R2_BASE_URL       = os.environ.get("R2_BASE_URL", "").rstrip("/")

    print(f"\n{'='*60}\nPIPELINE v5.0: {job_id} | {topic}\nTEST: {TEST_MODE}\n{'='*60}\n")

    hdrs = {"apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json"}

    def sb_get(ep):
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{ep}", headers=hdrs, timeout=5)
        return r.json() if r.ok else []

    def update_status(status, extra=None):
        try:
            p = {"status": status, "updated_at": datetime.utcnow().isoformat()}
            if extra: p.update(extra)
            requests.patch(f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
                           headers={**hdrs, "Prefer": "return=minimal"}, json=p, timeout=10)
        except Exception as e:
            print(f"  Status update failed: {e}")

    def log(msg):
        try:
            requests.post(f"{SUPABASE_URL}/rest/v1/render_logs", headers=hdrs,
                          json={"job_id": job_id, "message": str(msg)[:500]}, timeout=5)
        except Exception: pass

    def sanitize(text):
        import re
        if not text: return ""
        for bad, good in [("\u2019","'"),("\u2018","'"),("\u201c",'"'),("\u201d",'"'),
                          ("\u2013","-"),("\u2014","-"),("\u20b9","Rs."),("\u00a0"," ")]:
            text = text.replace(bad, good)
        text = re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>","",text)
        text = re.sub(r"[\U0001F000-\U0001FFFF]","",text)
        text = re.sub(r"[\u0900-\u097F]","",text)
        return text.strip()

    try:
        update_status("processing", {"topic": topic})
        log("Pipeline v5.0 started")

        # Read system state
        state         = {}
        try: rows = sb_get("system_state?id=eq.main&select=voice_mode,publish,subscribe_cta"); state = rows[0] if rows else {}
        except Exception: pass
        voice_mode    = state.get("voice_mode", "ai")
        subscribe_cta = state.get("subscribe_cta", False)

        # Read job cluster
        cluster = "AI"
        try: rows = sb_get(f"jobs?id=eq.{job_id}&select=cluster"); cluster = (rows[0].get("cluster") or "AI") if rows else "AI"
        except Exception: pass

        print(f"  voice={voice_mode} cta={subscribe_cta} cluster={cluster}")

        # ── STEP 1: RESEARCH ──────────────────────────────────────
        print("\n--- Research ---")
        fact_package = _research().remote(job_id, topic)
        log(f"Research: {'found' if fact_package.get('found') else 'no anchor'}")

        # ── STEP 2: SCRIPT ────────────────────────────────────────
        print("\n--- Script ---")
        script_pkg = _scriptwriter().remote(job_id, topic, fact_package, cluster, subscribe_cta)
        log(f"Script mood={script_pkg['mood']} words={len(script_pkg['script'].split())}")

        # ── STEP 3: IMAGES — PARALLEL ─────────────────────────────
        update_status("images")
        print("\n--- Images (parallel) ---")
        Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

        if image_urls and len(image_urls) >= 3:
            image_paths = _download_library(image_urls, job_id)
        else:
            futures = [
                _image_gen().spawn(
                    prompt=script_pkg["scene_prompts"][i],
                    scene_idx=i, job_id=job_id,
                )
                for i in range(3)
            ]
            results = [f.get() for f in futures]
            image_paths = []
            for res in results:
                if res["success"] and res["local_path"]:
                    image_paths.append(res["local_path"])
                else:
                    p = f"{TMP_DIR}/{job_id}_{res['scene_idx']}_black.png"
                    subprocess.run(["ffmpeg","-y","-f","lavfi",
                        "-i","color=c=0x0d1117:s=864x1536:d=1",
                        "-frames:v","1",p], capture_output=True, timeout=15)
                    image_paths.append(p)

        log(f"Images: {len(image_paths)}")

        # ── BRANCH: VOICE MODE ─────────────────────────────────────

        if voice_mode == "human":
            print("\n--- Render Silent ---")
            update_status("render")
            silent_path = _render_silent().remote(
                job_id=job_id, image_paths=image_paths,
                captions=script_pkg["captions"], mood=script_pkg["mood"],
            )
            r2_key       = f"staged/{job_id}/video.mp4"
            video_r2_url = _r2_upload().remote(silent_path, r2_key)
            update_status("staged", {
                "video_r2_url": video_r2_url, "video_r2_key": r2_key,
                "script_package": _make_pkg(script_pkg, fact_package),
            })
            log(f"Staged: {r2_key}")
            print(f"\nSTAGED: {job_id}")

        else:
            print("\n--- Voice ---")
            update_status("voice")
            voice_result = _voice_gen().remote(job_id=job_id,
                                               reviewed_script=script_pkg["reviewed_script"])
            audio_path = voice_result["audio_path"]
            audio_dur  = voice_result["duration"]
            log(f"Voice: {audio_dur:.1f}s")

            print("\n--- Render ---")
            update_status("render")
            video_path = _render_audio().remote(
                job_id=job_id, image_paths=image_paths,
                audio_path=audio_path, audio_dur=audio_dur,
                captions=script_pkg["captions"], mood=script_pkg["mood"],
            )
            log("Video rendered")

            if TEST_MODE:
                update_status("test_complete", {"script_package": _make_pkg(script_pkg, fact_package)})
                print(f"\nTEST MODE complete: {job_id}")
                return

            # Check publish gate
            publish_enabled = False
            try:
                pub = sb_get("system_state?id=eq.main&select=publish")
                publish_enabled = pub[0].get("publish", False) if pub else False
            except Exception: pass

            if not publish_enabled:
                print("\n[PUBLISH OFF — review queue]")
                r2_key       = f"review/{job_id}/video.mp4"
                video_r2_url = _r2_upload().remote(video_path, r2_key)
                title        = _title_gen().remote(topic, fact_package.get("key_fact","") if fact_package else "")
                update_status("review", {
                    "video_r2_url": video_r2_url,
                    "script_package": {**_make_pkg(script_pkg, fact_package), "title": title},
                })
                log(f"Review queue: {r2_key}")
            else:
                print("\n--- Publish ---")
                update_status("upload")
                title = _title_gen().remote(topic, fact_package.get("key_fact","") if fact_package else "")
                source_line = f"\nSource: {fact_package['source']}\n" if fact_package and fact_package.get("source") else ""
                description = (
                    f"{sanitize(script_pkg['reviewed_script'])}\n\n"
                    f"{sanitize(source_line)}"
                    "India20Sixty - India's near future, explained.\n\n"
                    "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
                )
                video_id = _yt_upload().remote(
                    video_path=video_path, title=title, description=description,
                    tags=["Future India","India innovation","AI","Technology","Shorts"],
                )
                log(f"Uploaded: {video_id}")
                try:
                    requests.post(f"{SUPABASE_URL}/rest/v1/videos",
                                  headers={**hdrs,"Prefer":"return=minimal"},
                                  json={"job_id":job_id,"topic":topic,
                                        "youtube_url":f"https://youtube.com/watch?v={video_id}"},
                                  timeout=10)
                except Exception as e:
                    print(f"  videos insert (non-fatal): {e}")
                update_status("complete", {
                    "youtube_id": video_id,
                    "script_package": _make_pkg(script_pkg, fact_package),
                })
                print(f"\nCOMPLETE: https://youtube.com/watch?v={video_id}")

    except Exception as e:
        msg = str(e)
        print(f"\nFAILED: {msg}\n{traceback.format_exc()}")
        log(f"FAILED: {msg[:400]}")
        upload_kw = ["400","401","403","youtube","upload","quota","oauth"]
        is_upload = any(kw.lower() in msg.lower() for kw in upload_kw)
        try:
            rows = sb_get(f"jobs?id=eq.{job_id}&select=script_package")
            has_script = bool(rows and rows[0].get("script_package"))
        except Exception: has_script = False
        update_status("cbdp" if (is_upload and has_script) else "failed", {"error": msg[:400]})
        raise


# ==========================================
# ADD VOICE AND PUBLISH
# ==========================================

@app.function(image=image, secrets=secrets, cpu=2.0, memory=1024, timeout=300)
@modal.fastapi_endpoint(method="POST")
def add_voice_and_publish(data: dict):
    job_id = data.get("job_id")
    if not job_id: return {"status":"error","message":"Missing job_id"}

    SUPABASE_URL      = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
    R2_BASE_URL       = os.environ.get("R2_BASE_URL","").rstrip("/")
    TEST_MODE         = os.environ.get("TEST_MODE","true").lower() == "true"
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    hdrs = {"apikey":SUPABASE_ANON_KEY,"Authorization":f"Bearer {SUPABASE_ANON_KEY}","Content-Type":"application/json"}

    def sb_patch(ep, data):
        requests.patch(f"{SUPABASE_URL}/rest/v1/{ep}", headers={**hdrs,"Prefer":"return=minimal"}, json=data, timeout=10)

    def log(msg):
        print(f"[add_voice] {msg}")
        try: requests.post(f"{SUPABASE_URL}/rest/v1/render_logs", headers=hdrs, json={"job_id":job_id,"message":msg}, timeout=5)
        except Exception: pass

    try:
        rows = requests.get(f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}&select=id,topic,status,script_package,video_r2_url", headers=hdrs, timeout=10).json()
        if not rows: return {"status":"error","message":"Job not found"}
        job = rows[0]
        script_pkg = job.get("script_package") or {}
        script     = script_pkg.get("text","")
        title      = script_pkg.get("title", job.get("topic","India Future Tech")[:80])
        video_url  = job.get("video_r2_url","")
        if not script:    return {"status":"error","message":"No script"}
        if not video_url: return {"status":"error","message":"No video_r2_url"}

        sb_patch(f"jobs?id=eq.{job_id}", {"status":"voice","error":None,"updated_at":datetime.utcnow().isoformat()})
        log("Generating AI voice for staged video")

        voice_result = _voice_gen().remote(job_id, script)
        audio_path   = voice_result["audio_path"]
        audio_dur    = voice_result["duration"]
        log(f"Voice: {audio_dur:.1f}s")

        full_url    = video_url if video_url.startswith("http") else f"{R2_BASE_URL}/{video_url}"
        silent_path = f"{TMP_DIR}/{job_id}_silent.mp4"
        r = requests.get(full_url, timeout=60, stream=True); r.raise_for_status()
        with open(silent_path,"wb") as f:
            for chunk in r.iter_content(8192): f.write(chunk)

        final_path = f"{TMP_DIR}/{job_id}_final.mp4"
        result = subprocess.run(["ffmpeg","-y","-stream_loop","-1","-i",silent_path,
            "-i",audio_path,"-map","0:v:0","-map","1:a:0","-c:v","copy",
            "-c:a","aac","-b:a","128k","-t",str(audio_dur),"-movflags","+faststart",final_path],
            capture_output=True, timeout=120)
        if result.returncode != 0:
            raise Exception(f"Mix failed: {result.stderr.decode()[:200]}")

        if TEST_MODE:
            sb_patch(f"jobs?id=eq.{job_id}", {"status":"complete","updated_at":datetime.utcnow().isoformat()})
            return {"status":"test_complete","job_id":job_id}

        import re
        def sanitize(t):
            if not t: return ""
            for bad,good in [("\u2019","'"),("\u2013","-"),("\u2014","-"),("\u20b9","Rs.")]: t=t.replace(bad,good)
            t=re.sub(r"[\U0001F000-\U0001FFFF]","",t); t=re.sub(r"[\u0900-\u097F]","",t)
            return t.strip()

        description = (f"{sanitize(script)}\n\nIndia20Sixty - India's near future, explained.\n\n"
                       "#IndiaFuture #FutureTech #India #Shorts #ISRO #Technology")
        video_id = _yt_upload().remote(video_path=final_path, title=title, description=description,
                                       tags=["Future India","India","ISRO","Technology","Shorts"])
        log(f"UPLOADED: https://youtube.com/watch?v={video_id}")
        sb_patch(f"jobs?id=eq.{job_id}",{"status":"complete","youtube_id":video_id,"error":None,"updated_at":datetime.utcnow().isoformat()})
        for f in [audio_path, silent_path, final_path]:
            try: os.remove(f)
            except Exception: pass
        return {"status":"published","job_id":job_id,"youtube_id":video_id}

    except Exception as e:
        msg = str(e)[:400]
        log(f"ERROR: {msg}")
        requests.patch(f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
                       headers={**hdrs,"Prefer":"return=minimal"},
                       json={"status":"failed","error":msg,"updated_at":datetime.utcnow().isoformat()}, timeout=10)
        return {"status":"error","message":msg}


# ==========================================
# RETRY UPLOAD
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=180)
@modal.fastapi_endpoint(method="POST")
def retry_upload(data: dict):
    job_id = data.get("job_id")
    if not job_id: return {"status":"error","message":"Missing job_id"}

    SUPABASE_URL      = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
    TEST_MODE         = os.environ.get("TEST_MODE","true").lower() == "true"
    hdrs = {"apikey":SUPABASE_ANON_KEY,"Authorization":f"Bearer {SUPABASE_ANON_KEY}","Content-Type":"application/json"}

    def sb_patch(ep, data):
        requests.patch(f"{SUPABASE_URL}/rest/v1/{ep}", headers={**hdrs,"Prefer":"return=minimal"}, json=data, timeout=10)

    try:
        rows = requests.get(f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}&select=id,topic,status,script_package,video_r2_url", headers=hdrs, timeout=10).json()
        if not rows: return {"status":"error","message":"Job not found"}
        job = rows[0]
        if job["status"] not in ("cbdp","failed"):
            return {"status":"error","message":f"Status '{job['status']}' not retryable"}

        script_pkg   = job.get("script_package") or {}
        topic        = job.get("topic","India Future Tech")
        script       = script_pkg.get("text","")
        fact_pkg     = script_pkg.get("fact_anchor",{})
        video_r2_url = job.get("video_r2_url","")
        if not script:    return {"status":"error","message":"No script — needs re-render"}
        if not video_r2_url or not video_r2_url.startswith("http"):
            return {"status":"error","message":"No R2 URL — needs re-render"}

        sb_patch(f"jobs?id=eq.{job_id}",{"status":"upload","error":None,"updated_at":datetime.utcnow().isoformat()})
        if TEST_MODE:
            sb_patch(f"jobs?id=eq.{job_id}",{"status":"test_complete","youtube_id":"CBDP_TEST","updated_at":datetime.utcnow().isoformat()})
            return {"status":"test_complete","job_id":job_id}

        video_path = f"/tmp/cbdp_{job_id}.mp4"
        r = requests.get(video_r2_url, timeout=120, stream=True); r.raise_for_status()
        with open(video_path,"wb") as f:
            for chunk in r.iter_content(8192): f.write(chunk)

        title = _title_gen().remote(topic, fact_pkg.get("key_fact","") if fact_pkg else "")
        import re
        def sanitize(t):
            if not t: return ""
            for bad,good in [("\u2019","'"),("\u2013","-"),("\u2014","-"),("\u20b9","Rs.")]: t=t.replace(bad,good)
            t=re.sub(r"[\U0001F000-\U0001FFFF]","",t); t=re.sub(r"[\u0900-\u097F]","",t)
            return t.strip()
        source = f"\nSource: {fact_pkg['source']}\n" if fact_pkg and fact_pkg.get("source") else ""
        description = (f"{sanitize(script)}\n\n{sanitize(source)}"
                       "India20Sixty - India's near future.\n\n#IndiaFuture #India #Shorts")
        video_id = _yt_upload().remote(video_path=video_path, title=title, description=description)
        sb_patch(f"jobs?id=eq.{job_id}",{"status":"complete","youtube_id":video_id,"error":None,"updated_at":datetime.utcnow().isoformat()})
        try: os.remove(video_path)
        except Exception: pass
        return {"status":"complete","job_id":job_id,"youtube_id":video_id,"url":f"https://youtube.com/watch?v={video_id}"}

    except Exception as e:
        msg = str(e)
        sb_patch(f"jobs?id=eq.{job_id}",{"status":"cbdp","error":f"Retry failed: {msg[:350]}","updated_at":datetime.utcnow().isoformat()})
        return {"status":"error","message":msg}


# ==========================================
# UTILITIES
# ==========================================

def _make_pkg(script_pkg, fact_package):
    return {
        "text":         script_pkg["reviewed_script"],
        "original":     script_pkg["script"],
        "lines":        script_pkg["script_lines"],
        "captions":     script_pkg["captions"],
        "fact_anchor":  fact_package,
        "mood":         script_pkg["mood"],
        "mood_label":   script_pkg["mood_label"],
        "generated_at": datetime.utcnow().isoformat(),
    }

def _download_library(image_urls, job_id):
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    paths = []
    for i, url in enumerate(image_urls[:3]):
        path = f"{TMP_DIR}/{job_id}_{i}.png"
        try:
            r = requests.get(url, timeout=60, stream=True); r.raise_for_status()
            with open(path,"wb") as f:
                for chunk in r.iter_content(8192): f.write(chunk)
            print(f"  Library image {i+1}: {os.path.getsize(path)//1024}KB")
        except Exception as e:
            print(f"  Download {i+1} failed ({e}), black frame")
            subprocess.run(["ffmpeg","-y","-f","lavfi","-i","color=c=0x0d1117:s=864x1536:d=1","-frames:v","1",path],capture_output=True,timeout=15)
        paths.append(path)
    return paths


@app.local_entrypoint()
def main():
    print("Pipeline v5.0 — use modal deploy pipeline.py")
    print("health:", health.remote())