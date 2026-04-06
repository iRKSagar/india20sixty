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
# All cross-app calls use modal.Function.from_name()
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
    return modal.Function.from_name("india20sixty-research", "run_research")

def _scriptwriter():
    return modal.Function.from_name("india20sixty-scriptwriter", "run_scriptwriter")

def _image_gen():
    return modal.Function.from_name("india20sixty-images", "generate_single_image")

def _voice_gen():
    return modal.Function.from_name("india20sixty-voice", "generate_voice")

def _render_audio():
    return modal.Function.from_name("india20sixty-renderer", "render_with_audio")

def _render_silent():
    return modal.Function.from_name("india20sixty-renderer", "render_silent")

def _r2_upload():
    return modal.Function.from_name("india20sixty-publisher", "upload_to_r2")

def _yt_upload():
    return modal.Function.from_name("india20sixty-publisher", "upload_to_youtube")

def _title_gen():
    return modal.Function.from_name("india20sixty-publisher", "generate_title")


# ==========================================
# WEB ENDPOINTS
# ==========================================

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="POST")
def trigger(data: dict):
    action = data.get("action", "run")

    # Route add-voice-and-publish
    if action == "add-voice-and-publish":
        add_voice_and_publish.spawn(data)
        return {"status": "started", "action": action, "job_id": data.get("job_id")}

    # Route retry-upload
    if action == "retry-upload":
        retry_upload.spawn(data)
        return {"status": "started", "action": action, "job_id": data.get("job_id")}

    # Default: run pipeline
    job_id      = data.get("job_id") or str(uuid.uuid4())
    topic       = data.get("topic", "Future India")
    webhook_url = data.get("webhook_url", "")
    image_urls  = data.get("image_urls") or []
    script_package = data.get("script_package")
    print(f"Trigger: {job_id} | {topic}")
    run_pipeline.spawn(job_id=job_id, topic=topic,
                       webhook_url=webhook_url, image_urls=image_urls,
                       script_package=script_package)
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
def run_pipeline(job_id: str, topic: str, webhook_url: str = "", image_urls: list = None, script_package: dict = None):

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
        if script_package and script_package.get("text"):
            raw_text  = script_package.get("text", "")
            word_count = len(raw_text.split())
            import re as _re
            devanagari = len(_re.findall(r'[\u0900-\u097F]', raw_text))
            hindi_ratio = devanagari / max(len(raw_text), 1)

            if hindi_ratio > 0.02 or word_count < 45:
                print(f"  Council script rejected: {word_count} words, {devanagari} Devanagari — running scriptwriter")
                # Fall through to research + scriptwriter below
                fact_package = _research().remote(job_id, topic)
                log(f"Research: {'found' if fact_package.get('found') else 'no anchor'}")
                print("\n--- Script ---")
                script_pkg = _scriptwriter().remote(job_id, topic, fact_package, cluster, subscribe_cta)
                log(f"Script mood={script_pkg['mood']} words={len(script_pkg['script'].split())}")
            else:
                print(f"  Using pre-generated council script ({word_count} words)")
                fact_package = {"found": True, "key_fact": script_package.get("key_fact", ""),
                                "source": script_package.get("source", "council")}
                script_pkg = {
                    "script":          raw_text,
                    "reviewed_script": script_package.get("reviewed_script", raw_text),
                    "mood":            script_package.get("mood", "hopeful_future"),
                    "scene_prompts":   script_package.get("scene_prompts", [f"cinematic modern India scene {i+1}" for i in range(3)]),
                    "captions":        script_package.get("captions", []),
                    "cluster":         script_package.get("cluster", cluster),
                    "key_fact":        script_package.get("key_fact", ""),
                }
                log(f"Script (council): mood={script_pkg['mood']} words={word_count}")
        else:
            fact_package = _research().remote(job_id, topic)
            log(f"Research: {'found' if fact_package.get('found') else 'no anchor'}")
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
            # Validate scene prompts are topic-relevant (not generic offices)
            validated_prompts = []
            for i in range(3):
                sp = script_pkg["scene_prompts"][i] if i < len(script_pkg.get("scene_prompts",[])) else ""
                if not sp:
                    sp = f"photorealistic India, {topic}, natural daylight, sharp focus"
                # If non-AI topic has generic office prompt, rebuild from topic
                office_words = ["engineer at computer", "software engineer", "at workstation", "at desk", "office worker"]
                ai_cluster = script_pkg.get("cluster", cluster) == "AI"
                if not ai_cluster and any(w in sp.lower() for w in office_words):
                    sp = f"photorealistic India, {topic}, natural daylight, Indian people, sharp focus, no text"
                    print(f"  Scene {i}: office prompt replaced with topic-based prompt")
                validated_prompts.append(sp)

            futures = [
                _image_gen().spawn(
                    prompt=validated_prompts[i],
                    scene_idx=i, job_id=job_id,
                    cluster=script_pkg.get("cluster", cluster),
                    job_type="shorts",
                    topic=topic,
                )
                for i in range(3)
            ]
            results = [f.get() for f in futures]
            image_paths = []
            for res in sorted(results, key=lambda x: x["scene_idx"]):
                p = f"{TMP_DIR}/{job_id}_{res['scene_idx']}.png"
                if res["success"] and res.get("image_bytes"):
                    with open(p, "wb") as f:
                        f.write(res["image_bytes"])
                    image_paths.append(p)
                else:
                    # Image failed — pass None, renderer will generate black frame
                    print(f"  Image {res['scene_idx']} failed — renderer will use black frame")
                    image_paths.append(None)

        log(f"Images: {len(image_paths)}")

        # ── BRANCH: VOICE MODE ─────────────────────────────────────

        if voice_mode == "human":
            print("\n--- Render Silent ---")
            update_status("render")
            image_bytes_list = [res.get("image_bytes") for res in sorted(results, key=lambda x: x["scene_idx"])]
            silent_bytes = _render_silent().remote(
                job_id=job_id, image_paths=[None] * 3,
                captions=script_pkg["captions"], mood=script_pkg["mood"],
                image_bytes_list=image_bytes_list,
            )
            silent_local = f"{TMP_DIR}/{job_id}_silent.mp4"
            with open(silent_local, "wb") as f:
                f.write(silent_bytes)
            r2_key       = f"staged/{job_id}/video.mp4"
            video_r2_url = _r2_upload().remote(silent_local, r2_key)
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
            # Write audio bytes into THIS container's /tmp/ — voice ran in separate container
            audio_bytes = voice_result.get("audio_bytes")
            audio_dur   = voice_result["duration"]
            audio_path  = f"{TMP_DIR}/{job_id}.mp3"
            if audio_bytes:
                with open(audio_path, "wb") as f:
                    f.write(audio_bytes)
            else:
                raise Exception("Voice worker returned no audio bytes")

            log(f"Voice: {audio_dur:.1f}s engine={voice_result.get('engine','?')}")

            print("\n--- Render ---")
            update_status("render")

            # Collect image bytes for renderer
            image_bytes_list = []
            for res in sorted(results, key=lambda x: x["scene_idx"]):
                image_bytes_list.append(res.get("image_bytes"))

            video_path = _render_audio().remote(
                job_id=job_id,
                image_paths=[None] * 3,      # renderer ignores paths when bytes provided
                audio_path="",               # renderer ignores path when bytes provided
                audio_dur=audio_dur,
                captions=script_pkg["captions"],
                mood=script_pkg["mood"],
                image_bytes_list=image_bytes_list,
                audio_bytes=audio_bytes,
            )
            # video_path is now bytes returned from renderer
            video_bytes = video_path  # rename for clarity
            local_video = f"{TMP_DIR}/{job_id}.mp4"
            with open(local_video, "wb") as f:
                f.write(video_bytes)
            video_path = local_video
            log(f"Video rendered: {len(video_bytes)//1024}KB")

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
                video_r2_url = _r2_upload().remote("", r2_key, file_bytes=video_bytes)
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
                try:
                    video_id = _yt_upload().remote(
                        video_path="", title=title, description=description,
                        tags=["Future India","India innovation","AI","Technology","Shorts"],
                        video_bytes=video_bytes,
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
                except Exception as pub_err:
                    # Publish failed — save video to staged so it can be published later
                    pub_msg = str(pub_err)
                    print(f"\nPUBLISH FAILED ({pub_msg}) — saving to staged for manual publish")
                    log(f"Publish failed: {pub_msg[:200]}")
                    try:
                        r2_key = f"staged/{job_id}/video.mp4"
                        video_r2_url = _r2_upload().remote("", r2_key, file_bytes=video_bytes)
                        update_status("staged", {
                            "video_r2_url":  video_r2_url,
                            "error":         f"publish_failed: {pub_msg[:200]}",
                            "script_package": {**_make_pkg(script_pkg, fact_package), "title": title},
                        })
                        log(f"Staged after publish failure: {r2_key}")
                        print(f"  Staged at {r2_key} — publish from dashboard when ready")
                    except Exception as stage_err:
                        print(f"  Staging also failed: {stage_err}")
                        update_status("cbdp", {"error": f"publish+stage failed: {pub_msg[:200]}"})
                    # Don't re-raise — job is now staged, not failed

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
        "text":         script_pkg.get("reviewed_script", script_pkg.get("script", "")),
        "original":     script_pkg.get("script", ""),
        "lines":        script_pkg.get("script_lines", []),
        "captions":     script_pkg.get("captions", []),
        "fact_anchor":  fact_package,
        "mood":         script_pkg.get("mood", "hopeful_future"),
        "mood_label":   script_pkg.get("mood_label", ""),
        "scene_prompts":script_pkg.get("scene_prompts", []),
        "key_fact":     script_pkg.get("key_fact", ""),
        "source":       script_pkg.get("source", "pipeline"),
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