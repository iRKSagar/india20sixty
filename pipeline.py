import modal
import os
import subprocess
import json
import time
import shutil
import uuid
import traceback
import re
import requests
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP DEFINITION
# ==========================================

app = modal.App("india20sixty")

# Container image — debian with ffmpeg + fonts + python deps
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "ffmpeg",
        "fonts-liberation",
        "fonts-dejavu-core",
        "fonts-noto",
    )
    .pip_install(
        "requests",
        "flask",
    )
)

# All secrets stored in Modal dashboard under "india20sixty-secrets"
secrets = [modal.Secret.from_name("india20sixty-secrets")]

# Temp directory inside Modal container
TMP_DIR = "/tmp/india20sixty"

IMG_WIDTH  = 864
IMG_HEIGHT = 1536
OUT_WIDTH  = 1080
OUT_HEIGHT = 1920
FPS        = 25

LEONARDO_MODELS = [
    "aa77f04e-3eec-4034-9c07-d0f619684628",
    "1e60896f-3c26-4296-8ecc-53e2afecc132",
    "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
]

SCENE_TEMPLATES = [
    "futuristic Indian megacity at golden hour, lotus-shaped towers, electric air taxis, saffron teal palette, cinematic ultra-realistic photography",
    "Indian scientists in smart traditional attire, holographic data displays, temple architecture meets research campus, dramatic cinematic lighting",
    "aerial view transformed green India, solar farms vertical gardens, diverse communities, Indian tricolor, hopeful sunrise, epic cinematic wide shot"
]

# ==========================================
# WEB ENDPOINT — replaces /full-pipeline
# Called by Cloudflare worker exactly like before
# ==========================================

@app.function(image=image, secrets=secrets)
@modal.web_endpoint(method="POST")
def trigger(data: dict):
    """
    Receives POST from Cloudflare worker.
    Spawns pipeline async and returns immediately.
    """
    job_id      = data.get("job_id") or str(uuid.uuid4())
    topic       = data.get("topic", "Future India")
    webhook_url = data.get("webhook_url", "")

    print(f"Trigger received: {job_id} | {topic}")

    # Fire async — does not block, returns instantly
    run_pipeline.spawn(job_id=job_id, topic=topic, webhook_url=webhook_url)

    return {"status": "started", "job_id": job_id, "topic": topic}


@app.function(image=image, secrets=secrets)
@modal.web_endpoint(method="GET")
def health():
    """Health check — replaces /health on Render."""
    return {
        "status":           "healthy",
        "platform":         "modal",
        "images_per_video": 3,
        "out_dimensions":   f"{OUT_WIDTH}x{OUT_HEIGHT}",
        "ken_burns":        True,
        "memory_gb":        2,
    }


# ==========================================
# MAIN PIPELINE FUNCTION
# ==========================================

@app.function(
    image=image,
    secrets=secrets,
    cpu=2.0,        # 2 dedicated CPUs — ffmpeg loves this
    memory=2048,    # 2GB RAM — no more OOM
    timeout=600,    # 10 min max
)
def run_pipeline(job_id: str, topic: str, webhook_url: str = ""):
    """Full video pipeline: script → images → voice → render → YouTube."""

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    # Read env vars from Modal secret
    OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
    LEONARDO_API_KEY   = os.environ["LEONARDO_API_KEY"]
    ELEVENLABS_API_KEY = os.environ["ELEVENLABS_API_KEY"]
    VOICE_ID           = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
    SUPABASE_URL       = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY  = os.environ["SUPABASE_ANON_KEY"]
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    TEST_MODE = os.environ.get("TEST_MODE", "true").lower() == "true"

    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"TIME:  {datetime.utcnow().isoformat()}")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"{'='*60}\n")

    # ── HELPERS ────────────────────────────────────────────────────

    def update_status(status, data=None):
        try:
            payload = {"status": status, "updated_at": datetime.utcnow().isoformat()}
            if data:
                payload.update(data)
            requests.patch(
                f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal"
                },
                json=payload, timeout=10
            )
        except Exception as e:
            print(f"STATUS UPDATE FAILED: {e}")

    def log_to_db(message):
        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/render_logs",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                    "Content-Type": "application/json"
                },
                json={"job_id": job_id, "message": str(message)[:500]},
                timeout=5
            )
        except Exception:
            pass

    def get_audio_duration(path):
        try:
            r = subprocess.run([
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", path
            ], capture_output=True, text=True, timeout=10)
            return float(r.stdout.strip())
        except Exception:
            return 27.0

    def escape_dt(text):
        text = text.replace('\\', '\\\\')
        text = text.replace("'", "\u2019")
        text = text.replace(':', '\\:')
        text = text.replace('%', '\\%')
        return text

    # ── SCRIPT ─────────────────────────────────────────────────────

    def generate_script():
        print("SCRIPT START")
        prompt = f"""You are a viral YouTube Shorts scriptwriter for India20Sixty.

Topic: {topic}

Write exactly 8 punchy lines:
- Line 1: Shocking hook (6-10 words)
- Lines 2-3: Bold predictions about the near future
- Lines 4-6: Vivid specific details
- Line 7: Emotional payoff
- Line 8: Question that demands a comment
- 70% English + 30% Hinglish
- Do NOT say "2060" — use "near future", "by 2035", "soon"
- No numbering, no labels, max 12 words per line"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.85, "max_tokens": 400},
                timeout=30
            )
            r.raise_for_status()
            raw   = r.json()["choices"][0]["message"]["content"].strip()
            lines = [re.sub(r'^[\d]+[.)]\s*', '', l.strip()) for l in raw.split('\n') if l.strip()]
            script = ' '.join(lines)
            print(f"SCRIPT DONE ({len(lines)} lines): {script[:100]}...")
            return script, lines
        except Exception as e:
            print(f"SCRIPT FAILED: {e}")
            fallback = [
                f"Yeh soch lo — {topic} reality ban raha hai.",
                "India ka future ab sirf sapna nahi.",
                "Hamare engineers iss par kaam kar rahe hain.",
                "Ek dasak mein sab kuch badal jayega.",
                "Technology aur tradition ka perfect blend.",
                "Duniya dekh rahi hai, hum lead kar rahe hain.",
                "Yeh sirf shuruaat hai — best abhi aana baaki hai.",
                "Kya aap is future ka hissa banana chahte hain?"
            ]
            return ' '.join(fallback), fallback

    # ── CAPTIONS ───────────────────────────────────────────────────

    def extract_captions(script_lines):
        full = ' '.join(script_lines)
        prompt = f"""Extract exactly 9 ultra-short caption phrases from this script.
Rules: 3-5 words each, ALL CAPS, punchy, in order, no punctuation except ! or ?
Output exactly 9 lines, one phrase per line only.
Script: {full}"""
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.3, "max_tokens": 200},
                timeout=20
            )
            r.raise_for_status()
            raw      = r.json()["choices"][0]["message"]["content"].strip()
            captions = [re.sub(r'^[\d]+[.)]\s*', '', l.strip()).upper() for l in raw.split('\n') if l.strip()]
            captions = captions[:9]
            while len(captions) < 9:
                captions.append(captions[-1] if captions else "INDIA KA FUTURE")
            print(f"CAPTIONS: {captions}")
            return captions
        except Exception as e:
            print(f"CAPTION FAILED: {e}")
            words = full.upper().split()
            caps, step = [], max(1, len(words) // 9)
            for i in range(9):
                chunk = words[i * step: i * step + 4]
                caps.append(' '.join(chunk) if chunk else "INDIA KA FUTURE")
            return caps[:9]

    # ── IMAGES ─────────────────────────────────────────────────────

    def poll_for_image(generation_id, output_path):
        for poll in range(80):
            time.sleep(3)
            if poll % 5 == 0:
                print(f"  Polling {poll * 3}s...")
            try:
                r = requests.get(
                    f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"}, timeout=15
                )
                r.raise_for_status()
                gen    = r.json().get("generations_by_pk", {})
                status = gen.get("status", "UNKNOWN")
                if status == "FAILED":
                    raise Exception(f"Leonardo FAILED: {str(gen)[:200]}")
                if status == "COMPLETE":
                    images = gen.get("generated_images", [])
                    if not images:
                        raise Exception("COMPLETE but no images")
                    img_r = requests.get(images[0]["url"], timeout=30)
                    img_r.raise_for_status()
                    with open(output_path, "wb") as f:
                        f.write(img_r.content)
                    size = os.path.getsize(output_path)
                    print(f"  Saved: {size // 1024}KB")
                    return True
            except Exception as e:
                if "FAILED" in str(e) or "COMPLETE" in str(e):
                    raise
        raise Exception("Timeout: no image after 240s")

    def generate_image(prompt, output_path):
        last_error = None
        for model_id in LEONARDO_MODELS:
            try:
                print(f"  Trying model: {model_id}")
                r = requests.post(
                    "https://cloud.leonardo.ai/api/rest/v1/generations",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}", "Content-Type": "application/json"},
                    json={"prompt": prompt, "modelId": model_id, "width": IMG_WIDTH,
                          "height": IMG_HEIGHT, "num_images": 1, "presetStyle": "CINEMATIC"},
                    timeout=30
                )
                if r.status_code != 200:
                    raise Exception(f"{r.status_code}: {r.text[:200]}")
                data = r.json()
                if "sdGenerationJob" not in data:
                    raise Exception(f"No sdGenerationJob: {str(data)[:200]}")
                gen_id = data["sdGenerationJob"]["generationId"]
                print(f"  Generation ID: {gen_id}")
                return poll_for_image(gen_id, output_path)
            except Exception as e:
                last_error = e
                print(f"  Model failed: {str(e)[:150]}")
                time.sleep(5)
        raise Exception(f"All models failed. Last: {last_error}")

    def generate_all_images():
        scenes = [
            {"prompt": SCENE_TEMPLATES[0], "duration": 9},
            {"prompt": SCENE_TEMPLATES[1], "duration": 9},
            {"prompt": SCENE_TEMPLATES[2], "duration": 9},
        ]
        image_paths = []
        for i, scene in enumerate(scenes):
            update_status("images")
            print(f"\n[Image {i+1}/3]")
            path = f"{TMP_DIR}/{job_id}_{i}.png"
            if i > 0:
                time.sleep(8)
            try:
                generate_image(scene["prompt"], path)
                image_paths.append(path)
            except Exception as e:
                print(f"Image {i+1} failed, fallback: {e}")
                if image_paths:
                    shutil.copy(image_paths[-1], path)
                else:
                    subprocess.run(["ffmpeg", "-y", "-f", "lavfi",
                        "-i", f"color=c=0x1a1a2e:s={IMG_WIDTH}x{IMG_HEIGHT}:d=1",
                        "-frames:v", "1", path], capture_output=True, timeout=15)
                image_paths.append(path)
        return image_paths

    # ── VOICE ──────────────────────────────────────────────────────

    def generate_voice(script):
        update_status("voice")
        print("\n[Voice]")
        r = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
            json={"text": script, "model_id": "eleven_multilingual_v2",
                  "voice_settings": {"stability": 0.5, "similarity_boost": 0.75}},
            timeout=60
        )
        r.raise_for_status()
        raw_path   = f"{TMP_DIR}/{job_id}_raw.mp3"
        audio_path = f"{TMP_DIR}/{job_id}.mp3"
        with open(raw_path, "wb") as f:
            f.write(r.content)
        duration = get_audio_duration(raw_path)
        print(f"  Raw audio: {duration:.1f}s")
        if duration < 24.0:
            subprocess.run(["ffmpeg", "-y", "-i", raw_path,
                "-af", f"apad=pad_dur={25.0 - duration}", "-t", "25",
                audio_path], capture_output=True, timeout=20)
            os.remove(raw_path)
        else:
            os.rename(raw_path, audio_path)
        print(f"  Final audio: {get_audio_duration(audio_path):.1f}s")
        return audio_path

    # ── RENDER ─────────────────────────────────────────────────────
    # On Modal we have dedicated CPU so Ken Burns works fine

    def render_scene_clip(img_path, duration, scene_idx, captions):
        clip_path = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
        third     = duration / 3.0

        # Ken Burns: scale 10% larger, slow drift crop
        kb_w = int(OUT_WIDTH  * 1.10)
        kb_h = int(OUT_HEIGHT * 1.10)
        dx, dy = kb_w - OUT_WIDTH, kb_h - OUT_HEIGHT

        drifts = [
            (f"'min({dx}*t/{duration},{dx})'", f"'min({dy}*t/{duration},{dy})'"),
            (f"'{dx}-min({dx}*t/{duration},{dx})'", f"'min({dy}*t/{duration},{dy})'"),
            (f"'min({dx}*t/{duration},{dx})'", f"'{dy}-min({dy}*t/{duration},{dy})'"),
        ]
        x_expr, y_expr = drifts[scene_idx % 3]

        scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
        while len(scene_caps) < 3:
            scene_caps.append("")

        cap_y, cap_size = int(OUT_HEIGHT * 0.73), 58

        vf_parts = [
            f"scale={kb_w}:{kb_h}:force_original_aspect_ratio=increase",
            f"crop={OUT_WIDTH}:{OUT_HEIGHT}:{x_expr}:{y_expr}",
            "setsar=1",
        ]

        for ci, cap in enumerate(scene_caps):
            cap = cap.strip()
            if not cap:
                continue
            escaped = escape_dt(cap)
            vf_parts.append(
                f"drawtext=text='{escaped}':fontsize={cap_size}:fontcolor=white"
                f":borderw=5:bordercolor=black@0.9"
                f":x=(w-text_w)/2:y={cap_y}"
                f":enable='between(t,{ci*third:.3f},{(ci+1)*third:.3f})'"
            )

        cmd = [
            "ffmpeg", "-y", "-loop", "1", "-r", str(FPS),
            "-i", img_path,
            "-vf", ",".join(vf_parts),
            "-t", str(duration), "-r", str(FPS),
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-pix_fmt", "yuv420p", clip_path
        ]

        print(f"  Rendering clip {scene_idx} ({duration:.1f}s)...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise Exception(f"Clip {scene_idx} failed: {result.stderr[-200:]}")

        size = os.path.getsize(clip_path)
        print(f"  Clip {scene_idx}: {size // 1024}KB")
        return clip_path

    def render_video(images, audio, captions):
        update_status("render")
        print("\n[Render]")
        audio_dur  = get_audio_duration(audio)
        total_dur  = max(audio_dur, 25.0)
        scene_dur  = total_dur / len(images)
        video_path = f"{TMP_DIR}/{job_id}.mp4"
        print(f"  Audio: {audio_dur:.1f}s | {len(images)} scenes x {scene_dur:.1f}s")

        clip_paths = []
        for i, img in enumerate(images):
            clip = render_scene_clip(img, scene_dur, i, captions)
            clip_paths.append(clip)
            try: os.remove(img)
            except Exception: pass

        list_path   = f"{TMP_DIR}/{job_id}_clips.txt"
        concat_path = f"{TMP_DIR}/{job_id}_concat.mp4"
        with open(list_path, "w") as f:
            for cp in clip_paths:
                f.write(f"file '{cp}'\n")

        result = subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", list_path, "-c", "copy", concat_path
        ], capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise Exception(f"Concat failed: {result.stderr[-150:]}")

        for cp in clip_paths:
            try: os.remove(cp)
            except Exception: pass
        try: os.remove(list_path)
        except Exception: pass

        cmd = [
            "ffmpeg", "-y",
            "-i", concat_path, "-i", audio,
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-shortest", "-movflags", "+faststart", video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        try: os.remove(concat_path)
        except Exception: pass
        if result.returncode != 0:
            raise Exception(f"Mux failed: {result.stderr[-200:]}")

        size = os.path.getsize(video_path)
        if size < 100_000:
            raise Exception(f"Video too small: {size}")
        print(f"  Final video: {size // 1024}KB")
        return video_path

    # ── YOUTUBE ────────────────────────────────────────────────────

    def upload_to_youtube(video_path, title, script):
        update_status("upload")
        print("\n[Upload]")
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id": YOUTUBE_CLIENT_ID, "client_secret": YOUTUBE_CLIENT_SECRET,
                  "refresh_token": YOUTUBE_REFRESH_TOKEN, "grant_type": "refresh_token"},
            timeout=10
        )
        r.raise_for_status()
        token = r.json()["access_token"]

        metadata = {
            "snippet": {
                "title": title[:100],
                "description": f"{script}\n\nIndia20Sixty - Exploring India's near future.\n\n#IndiaFuture #FutureTech #India #Shorts #AI #Technology",
                "tags": ["Future India", "India innovation", "AI", "Technology", "Shorts"],
                "categoryId": "28"
            },
            "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False}
        }

        with open(video_path, "rb") as vf:
            r = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",
                headers={"Authorization": f"Bearer {token}"},
                files={"snippet": (None, json.dumps(metadata), "application/json"),
                       "video": ("video.mp4", vf, "video/mp4")},
                timeout=300
            )
        r.raise_for_status()
        video_id = r.json()["id"]
        print(f"  YouTube: https://youtube.com/watch?v={video_id}")
        return video_id

    # ── RUN ────────────────────────────────────────────────────────

    try:
        update_status("processing", {"topic": topic})
        log_to_db("Pipeline started on Modal")

        script, script_lines = generate_script()
        log_to_db(f"Script ({len(script_lines)} lines): {script[:80]}")

        captions = extract_captions(script_lines)
        log_to_db(f"Captions: {captions[:3]}")

        images = generate_all_images()
        log_to_db(f"Images: {len(images)}")

        audio = generate_voice(script)
        log_to_db("Voice done")

        video = render_video(images, audio, captions)
        log_to_db("Video rendered")

        if TEST_MODE:
            print(f"\nTEST MODE — skipping YouTube upload")
            video_id, final_status = "TEST_MODE", "test_complete"
        else:
            title        = f"{topic} | India20Sixty #Shorts"
            video_id     = upload_to_youtube(video, title, script)
            final_status = "complete"
            log_to_db(f"Uploaded: {video_id}")

        # Write to videos table
        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/videos",
                headers={"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json", "Prefer": "return=minimal"},
                json={"job_id": job_id, "topic": topic,
                      "youtube_url": f"https://youtube.com/watch?v={video_id}" if video_id != "TEST_MODE" else None},
                timeout=10
            )
        except Exception as e:
            print(f"videos insert (non-fatal): {e}")

        update_status(final_status, {
            "youtube_id": video_id,
            "script_package": {"text": script, "lines": script_lines,
                               "captions": captions, "generated_at": datetime.utcnow().isoformat()}
        })

        for f in [audio, video]:
            try: os.remove(f)
            except Exception: pass

        print(f"\nPIPELINE COMPLETE: {video_id}\n")

    except Exception as e:
        msg = str(e)
        print(f"\nPIPELINE FAILED: {msg}\n{traceback.format_exc()}")
        log_to_db(f"FAILED: {msg[:400]}")
        update_status("failed", {"error": msg[:400]})
        raise


# ==========================================
# LOCAL TEST ENTRY POINT
# Run with: modal run pipeline.py
# ==========================================

@app.local_entrypoint()
def main():
    print("Running test pipeline via Modal...")
    run_pipeline.remote(
        job_id="local-test-001",
        topic="AI doctors transforming rural India",
        webhook_url=""
    )
