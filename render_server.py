from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json
import time
import shutil
import uuid
import traceback
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

# ==========================================
# ENVIRONMENT
# ==========================================

OPENAI_API_KEY     = os.environ.get("OPENAI_API_KEY")
LEONARDO_API_KEY   = os.environ.get("LEONARDO_API_KEY")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID           = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")

SUPABASE_URL      = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

R2_ENDPOINT   = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET     = os.environ.get("R2_BUCKET")

TEST_MODE = os.environ.get("TEST_MODE", "true").lower() == "true"
TMP_DIR   = "/tmp/india20sixty"

Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

# ==========================================
# STATUS UPDATES
# ==========================================

def update_status(job_id, status, data=None):
    try:
        payload = {
            "status":     status,
            "updated_at": datetime.utcnow().isoformat()
        }
        if data:
            payload.update(data)
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
            headers={
                "apikey":        SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "return=minimal"
            },
            json=payload,
            timeout=10
        )
    except Exception as e:
        print(f"STATUS UPDATE FAILED: {e}")


def log_step(job_id, step, message):
    print(f"[{job_id}] {step}: {message}")
    update_status(job_id, step.lower())
    log_to_db(job_id, f"{step}: {message}")


def log_to_db(job_id, message):
    try:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/render_logs",
            headers={
                "apikey":        SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type":  "application/json"
            },
            json={"job_id": job_id, "message": str(message)[:500]},
            timeout=5
        )
    except Exception:
        pass

# ==========================================
# SCRIPT GENERATION
# ==========================================

def generate_script(topic):
    print("SCRIPT START")
    prompt = f"""Create viral 25-second YouTube Shorts script: {topic}

70% English, 30% Hinglish. Short sentences. Hook in first 3 seconds. End with question.

Return ONLY script text, no labels, 40-50 words."""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type":  "application/json"
            },
            json={
                "model":       "gpt-4o-mini",
                "messages":    [{"role": "user", "content": prompt}],
                "temperature": 0.8,
                "max_tokens":  200
            },
            timeout=30
        )
        r.raise_for_status()
        text   = r.json()["choices"][0]["message"]["content"].strip()
        text   = text.replace('"', '').replace("'", "")
        lines  = [line.strip() for line in text.split('\n') if line.strip()]
        script = ' '.join(lines)
        print(f"SCRIPT DONE: {script[:80]}...")
        return script
    except Exception as e:
        print(f"SCRIPT FAILED: {e}")
        return f"Socho, {topic} reality ban jaye. India 2060 mein yeh normal. Comment karo!"

# ==========================================
# VISUAL SCENES
# ==========================================

SCENE_TEMPLATES = [
    "futuristic Indian city skyline 2060, lotus-shaped buildings, flying vehicles, saffron teal colors, cinematic lighting, ultra realistic, 8k",
    "advanced AI technology India, engineers in smart clothing, holographic displays, temple architecture blend, dramatic lighting, cinematic",
    "proud Indian future 2060, diverse people, advanced infrastructure, sunset colors, patriotic, cinematic wide shot, ultra realistic, 8k"
]

def generate_visual_scenes(topic):
    return [
        {"stage": "hook",    "prompt": SCENE_TEMPLATES[0], "duration": 8},
        {"stage": "insight", "prompt": SCENE_TEMPLATES[1], "duration": 9},
        {"stage": "ending",  "prompt": SCENE_TEMPLATES[2], "duration": 8}
    ]

# ==========================================
# LEONARDO IMAGES
# ==========================================

def generate_image_with_retry(prompt, output_path, job_id, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"[{job_id}] Leonardo attempt {attempt + 1}/{max_retries}")

            r = requests.post(
                "https://cloud.leonardo.ai/api/rest/v1/generations",
                headers={
                    "Authorization": f"Bearer {LEONARDO_API_KEY}",
                    "Content-Type":  "application/json"
                },
                json={
                    "prompt":      prompt,
                    "modelId":     "b24e16ff-06e3-43eb-8d33-4416c2d75876",
                    "width":       1080,
                    "height":      1920,
                    "num_images":  1,
                    "presetStyle": "CINEMATIC"
                },
                timeout=30
            )

            if r.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"[{job_id}] Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = r.json()
            print(f"[{job_id}] Leonardo response keys: {list(data.keys())}")

            if "sdGenerationJob" not in data:
                raise Exception(f"No sdGenerationJob in response: {str(data)[:300]}")

            generation_id = data["sdGenerationJob"]["generationId"]
            print(f"[{job_id}] Generation ID: {generation_id}")

            for poll in range(60):
                time.sleep(3)
                if poll % 5 == 0:
                    print(f"[{job_id}] Polling {poll * 3}s...")

                poll_r = requests.get(
                    f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                    timeout=15
                )
                poll_r.raise_for_status()
                result = poll_r.json()

                gen    = result.get("generations_by_pk", {})
                status = gen.get("status", "")
                print(f"[{job_id}] Poll {poll}: status={status}")

                if status == "FAILED":
                    raise Exception(f"Leonardo generation FAILED: {str(gen)[:200]}")

                images = gen.get("generated_images", [])
                if images and len(images) > 0:
                    img_url = images[0]["url"]
                    print(f"[{job_id}] Image URL: {img_url[:80]}")
                    img_r = requests.get(img_url, timeout=30)
                    img_r.raise_for_status()
                    with open(output_path, "wb") as f:
                        f.write(img_r.content)
                    size = os.path.getsize(output_path)
                    print(f"[{job_id}] Image saved: {size // 1024}KB")
                    if size < 1000:
                        raise Exception("Image file suspiciously small")
                    return True

            raise Exception("Generation timeout — no images after 180s")

        except Exception as e:
            print(f"[{job_id}] Attempt {attempt + 1} failed: {str(e)[:200]}")
            if attempt < max_retries - 1:
                time.sleep(15)
            else:
                raise

    return False


def generate_all_images(scenes, job_id):
    image_paths = []
    for i, scene in enumerate(scenes):
        log_step(job_id, "IMAGES", f"Scene {i + 1}/{len(scenes)}")
        path = f"{TMP_DIR}/{job_id}_{i}.png"
        if i > 0:
            time.sleep(10)
        try:
            generate_image_with_retry(scene["prompt"], path, job_id)
            image_paths.append(path)
        except Exception as e:
            print(f"[{job_id}] Image {i + 1} failed: {e}")
            if image_paths:
                shutil.copy(image_paths[-1], path)
            else:
                create_placeholder_image(path)
            image_paths.append(path)
    return image_paths


def create_placeholder_image(path):
    try:
        subprocess.run([
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", "color=c=blue:s=1080x1920:d=1",
            "-vf", "drawtext=text='India20Sixty':fontsize=60:fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2",
            "-frames:v", "1", path
        ], capture_output=True, timeout=10)
    except Exception as e:
        print(f"Placeholder failed: {e}")
        Path(path).touch()

# ==========================================
# VOICE GENERATION
# ==========================================

def generate_voice(script, job_id):
    log_step(job_id, "VOICE", "Generating audio...")
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
        headers={
            "xi-api-key":   ELEVENLABS_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "text":     script,
            "model_id": "eleven_multilingual_v2",
            "voice_settings": {
                "stability":        0.5,
                "similarity_boost": 0.75
            }
        },
        timeout=60
    )
    r.raise_for_status()
    audio_path = f"{TMP_DIR}/{job_id}.mp3"
    with open(audio_path, "wb") as f:
        f.write(r.content)
    return audio_path

# ==========================================
# VIDEO RENDERING
# ==========================================

def get_audio_duration(audio_path):
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            audio_path
        ], capture_output=True, text=True, timeout=10)
        return float(result.stdout.strip())
    except Exception:
        return 25.0


def render_video(images, audio, job_id):
    log_step(job_id, "RENDER", "Rendering video...")
    video_path     = f"{TMP_DIR}/{job_id}.mp4"
    audio_duration = get_audio_duration(audio)
    scene_duration = audio_duration / len(images)

    inputs       = []
    filter_parts = []

    for i, img in enumerate(images):
        inputs.extend(["-loop", "1", "-t", str(scene_duration), "-i", img])
        zoom_end = 1.1
        x_end    = (i % 2) * 15
        filter_parts.append(
            f"[{i}:v]scale=1080:1920:force_original_aspect_ratio=decrease,"
            f"zoompan=z='min(zoom+0.001,{zoom_end})':"
            f"x='iw/2-(iw/zoom/2)+{x_end}*on/{int(scene_duration*30)}':"
            f"y='ih/2-(ih/zoom/2)':"
            f"d={int(scene_duration * 30)}:s=1080x1920[v{i}];"
        )

    concat_inputs = "".join([f"[v{i}]" for i in range(len(images))])
    filter_parts.append(f"{concat_inputs}concat=n={len(images)}:v=1:a=0[video];")
    audio_index = len(images)

    cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-i", audio,
        "-filter_complex", "".join(filter_parts) + f"[{audio_index}:a]anull[audio]",
        "-map", "[video]",
        "-map", "[audio]",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-shortest",
        "-movflags", "+faststart",
        video_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)

    if result.returncode != 0:
        print(f"FFMPEG STDERR: {result.stderr[-800:]}")
        raise Exception(f"ffmpeg failed: {result.stderr[-200:]}")

    if not os.path.exists(video_path) or os.path.getsize(video_path) < 100_000:
        raise Exception("Video file missing or too small")

    print(f"[{job_id}] Video rendered: {os.path.getsize(video_path) // 1024}KB")
    return video_path

# ==========================================
# YOUTUBE UPLOAD
# ==========================================

def get_youtube_token():
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     YOUTUBE_CLIENT_ID,
            "client_secret": YOUTUBE_CLIENT_SECRET,
            "refresh_token": YOUTUBE_REFRESH_TOKEN,
            "grant_type":    "refresh_token"
        },
        timeout=10
    )
    r.raise_for_status()
    return r.json()["access_token"]


def upload_to_youtube(video_path, title, script, job_id):
    log_step(job_id, "UPLOAD", "Uploading to YouTube...")
    token = get_youtube_token()

    description = f"""{script}

India20Sixty - Exploring India's future by 2060

#India2060 #FutureTech #IndiaFuture #Shorts #AI #Technology

Follow for daily glimpses into India's future!"""

    metadata = {
        "snippet": {
            "title":       title[:100],
            "description": description[:5000],
            "tags":        ["India 2060", "Future India", "AI", "Technology", "Shorts"],
            "categoryId":  "28"
        },
        "status": {
            "privacyStatus":           "public",
            "selfDeclaredMadeForKids": False
        }
    }

    with open(video_path, "rb") as video_file:
        r = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",
            headers={"Authorization": f"Bearer {token}"},
            files={
                "snippet": (None, json.dumps(metadata), "application/json"),
                "video":   ("video.mp4", video_file, "video/mp4")
            },
            timeout=300
        )

    r.raise_for_status()
    video_id = r.json()["id"]
    print(f"[{job_id}] Uploaded: https://youtube.com/watch?v={video_id}")
    return video_id

# ==========================================
# MAIN PIPELINE
# ==========================================

@app.route("/full-pipeline", methods=["POST"])
def pipeline():
    data    = request.json or {}
    job_id  = data.get("job_id") or str(uuid.uuid4())
    topic   = data.get("topic", "Future India")

    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"TIME:  {datetime.utcnow().isoformat()}")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"{'='*60}\n")

    try:
        update_status(job_id, "processing", {"topic": topic})
        log_to_db(job_id, "Pipeline started")

        script = generate_script(topic)
        log_to_db(job_id, f"Script: {script[:80]}")

        scenes = generate_visual_scenes(topic)
        images = generate_all_images(scenes, job_id)
        log_to_db(job_id, f"Images done: {len(images)}")

        audio = generate_voice(script, job_id)
        log_to_db(job_id, "Voice done")

        video = render_video(images, audio, job_id)
        log_to_db(job_id, "Video rendered")

        if TEST_MODE:
            print(f"[{job_id}] TEST MODE — skipping YouTube upload")
            video_id     = "TEST_MODE"
            final_status = "test_complete"
        else:
            title        = f"{topic} | India20Sixty #Shorts"
            video_id     = upload_to_youtube(video, title, script, job_id)
            final_status = "complete"
            log_to_db(job_id, f"Uploaded: {video_id}")

        # Write to videos table
        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/videos",
                headers={
                    "apikey":        SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                    "Content-Type":  "application/json",
                    "Prefer":        "return=minimal"
                },
                json={
                    "job_id":      job_id,
                    "topic":       topic,
                    "youtube_url": f"https://youtube.com/watch?v={video_id}" if video_id != "TEST_MODE" else None
                },
                timeout=10
            )
        except Exception as e:
            print(f"[{job_id}] videos insert failed (non-fatal): {e}")

        update_status(job_id, final_status, {
            "youtube_id":     video_id,
            "script_package": {"text": script, "generated_at": datetime.utcnow().isoformat()}
        })

        # Cleanup
        for img in images:
            try: os.remove(img)
            except Exception: pass
        try: os.remove(audio)
        except Exception: pass

        print(f"\nPIPELINE COMPLETE: {video_id}\n")
        return jsonify({"status": final_status, "job_id": job_id, "youtube_id": video_id, "script": script})

    except Exception as e:
        tb  = traceback.format_exc()
        msg = str(e)
        print(f"\nPIPELINE FAILED: {msg}\n{tb}")
        log_to_db(job_id, f"FAILED: {msg[:400]}")
        update_status(job_id, "failed", {"error": msg[:400]})
        return jsonify({"status": "failed", "job_id": job_id, "error": msg}), 500

# ==========================================
# HEALTH
# ==========================================

@app.route("/health")
def health():
    return jsonify({
        "status":           "healthy",
        "test_mode":        TEST_MODE,
        "images_per_video": 3
    })

@app.route("/")
def home():
    return jsonify({"status": "india20sixty render pipeline running"})

# ==========================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
