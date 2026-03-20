from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json
import time
import shutil
import uuid
import traceback
import re
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

TEST_MODE = os.environ.get("TEST_MODE", "true").lower() == "true"
TMP_DIR   = "/tmp/india20sixty"
LOGO_PATH = os.path.join(os.path.dirname(__file__), "India20Sixty_logo.png")

Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

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

# ==========================================
# STATUS + LOGGING
# ==========================================

def update_status(job_id, status, data=None):
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


def log_step(job_id, step, message):
    print(f"[{job_id}] {step}: {message}")
    update_status(job_id, step.lower())
    log_to_db(job_id, f"{step}: {message}")


def log_to_db(job_id, message):
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

# ==========================================
# SCRIPT GENERATION
# ==========================================

def generate_script(topic):
    print("SCRIPT START")
    prompt = f"""You are a viral YouTube Shorts scriptwriter for India20Sixty — a channel about India's near future.

Topic: {topic}

Write exactly 8 punchy lines. Rules:
- Line 1: Shocking hook (6-10 words) — make people stop scrolling
- Lines 2-3: Bold surprising predictions about the near future
- Lines 4-6: Vivid specific details that paint the picture
- Line 7: Emotional payoff — pride, wonder, excitement
- Line 8: Engaging question that demands a comment
- Mix 70% English + 30% Hinglish naturally
- Do NOT say "2060" — use "near future", "by 2035", "soon", "within a decade"
- No numbering, no labels, no bullets
- One sentence per line, max 12 words each"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.85,
                "max_tokens": 400
            },
            timeout=30
        )
        r.raise_for_status()
        raw   = r.json()["choices"][0]["message"]["content"].strip()
        lines = [re.sub(r'^[\d]+[.)]\s*', '', l.strip()) for l in raw.split('\n') if l.strip()]
        script = ' '.join(lines)
        print(f"SCRIPT DONE ({len(lines)} lines): {script[:120]}...")
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

# ==========================================
# CAPTION EXTRACTION
# ==========================================

def extract_captions(script_lines):
    full_script = ' '.join(script_lines)
    prompt = f"""Extract exactly 9 ultra-short caption phrases from this script for a YouTube Short.

Rules:
- 3-5 words each
- ALL CAPS
- Punchy and bold
- In script order
- No punctuation except ! or ?
- Output exactly 9 lines, one phrase per line, nothing else

Script: {full_script}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 200
            },
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
        words = full_script.upper().split()
        caps  = []
        step  = max(1, len(words) // 9)
        for i in range(9):
            chunk = words[i * step: i * step + 4]
            caps.append(' '.join(chunk) if chunk else "INDIA KA FUTURE")
        return caps[:9]

# ==========================================
# VISUAL SCENES
# ==========================================

SCENE_TEMPLATES = [
    "futuristic Indian megacity at golden hour, lotus-shaped towers, clean electric air taxis, warm saffron teal palette, cinematic ultra-realistic photography, 8k",
    "Indian scientists and engineers in smart traditional attire, holographic data displays, ancient temple architecture meets gleaming research campus, dramatic cinematic lighting",
    "aerial view of transformed green India, solar farms and vertical gardens, thriving diverse communities, Indian tricolor, hopeful sunrise, epic cinematic wide shot"
]

def generate_visual_scenes(topic):
    return [
        {"stage": "hook",    "prompt": SCENE_TEMPLATES[0], "duration": 9},
        {"stage": "insight", "prompt": SCENE_TEMPLATES[1], "duration": 9},
        {"stage": "ending",  "prompt": SCENE_TEMPLATES[2], "duration": 9}
    ]

# ==========================================
# LEONARDO IMAGES
# ==========================================

def try_generate_with_model(model_id, prompt, job_id):
    print(f"[{job_id}] Trying model: {model_id}")
    r = requests.post(
        "https://cloud.leonardo.ai/api/rest/v1/generations",
        headers={"Authorization": f"Bearer {LEONARDO_API_KEY}", "Content-Type": "application/json"},
        json={
            "prompt": prompt, "modelId": model_id,
            "width": IMG_WIDTH, "height": IMG_HEIGHT,
            "num_images": 1, "presetStyle": "CINEMATIC"
        },
        timeout=30
    )
    print(f"[{job_id}] Response: {r.status_code}")
    if r.status_code != 200:
        raise Exception(f"{r.status_code}: {r.text[:200]}")
    data = r.json()
    if "sdGenerationJob" not in data:
        raise Exception(f"No sdGenerationJob: {str(data)[:200]}")
    return data["sdGenerationJob"]["generationId"]


def poll_for_image(generation_id, output_path, job_id):
    for poll in range(80):
        time.sleep(3)
        if poll % 5 == 0:
            print(f"[{job_id}] Polling {poll * 3}s...")
        try:
            r = requests.get(
                f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
                headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                timeout=15
            )
            r.raise_for_status()
            gen    = r.json().get("generations_by_pk", {})
            status = gen.get("status", "UNKNOWN")
            print(f"[{job_id}] Poll {poll}: status={status}")
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
                print(f"[{job_id}] Saved: {size // 1024}KB")
                if size < 5000:
                    raise Exception(f"Image too small: {size}")
                return True
        except Exception as e:
            if "FAILED" in str(e) or "COMPLETE" in str(e):
                raise
            print(f"[{job_id}] Poll error: {e}")
    raise Exception("Timeout: no image after 240s")


def generate_image_with_retry(prompt, output_path, job_id):
    last_error = None
    for model_id in LEONARDO_MODELS:
        try:
            gen_id = try_generate_with_model(model_id, prompt, job_id)
            return poll_for_image(gen_id, output_path, job_id)
        except Exception as e:
            last_error = e
            print(f"[{job_id}] Model failed: {str(e)[:150]}")
            time.sleep(5)
    raise Exception(f"All models failed. Last: {last_error}")


def generate_all_images(scenes, job_id):
    image_paths = []
    for i, scene in enumerate(scenes):
        log_step(job_id, "IMAGES", f"Scene {i + 1}/{len(scenes)}")
        path = f"{TMP_DIR}/{job_id}_{i}.png"
        if i > 0:
            time.sleep(8)
        try:
            generate_image_with_retry(scene["prompt"], path, job_id)
            image_paths.append(path)
        except Exception as e:
            print(f"[{job_id}] Image {i + 1} failed, fallback: {e}")
            if image_paths:
                shutil.copy(image_paths[-1], path)
            else:
                create_placeholder_image(path)
            image_paths.append(path)
    return image_paths


def create_placeholder_image(path):
    try:
        subprocess.run([
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", f"color=c=0x1a1a2e:s={OUT_WIDTH}x{OUT_HEIGHT}:d=1",
            "-vf", f"drawtext=text='India20Sixty':fontsize=80:fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2",
            "-frames:v", "1", path
        ], capture_output=True, timeout=15)
    except Exception as e:
        print(f"Placeholder failed: {e}")
        Path(path).touch()

# ==========================================
# VOICE GENERATION
# ==========================================

def get_audio_duration(path):
    try:
        r = subprocess.run([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", path
        ], capture_output=True, text=True, timeout=10)
        return float(r.stdout.strip())
    except Exception:
        return 27.0


def generate_voice(script, job_id):
    log_step(job_id, "VOICE", "Generating audio...")
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
        headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
        json={
            "text": script,
            "model_id": "eleven_multilingual_v2",
            "voice_settings": {"stability": 0.5, "similarity_boost": 0.75}
        },
        timeout=60
    )
    r.raise_for_status()

    raw_path   = f"{TMP_DIR}/{job_id}_raw.mp3"
    audio_path = f"{TMP_DIR}/{job_id}.mp3"
    with open(raw_path, "wb") as f:
        f.write(r.content)

    duration = get_audio_duration(raw_path)
    print(f"[{job_id}] Raw audio: {duration:.1f}s")

    if duration < 24.0:
        pad = 25.0 - duration
        print(f"[{job_id}] Padding {pad:.1f}s")
        subprocess.run([
            "ffmpeg", "-y", "-i", raw_path,
            "-af", f"apad=pad_dur={pad}", "-t", "25",
            audio_path
        ], capture_output=True, timeout=20)
        os.remove(raw_path)
    else:
        os.rename(raw_path, audio_path)

    print(f"[{job_id}] Final audio: {get_audio_duration(audio_path):.1f}s")
    return audio_path

# ==========================================
# VIDEO RENDERING — TWO-PASS PER CLIP
#
# PASS 1 (fast, ~2s): scale+crop raw image → exact OUT_WIDTH×OUT_HEIGHT PNG
# PASS 2 (moderate, ~30s): Ken Burns on already-sized image + captions → clip
#
# This avoids the timeout caused by scaling 1188×2112 + crop + drawtext
# all in one heavy filter chain on a free-tier CPU.
# ==========================================

def escape_dt(text):
    text = text.replace('\\', '\\\\')
    text = text.replace("'", "\u2019")
    text = text.replace(':', '\\:')
    text = text.replace('%', '\\%')
    return text


def preprocess_image(src_path, dst_path, job_id):
    """
    PASS 1: Scale+crop source image to exact OUT_WIDTH×OUT_HEIGHT.
    This is a single fast operation — no animation, no text.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", src_path,
        "-vf", (
            f"scale={OUT_WIDTH}:{OUT_HEIGHT}:force_original_aspect_ratio=increase,"
            f"crop={OUT_WIDTH}:{OUT_HEIGHT}"
        ),
        "-frames:v", "1",
        dst_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise Exception(f"Preprocess failed: {result.stderr[-200:]}")
    print(f"[{job_id}] Preprocessed: {os.path.getsize(dst_path) // 1024}KB")


def render_scene_clip(preprocessed_img, duration, scene_idx, captions, job_id):
    """
    PASS 2: Ken Burns on already-sized image + captions.
    Image is already OUT_WIDTH×OUT_HEIGHT — Ken Burns = scale to 105%,
    then crop with slow drift. Much less CPU than scaling from raw.
    """
    clip_path = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
    third     = duration / 3.0

    # 5% larger than output for Ken Burns headroom
    kb_w = int(OUT_WIDTH  * 1.05)
    kb_h = int(OUT_HEIGHT * 1.05)
    dx   = kb_w - OUT_WIDTH   # 54px horizontal headroom
    dy   = kb_h - OUT_HEIGHT  # 96px vertical headroom

    # Drift direction per scene
    drifts = [
        (f"'min({dx}*t/{duration},{dx})'", f"'min({dy}*t/{duration},{dy})'"),           # right+down
        (f"'{dx}-min({dx}*t/{duration},{dx})'", f"'min({dy}*t/{duration},{dy})'"),       # left+down
        (f"'min({dx}*t/{duration},{dx})'", f"'{dy}-min({dy}*t/{duration},{dy})'"),       # right+up
    ]
    x_expr, y_expr = drifts[scene_idx % 3]

    # 3 captions for this scene
    scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
    while len(scene_caps) < 3:
        scene_caps.append("")

    cap_y    = int(OUT_HEIGHT * 0.73)
    cap_size = 56

    # Build vf chain
    vf_parts = [
        f"scale={kb_w}:{kb_h}",
        f"crop={OUT_WIDTH}:{OUT_HEIGHT}:{x_expr}:{y_expr}",
        "setsar=1",
    ]

    for ci, cap in enumerate(scene_caps):
        cap = cap.strip()
        if not cap:
            continue
        t_start = f"{ci * third:.3f}"
        t_end   = f"{(ci + 1) * third:.3f}"
        escaped = escape_dt(cap)
        vf_parts.append(
            f"drawtext=text='{escaped}'"
            f":fontsize={cap_size}"
            f":fontcolor=white"
            f":borderw=5:bordercolor=black@0.9"
            f":x=(w-text_w)/2:y={cap_y}"
            f":enable='between(t,{t_start},{t_end})'"
        )

    vf = ",".join(vf_parts)

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1",
        "-r", str(FPS),
        "-i", preprocessed_img,
        "-vf", vf,
        "-t", str(duration),
        "-r", str(FPS),
        "-c:v", "libx264",
        "-preset", "ultrafast",   # ultrafast on free tier — quality still fine for Shorts
        "-crf", "22",
        "-pix_fmt", "yuv420p",
        clip_path
    ]

    print(f"[{job_id}] Rendering clip {scene_idx} ({duration:.1f}s)...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        print(f"[{job_id}] Clip {scene_idx} stderr:\n{result.stderr[-600:]}")
        raise Exception(f"Clip {scene_idx} failed: {result.stderr[-150:]}")

    size = os.path.getsize(clip_path)
    print(f"[{job_id}] Clip {scene_idx} done: {size // 1024}KB")
    return clip_path


def concat_clips_with_audio(clip_paths, audio_path, output_path, job_id):
    list_path   = f"{TMP_DIR}/{job_id}_clips.txt"
    concat_path = f"{TMP_DIR}/{job_id}_concat.mp4"

    with open(list_path, "w") as f:
        for cp in clip_paths:
            f.write(f"file '{cp}'\n")

    print(f"[{job_id}] Concatenating {len(clip_paths)} clips...")
    result = subprocess.run([
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_path,
        "-c", "copy",
        concat_path
    ], capture_output=True, text=True, timeout=60)

    if result.returncode != 0:
        raise Exception(f"Concat failed: {result.stderr[-150:]}")

    print(f"[{job_id}] Concat: {os.path.getsize(concat_path) // 1024}KB")

    has_logo = os.path.exists(LOGO_PATH) and os.path.getsize(LOGO_PATH) > 100
    print(f"[{job_id}] Logo: {has_logo}")

    if has_logo:
        cmd = [
            "ffmpeg", "-y",
            "-i", concat_path,
            "-i", LOGO_PATH,
            "-i", audio_path,
            "-filter_complex",
            "[1:v]scale=130:-1,format=rgba,colorchannelmixer=aa=0.6[logo];"
            "[0:v][logo]overlay=x=20:y=20:format=auto[vout]",
            "-map", "[vout]", "-map", "2:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-shortest", "-movflags", "+faststart",
            output_path
        ]
    else:
        cmd = [
            "ffmpeg", "-y",
            "-i", concat_path, "-i", audio_path,
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-shortest", "-movflags", "+faststart",
            output_path
        ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise Exception(f"Mux failed: {result.stderr[-200:]}")

    for cp in clip_paths:
        try: os.remove(cp)
        except Exception: pass
    try: os.remove(concat_path)
    except Exception: pass
    try: os.remove(list_path)
    except Exception: pass


def render_video(images, audio, captions, job_id):
    log_step(job_id, "RENDER", "Rendering video...")

    audio_dur  = get_audio_duration(audio)
    total_dur  = max(audio_dur, 25.0)
    scene_dur  = total_dur / len(images)
    video_path = f"{TMP_DIR}/{job_id}.mp4"

    print(f"[{job_id}] Audio: {audio_dur:.1f}s | Total: {total_dur:.1f}s | {len(images)} scenes x {scene_dur:.1f}s")

    clip_paths = []
    for i, img in enumerate(images):
        # PASS 1: pre-scale image to output size
        pre_path = f"{TMP_DIR}/{job_id}_pre{i}.png"
        preprocess_image(img, pre_path, job_id)

        # PASS 2: Ken Burns + captions → clip
        clip = render_scene_clip(pre_path, scene_dur, i, captions, job_id)
        clip_paths.append(clip)

        # Clean up preprocessed image
        try: os.remove(pre_path)
        except Exception: pass

    concat_clips_with_audio(clip_paths, audio, video_path, job_id)

    size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
    if size < 100_000:
        raise Exception(f"Final video too small: {size} bytes")

    print(f"[{job_id}] Final video: {size // 1024}KB")
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

India20Sixty - Exploring India's near future.

#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"""

    metadata = {
        "snippet": {
            "title":       title[:100],
            "description": description[:5000],
            "tags":        ["Future India", "India innovation", "AI", "Technology", "Shorts"],
            "categoryId":  "28"
        },
        "status": {
            "privacyStatus":           "public",
            "selfDeclaredMadeForKids": False
        }
    }

    with open(video_path, "rb") as vf:
        r = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",
            headers={"Authorization": f"Bearer {token}"},
            files={
                "snippet": (None, json.dumps(metadata), "application/json"),
                "video":   ("video.mp4", vf, "video/mp4")
            },
            timeout=300
        )
    r.raise_for_status()
    video_id = r.json()["id"]
    print(f"[{job_id}] YouTube: https://youtube.com/watch?v={video_id}")
    return video_id

# ==========================================
# MAIN PIPELINE
# ==========================================

@app.route("/full-pipeline", methods=["POST"])
def pipeline():
    data   = request.json or {}
    job_id = data.get("job_id") or str(uuid.uuid4())
    topic  = data.get("topic", "Future India")

    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"TIME:  {datetime.utcnow().isoformat()}")
    print(f"TEST_MODE: {TEST_MODE} | LOGO: {os.path.exists(LOGO_PATH)}")
    print(f"{'='*60}\n")

    try:
        update_status(job_id, "processing", {"topic": topic})
        log_to_db(job_id, "Pipeline started")

        script, script_lines = generate_script(topic)
        log_to_db(job_id, f"Script ({len(script_lines)} lines): {script[:80]}")

        captions = extract_captions(script_lines)
        log_to_db(job_id, f"Captions: {captions[:3]}")

        scenes = generate_visual_scenes(topic)
        images = generate_all_images(scenes, job_id)
        log_to_db(job_id, f"Images: {len(images)}")

        audio = generate_voice(script, job_id)
        log_to_db(job_id, "Voice done")

        video = render_video(images, audio, captions, job_id)
        log_to_db(job_id, "Video rendered")

        if TEST_MODE:
            print(f"[{job_id}] TEST MODE — skipping YouTube")
            video_id, final_status = "TEST_MODE", "test_complete"
        else:
            title        = f"{topic} | India20Sixty #Shorts"
            video_id     = upload_to_youtube(video, title, script, job_id)
            final_status = "complete"
            log_to_db(job_id, f"Uploaded: {video_id}")

        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/videos",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal"
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
            "youtube_id": video_id,
            "script_package": {
                "text":         script,
                "lines":        script_lines,
                "captions":     captions,
                "generated_at": datetime.utcnow().isoformat()
            }
        })

        for img in images:
            try: os.remove(img)
            except Exception: pass
        try: os.remove(audio)
        except Exception: pass
        try: os.remove(video)
        except Exception: pass

        print(f"\nPIPELINE COMPLETE: {video_id}\n")
        return jsonify({"status": final_status, "job_id": job_id, "youtube_id": video_id, "script": script, "captions": captions})

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
        "images_per_video": 3,
        "img_dimensions":   f"{IMG_WIDTH}x{IMG_HEIGHT}",
        "out_dimensions":   f"{OUT_WIDTH}x{OUT_HEIGHT}",
        "logo_present":     os.path.exists(LOGO_PATH)
    })

@app.route("/")
def home():
    return jsonify({"status": "india20sixty render pipeline running"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
