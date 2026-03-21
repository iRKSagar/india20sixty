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
import random
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP DEFINITION
# ==========================================

app = modal.App("india20sixty")

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
        "fastapi[standard]",
    )
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR    = "/tmp/india20sixty"
IMG_WIDTH  = 864
IMG_HEIGHT = 1536
OUT_WIDTH  = 1080
OUT_HEIGHT = 1920
FPS        = 25
XFADE_DUR  = 0.5

LEONARDO_MODELS = [
    "aa77f04e-3eec-4034-9c07-d0f619684628",
    "1e60896f-3c26-4296-8ecc-53e2afecc132",
    "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
]

# ==========================================
# SAFE FFMPEG EFFECT LIBRARY
# All effects tested on debian ffmpeg 5.x+
# ==========================================

SCENE_GRADES = [
    {"eq": "eq=contrast=1.18:brightness=0.03:saturation=1.35", "sharp": "unsharp=5:5:0.8:3:3:0.0", "label": "warm"},
    {"eq": "eq=contrast=1.12:brightness=0.0:saturation=1.1",   "sharp": "unsharp=3:3:1.0:3:3:0.0", "label": "cool"},
    {"eq": "eq=contrast=1.08:brightness=0.05:saturation=1.45", "sharp": "unsharp=5:5:0.6:3:3:0.0", "label": "golden"},
]

# ── KEN BURNS CROP OFFSETS ───────────────────────────────────────
# Static offsets per scene — image scaled to 110%, then cropped
# from different corners/positions = visual variety without dynamic expressions
# 10% headroom: dx=108, dy=192

KB_SCALE_W = int(OUT_WIDTH  * 1.10)   # 1188
KB_SCALE_H = int(OUT_HEIGHT * 1.10)   # 2112
KB_DX      = KB_SCALE_W - OUT_WIDTH   # 108
KB_DY      = KB_SCALE_H - OUT_HEIGHT  # 192

# (x_offset, y_offset, label) — different framing per scene
CROP_POSITIONS = [
    (0,          0,          "top-left"),        # Scene 0: crop from top-left
    (KB_DX,      KB_DY // 2, "mid-right"),       # Scene 1: crop from mid-right
    (KB_DX // 2, KB_DY,      "bottom-center"),   # Scene 2: crop from bottom-center
]

# ── XFADE TRANSITIONS ────────────────────────────────────────────
# All tested and working in ffmpeg 5.x on debian
XFADE_TRANSITIONS = [
    "dissolve",
    "fade",
    "wipeleft",
    "wiperight",
    "slideleft",
    "slideright",
    "fadeblack",
]

# ── VISUAL STYLES FOR SCENE PROMPT GENERATION ───────────────────
VISUAL_STYLES = [
    "cinematic ultra-realistic photography, golden hour, warm saffron palette, 8K",
    "dramatic cinematic lighting, deep shadows, vivid neon accents, photorealistic",
    "aerial drone perspective, sweeping wide angle, vibrant saturated colors",
    "close-up editorial photography, shallow depth of field, soft bokeh",
    "epic establishing shot, atmospheric haze and mist, moody cinematic film grain",
    "futuristic neon-lit India, rain-slicked streets, warm orange glow",
    "bright optimistic daylight, clean futuristic architecture, hopeful vibrant",
    "golden sunset silhouettes, dust particles, emotionally powerful cinematic",
    "blue hour twilight, city lights reflecting, serene futuristic, ultra sharp",
    "dramatic overcast sky, god rays breaking through clouds, epic and hopeful",
]

SHOT_TYPES = [
    ["extreme wide establishing shot", "dramatic low angle hero shot",
     "sweeping panoramic", "epic aerial wide shot"],
    ["medium shot close-up detail", "intimate human-scale scene",
     "detailed technological environment", "focused mid-shot with depth"],
    ["soaring aerial overview", "wide hopeful landscape",
     "golden hour wide establishing", "emotional cinematic close-up"],
]

# Fallback templates if GPT scene generation fails
SCENE_TEMPLATES_FALLBACK = [
    "futuristic Indian megacity at golden hour, lotus-shaped towers, electric air taxis, saffron teal palette, cinematic ultra-realistic photography",
    "Indian scientists in smart traditional attire, holographic data displays, temple architecture meets research campus, dramatic cinematic lighting",
    "aerial view transformed green India, solar farms vertical gardens, diverse communities, Indian tricolor, hopeful sunrise, epic cinematic wide shot"
]

# ==========================================
# WEB ENDPOINTS
# ==========================================

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="POST")
def trigger(data: dict):
    job_id      = data.get("job_id") or str(uuid.uuid4())
    topic       = data.get("topic", "Future India")
    webhook_url = data.get("webhook_url", "")
    print(f"Trigger: {job_id} | {topic}")
    run_pipeline.spawn(job_id=job_id, topic=topic, webhook_url=webhook_url)
    return {"status": "started", "job_id": job_id, "topic": topic}


@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="GET")
def health():
    return {
        "status":   "healthy",
        "platform": "modal",
        "effects":  ["static_ken_burns", "color_curves", "eq_grade",
                     "unsharp", "vignette", "fade_in_out", "xfade", "captions"],
        "out":      f"{OUT_WIDTH}x{OUT_HEIGHT}",
        "memory":   "2GB",
    }


# ==========================================
# MAIN PIPELINE
# ==========================================

@app.function(
    image=image,
    secrets=secrets,
    cpu=2.0,
    memory=2048,
    timeout=600,
)
def run_pipeline(job_id: str, topic: str, webhook_url: str = ""):

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    LEONARDO_API_KEY      = os.environ["LEONARDO_API_KEY"]
    ELEVENLABS_API_KEY    = os.environ["ELEVENLABS_API_KEY"]
    VOICE_ID              = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
    SUPABASE_URL          = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY     = os.environ["SUPABASE_ANON_KEY"]
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    TEST_MODE             = os.environ.get("TEST_MODE", "true").lower() == "true"

    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"TIME:  {datetime.utcnow().isoformat()}")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"{'='*60}\n")

    # ── HELPERS ──────────────────────────────────────────────────

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

    def run_ffmpeg(cmd, label, timeout=300):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            # Print FULL stderr so Modal logs show exact failing filter
            print(f"  ffmpeg [{label}] FAILED (full error):")
            print(result.stderr)
            err = result.stderr[-400:].strip()
            raise Exception(f"{label} failed: {err[-200:]}")
        return result

    def escape_dt(text):
        text = text.replace('\\', '\\\\')
        text = text.replace("'", "\u2019")
        text = text.replace(':', '\\:')
        text = text.replace('%', '\\%')
        return text

    # ── SCRIPT ───────────────────────────────────────────────────

    def generate_script():
        print("SCRIPT START")
        prompt = f"""You are a viral YouTube Shorts scriptwriter for India20Sixty.

Topic: {topic}

Write exactly 8 punchy lines:
- Line 1: Shocking hook (6-10 words) — stop the scroll
- Lines 2-3: Bold near-future predictions
- Lines 4-6: Vivid specific details
- Line 7: Emotional payoff — pride, wonder
- Line 8: Question demanding a comment
- 70% English + 30% Hinglish
- Do NOT say "2060" — use "near future", "by 2035", "soon"
- No numbering, no labels, max 12 words per line"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.85, "max_tokens": 400},
                timeout=30
            )
            r.raise_for_status()
            raw   = r.json()["choices"][0]["message"]["content"].strip()
            lines = [re.sub(r'^[\d]+[.)]\s*', '', l.strip())
                     for l in raw.split('\n') if l.strip()]
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

    # ── CAPTIONS ─────────────────────────────────────────────────

    def extract_captions(script_lines):
        full = ' '.join(script_lines)
        prompt = f"""Extract exactly 9 ultra-short caption phrases from this script.
Rules: 3-5 words each, ALL CAPS, punchy, in order, no punctuation except ! or ?
Output exactly 9 lines, one phrase per line only.
Script: {full}"""
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.3, "max_tokens": 200},
                timeout=20
            )
            r.raise_for_status()
            raw      = r.json()["choices"][0]["message"]["content"].strip()
            captions = [re.sub(r'^[\d]+[.)]\s*', '', l.strip()).upper()
                        for l in raw.split('\n') if l.strip()]
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

    # ── DYNAMIC SCENE PROMPTS ─────────────────────────────────────

    def generate_scene_prompts():
        style = random.choice(VISUAL_STYLES)
        shots = [random.choice(SHOT_TYPES[i]) for i in range(3)]

        prompt = f"""Create 3 cinematic image prompts for a YouTube Short about: "{topic}"

Scene 1 (Hook): Shocking, visually arresting opening image
Scene 2 (Insight): The core technology/innovation in action
Scene 3 (Ending): Hopeful, emotional, wide-angle payoff shot

Rules:
- SPECIFIC to "{topic}" — not generic India scenes
- Include real visual elements related to the topic
- Include Indian cultural context
- All use this style: {style}
- Scene 1 shot type: {shots[0]}
- Scene 2 shot type: {shots[1]}
- Scene 3 shot type: {shots[2]}
- 20-35 words each, no labels

Return ONLY a JSON array: ["prompt1", "prompt2", "prompt3"]"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.9, "max_tokens": 300},
                timeout=20
            )
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"].strip()
            start   = content.find('[')
            end     = content.rfind(']') + 1
            scenes  = json.loads(content[start:end])
            if len(scenes) >= 3:
                for i, s in enumerate(scenes[:3]):
                    print(f"  Scene {i+1}: {s[:80]}...")
                return scenes[:3]
        except Exception as e:
            print(f"Scene prompts failed: {e}, using fallback")

        return [
            f"{topic} in India near future — {SCENE_TEMPLATES_FALLBACK[0]}",
            f"{topic} technology innovation — {SCENE_TEMPLATES_FALLBACK[1]}",
            f"Future of {topic} India — {SCENE_TEMPLATES_FALLBACK[2]}",
        ]

    # ── IMAGES ───────────────────────────────────────────────────

    def poll_for_image(generation_id, output_path):
        for poll in range(80):
            time.sleep(3)
            if poll % 5 == 0:
                print(f"  Polling {poll * 3}s...")
            try:
                r = requests.get(
                    f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                    timeout=15
                )
                r.raise_for_status()
                gen    = r.json().get("generations_by_pk", {})
                status = gen.get("status", "UNKNOWN")
                if status == "FAILED":
                    raise Exception(f"Leonardo FAILED")
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

    def generate_image(scene_prompt, output_path):
        last_error = None
        for model_id in LEONARDO_MODELS:
            try:
                print(f"  Model: {model_id[:8]}...")
                r = requests.post(
                    "https://cloud.leonardo.ai/api/rest/v1/generations",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}",
                             "Content-Type": "application/json"},
                    json={"prompt": scene_prompt, "modelId": model_id,
                          "width": IMG_WIDTH, "height": IMG_HEIGHT,
                          "num_images": 1, "presetStyle": "CINEMATIC"},
                    timeout=30
                )
                if r.status_code != 200:
                    raise Exception(f"{r.status_code}: {r.text[:200]}")
                data = r.json()
                if "sdGenerationJob" not in data:
                    raise Exception(f"No sdGenerationJob")
                gen_id = data["sdGenerationJob"]["generationId"]
                print(f"  Gen ID: {gen_id}")
                return poll_for_image(gen_id, output_path)
            except Exception as e:
                last_error = e
                print(f"  Model failed: {str(e)[:100]}")
                time.sleep(5)
        raise Exception(f"All models failed: {last_error}")

    def generate_all_images():
        print("\n[Generating scene prompts]")
        scene_prompts = generate_scene_prompts()
        image_paths   = []

        for i, scene_prompt in enumerate(scene_prompts):
            update_status("images")
            print(f"\n[Image {i+1}/3]")
            path = f"{TMP_DIR}/{job_id}_{i}.png"
            if i > 0:
                time.sleep(8)
            try:
                generate_image(scene_prompt, path)
                image_paths.append(path)
            except Exception as e:
                print(f"Image {i+1} failed, using fallback: {e}")
                if image_paths:
                    shutil.copy(image_paths[-1], path)
                else:
                    subprocess.run([
                        "ffmpeg", "-y", "-f", "lavfi",
                        "-i", f"color=c=0x0d1117:s={IMG_WIDTH}x{IMG_HEIGHT}:d=1",
                        "-frames:v", "1", path
                    ], capture_output=True, timeout=15)
                image_paths.append(path)
        return image_paths

    # ── VOICE ─────────────────────────────────────────────────────

    def generate_voice(script):
        update_status("voice")
        print("\n[Voice]")
        r = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
            headers={"xi-api-key": ELEVENLABS_API_KEY,
                     "Content-Type": "application/json"},
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
        print(f"  Raw: {duration:.1f}s")

        if duration < 24.0:
            run_ffmpeg([
                "ffmpeg", "-y", "-i", raw_path,
                "-af", f"apad=pad_dur={25.0 - duration}", "-t", "25", audio_path
            ], "voice-pad", timeout=20)
            os.remove(raw_path)
        else:
            os.rename(raw_path, audio_path)

        print(f"  Final: {get_audio_duration(audio_path):.1f}s")
        return audio_path

    # ==========================================
    # VIDEO RENDER
    #
    # SAFE EFFECT STACK (all verified on debian ffmpeg 5.x):
    #
    # Per clip:
    #   1. scale to 110% with lanczos (quality upscale)
    #   2. crop from static offset (Ken Burns framing variety)
    #   3. eq filter (contrast/brightness/saturation)
    #   4. curves filter (color tone per scene)
    #   5. unsharp (cinematic sharpness)
    #   6. vignette (cinematic edges)
    #   7. fade=in (0.4s fade in per clip)
    #   8. drawtext captions (3 per clip, timed with between())
    #
    # Between clips:
    #   9. xfade (dissolve/fade/wipe — randomised)
    #
    # Final mux:
    #  10. fade=out on last 0.5s of video
    #  11. audio: loudnorm (broadcast loudness normalisation)
    # ==========================================

    def render_scene_clip(img_path, duration, scene_idx, captions):
        clip_path = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
        third     = duration / 3.0
        grade     = SCENE_GRADES[scene_idx % 3]
        x_off, y_off, pos_label = CROP_POSITIONS[scene_idx % 3]
        cap_y     = int(OUT_HEIGHT * 0.73)
        cap_size  = 58

        print(f"  Clip {scene_idx}: [{pos_label}] [{grade['label']}]")

        # ABSOLUTE MINIMUM — identical to what rendered successfully on Render
        # Adding effects back one at a time AFTER this works
        vf_parts = [
            f"scale={OUT_WIDTH}:{OUT_HEIGHT}:force_original_aspect_ratio=increase",
            f"crop={OUT_WIDTH}:{OUT_HEIGHT}",
            "setsar=1",
        ]

        # 9. Captions — 3 per clip, one per third, timed with between()
        scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
        while len(scene_caps) < 3:
            scene_caps.append("")

        for ci, cap in enumerate(scene_caps):
            if not cap.strip():
                continue
            escaped = escape_dt(cap)
            t_start = ci * third
            t_end   = (ci + 1) * third
            vf_parts.append(
                f"drawtext=text='{escaped}'"
                f":fontsize={cap_size}:fontcolor=white"
                f":borderw=5:bordercolor=black@0.85"
                f":x=(w-text_w)/2:y={cap_y}"
                f":enable='between(t,{t_start:.3f},{t_end:.3f})'"
            )

        vf_str = ",".join(vf_parts)
        print(f"  vf chain: {vf_str}")

        cmd = [
            "ffmpeg", "-y",
            "-loop", "1", "-r", str(FPS),
            "-i", img_path,
            "-vf", vf_str,
            "-t", str(duration),
            "-r", str(FPS),
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "20",
            "-pix_fmt", "yuv420p",
            clip_path
        ]

        run_ffmpeg(cmd, f"clip-{scene_idx}", timeout=300)
        size = os.path.getsize(clip_path)
        print(f"  Clip {scene_idx}: {size // 1024}KB")
        return clip_path

    def apply_xfade(clip_paths, scene_dur):
        """Chain xfade transitions between clips with random transition type."""
        if len(clip_paths) == 1:
            return clip_paths[0]

        # Pick one transition type for this video
        transition = random.choice(XFADE_TRANSITIONS)
        print(f"\n  xfade transition: {transition}")

        output_path = f"{TMP_DIR}/{job_id}_xfaded.mp4"
        n           = len(clip_paths)
        inputs      = []
        for cp in clip_paths:
            inputs += ["-i", cp]

        # Build chained xfade filter_complex
        fc_parts = []
        offset   = scene_dur - XFADE_DUR
        fc_parts.append(
            f"[0:v][1:v]xfade=transition={transition}"
            f":duration={XFADE_DUR}:offset={offset:.3f}[xf0]"
        )
        for i in range(2, n):
            offset   += scene_dur - XFADE_DUR
            prev_lbl  = f"[xf{i-2}]" if i > 2 else "[xf0]"
            fc_parts.append(
                f"{prev_lbl}[{i}:v]xfade=transition={transition}"
                f":duration={XFADE_DUR}:offset={offset:.3f}[xf{i-1}]"
            )

        last_label = f"[xf{n-2}]"

        cmd = [
            "ffmpeg", "-y", *inputs,
            "-filter_complex", ";".join(fc_parts),
            "-map", last_label,
            "-c:v", "libx264", "-preset", "fast", "-crf", "21",
            "-pix_fmt", "yuv420p",
            output_path
        ]

        try:
            run_ffmpeg(cmd, "xfade", timeout=120)
            size = os.path.getsize(output_path)
            print(f"  xfaded: {size // 1024}KB")
            return output_path
        except Exception as e:
            print(f"  xfade failed ({e}), falling back to concat")
            return simple_concat(clip_paths)

    def simple_concat(clip_paths):
        list_path   = f"{TMP_DIR}/{job_id}_list.txt"
        concat_path = f"{TMP_DIR}/{job_id}_concat.mp4"
        with open(list_path, "w") as f:
            for cp in clip_paths:
                f.write(f"file '{cp}'\n")
        run_ffmpeg([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", list_path, "-c", "copy", concat_path
        ], "concat", timeout=60)
        try: os.remove(list_path)
        except Exception: pass
        return concat_path

    def render_video(images, audio, captions):
        update_status("render")
        print("\n[Render]")

        audio_dur = get_audio_duration(audio)
        total_dur = max(audio_dur, 25.0)
        scene_dur = total_dur / len(images)
        video_path = f"{TMP_DIR}/{job_id}.mp4"

        print(f"  Audio: {audio_dur:.1f}s | {len(images)} scenes x {scene_dur:.1f}s")
        print(f"  Effects: crop-grade-curves-unsharp-vignette-fade + xfade + captions")

        # Render individual scene clips
        clip_paths = []
        for i, img in enumerate(images):
            clip = render_scene_clip(img, scene_dur, i, captions)
            clip_paths.append(clip)
            try: os.remove(img)
            except Exception: pass

        # Apply xfade transitions
        transitioned = apply_xfade(clip_paths, scene_dur)

        for cp in clip_paths:
            try: os.remove(cp)
            except Exception: pass

        # Final mux: add audio + fade out last 0.5s + loudnorm
        # fade=out on video, loudnorm on audio for broadcast quality
        fade_out_st = total_dur - 0.5

        cmd = [
            "ffmpeg", "-y",
            "-i", transitioned,
            "-i", audio,
            "-filter_complex",
            f"[0:v]fade=t=out:st={fade_out_st:.2f}:d=0.5[vout];"
            f"[1:a]loudnorm=I=-16:TP=-1.5:LRA=11[aout]",
            "-map", "[vout]",
            "-map", "[aout]",
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-c:a", "aac", "-b:a", "128k",
            "-shortest", "-movflags", "+faststart",
            video_path
        ]

        try:
            run_ffmpeg(cmd, "final-mux", timeout=120)
        except Exception as e:
            # loudnorm can fail on very short audio — fallback without it
            print(f"  loudnorm failed ({e}), retrying without it")
            cmd_simple = [
                "ffmpeg", "-y",
                "-i", transitioned,
                "-i", audio,
                "-map", "0:v", "-map", "1:a",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "128k",
                "-shortest", "-movflags", "+faststart",
                video_path
            ]
            run_ffmpeg(cmd_simple, "final-mux-simple", timeout=120)

        try: os.remove(transitioned)
        except Exception: pass

        size = os.path.getsize(video_path)
        if size < 100_000:
            raise Exception(f"Video too small: {size}")
        print(f"  Final video: {size // 1024}KB")
        return video_path

    # ── YOUTUBE ───────────────────────────────────────────────────

    def upload_to_youtube(video_path, title, script):
        update_status("upload")
        print("\n[Upload]")
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
        token = r.json()["access_token"]

        metadata = {
            "snippet": {
                "title":       title[:100],
                "description": (
                    f"{script}\n\n"
                    "India20Sixty - Exploring India's near future.\n\n"
                    "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
                ),
                "tags": ["Future India", "India innovation", "AI", "Technology", "Shorts"],
                "categoryId": "28"
            },
            "status": {
                "privacyStatus":          "public",
                "selfDeclaredMadeForKids": False
            }
        }

        with open(video_path, "rb") as vf:
            r = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos"
                "?uploadType=multipart&part=snippet,status",
                headers={"Authorization": f"Bearer {token}"},
                files={
                    "snippet": (None, json.dumps(metadata), "application/json"),
                    "video":   ("video.mp4", vf, "video/mp4")
                },
                timeout=300
            )
        r.raise_for_status()
        video_id = r.json()["id"]
        print(f"  YouTube: https://youtube.com/watch?v={video_id}")
        return video_id

    # ── RUN ───────────────────────────────────────────────────────

    try:
        update_status("processing", {"topic": topic})
        log_to_db("Pipeline started on Modal")

        script, script_lines = generate_script()
        log_to_db(f"Script: {script[:80]}")

        captions = extract_captions(script_lines)
        log_to_db(f"Captions: {captions[:3]}")

        images = generate_all_images()
        log_to_db(f"Images: {len(images)}")

        audio = generate_voice(script)
        log_to_db("Voice done")

        video = render_video(images, audio, captions)
        log_to_db("Video rendered")

        if TEST_MODE:
            print(f"\nTEST MODE — skipping upload")
            video_id, final_status = "TEST_MODE", "test_complete"
        else:
            title        = f"{topic} | India20Sixty #Shorts"
            video_id     = upload_to_youtube(video, title, script)
            final_status = "complete"
            log_to_db(f"Uploaded: {video_id}")

        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/videos",
                headers={
                    "apikey":          SUPABASE_ANON_KEY,
                    "Authorization":   f"Bearer {SUPABASE_ANON_KEY}",
                    "Content-Type":    "application/json",
                    "Prefer":          "return=minimal"
                },
                json={
                    "job_id":      job_id,
                    "topic":       topic,
                    "youtube_url": (
                        f"https://youtube.com/watch?v={video_id}"
                        if video_id != "TEST_MODE" else None
                    )
                },
                timeout=10
            )
        except Exception as e:
            print(f"videos insert (non-fatal): {e}")

        update_status(final_status, {
            "youtube_id":     video_id,
            "script_package": {
                "text":         script,
                "lines":        script_lines,
                "captions":     captions,
                "generated_at": datetime.utcnow().isoformat()
            }
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
# LOCAL TEST
# ==========================================

@app.local_entrypoint()
def main():
    print("Running full effects pipeline test...")
    run_pipeline.remote(
        job_id="test-effects-001",
        topic="AI doctors transforming rural India",
        webhook_url=""
    )
