from flask import Flask, request, jsonify
import requests
import os
import uuid
import subprocess
import time

app = Flask(__name__)

# =============================
# ENV
# =============================

LEONARDO_API_KEY = os.environ.get("LEONARDO_API_KEY")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")


# =============================
# HEALTH
# =============================

@app.route("/")
def home():
    return {"status": "India20Sixty Render Server Running"}


# =============================
# SUPABASE JOB UPDATE
# =============================

def update_job(job_id, status):

    try:

        url = f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"

        requests.patch(
            url,
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "status": status,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S")
            }
        )

    except Exception as e:

        print("Supabase update failed:", e)


# =============================
# MAIN PIPELINE
# =============================

@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():

    data = request.json or {}

    job_id = data.get("job_id", str(uuid.uuid4()))
    topic = data.get("topic", "Future India")
    script = data.get("script", "Future technology in India.")
    prompts = data.get("visual_prompts", [])

    if len(prompts) == 0:

        prompts = [
            "futuristic Indian AI hospital",
            "advanced smart city India future",
            "robotics healthcare India",
            "AI powered hospital lab India",
            "smart healthcare India 2040"
        ]

    try:

        print("JOB STARTED:", job_id)

        update_job(job_id, "images")

        image_paths = generate_images(prompts, job_id)

        update_job(job_id, "voice")

        audio_path = generate_voice(script, job_id)

        update_job(job_id, "render")

        video_path = render_video(image_paths, audio_path, job_id)

        update_job(job_id, "upload")

        youtube_id = upload_youtube(video_path, topic)

        update_job(job_id, "complete")

        return jsonify({
            "status": "complete",
            "job_id": job_id,
            "youtube_id": youtube_id
        })

    except Exception as e:

        update_job(job_id, "failed")

        return jsonify({
            "status": "error",
            "message": str(e)
        })


# =============================
# LEONARDO IMAGE GENERATION
# =============================

def generate_images(prompts, job_id):

    images = []

    for i, prompt in enumerate(prompts):

        print("Generating image:", prompt)

        r = requests.post(
            "https://cloud.leonardo.ai/api/rest/v1/generations",
            headers={
                "Authorization": f"Bearer {LEONARDO_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "prompt": prompt,
                "width": 1080,
                "height": 1536
            }
        )

        data = r.json()

        gen_id = data["sdGenerationJob"]["generationId"]

        img_url = wait_for_image(gen_id)

        img_data = requests.get(img_url).content

        path = f"/tmp/{job_id}_{i}.png"

        with open(path, "wb") as f:
            f.write(img_data)

        images.append(path)

    return images


# =============================
# LEONARDO POLLING FIX
# =============================

def wait_for_image(gen_id):

    for _ in range(20):

        r = requests.get(
            f"https://cloud.leonardo.ai/api/rest/v1/generations/{gen_id}",
            headers={
                "Authorization": f"Bearer {LEONARDO_API_KEY}"
            }
        )

        data = r.json()

        images = data.get("generations_by_pk", {}).get("generated_images")

        if images:

            return images[0]["url"]

        time.sleep(2)

    raise Exception("Leonardo image timeout")


# =============================
# ELEVENLABS VOICE
# =============================

def generate_voice(script, job_id):

    print("Generating voice...")

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"

    r = requests.post(
        url,
        headers={
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "text": script,
            "model_id": "eleven_multilingual_v2"
        }
    )

    path = f"/tmp/{job_id}.mp3"

    with open(path, "wb") as f:
        f.write(r.content)

    return path


# =============================
# VIDEO RENDER
# =============================

def render_video(images, audio, job_id):

    print("Rendering video...")

    video_path = f"/tmp/{job_id}.mp4"

    inputs = []

    for img in images:

        inputs.extend(["-loop", "1", "-t", "5", "-i", img])

    cmd = [
        "ffmpeg",
        *inputs,
        "-i", audio,
        "-filter_complex",
        f"concat=n={len(images)}:v=1:a=0",
        "-shortest",
        "-s", "1080x1920",
        "-pix_fmt", "yuv420p",
        video_path
    ]

    subprocess.run(cmd)

    return video_path


# =============================
# YOUTUBE UPLOAD (PLACEHOLDER)
# =============================

def upload_youtube(video_path, topic):

    print("Uploading to YouTube:", video_path)

    return "youtube_video_id_placeholder"


# =============================
# SERVER START
# =============================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)
