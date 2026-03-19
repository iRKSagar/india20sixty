from flask import Flask, request, jsonify
import requests
import os
import uuid
import subprocess

app = Flask(__name__)
@app.route("/")
def home():
    return {"status": "India20Sixty Render Server Running"}

LEONARDO_API_KEY = os.environ.get("LEONARDO_API_KEY")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")

VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID")

YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_PROJECT_ID = os.environ.get("YOUTUBE_PROJECT_ID")


# =============================
# MAIN PIPELINE
# =============================

@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():

    data = request.json

    job_id = str(uuid.uuid4())

    topic = data["topic"]
    script = data["script"]
    prompts = data["visual_prompts"]

    image_paths = generate_images(prompts, job_id)

    audio_path = generate_voice(script, job_id)

    video_path = render_video(image_paths, audio_path, job_id)

    youtube_id = upload_youtube(video_path, topic)

    return jsonify({
        "status": "complete",
        "job_id": job_id,
        "youtube_id": youtube_id
    })


# =============================
# IMAGE GENERATION
# =============================

def generate_images(prompts, job_id):

    images = []

    for i, prompt in enumerate(prompts):

        r = requests.post(
            "https://cloud.leonardo.ai/api/rest/v1/generations",
            headers={
                "Authorization": f"Bearer {LEONARDO_API_KEY}"
            },
            json={
                "prompt": prompt,
                "width": 1080,
                "height": 1536
            }
        )

        img_url = r.json()["generations_by_pk"]["generated_images"][0]["url"]

        img_data = requests.get(img_url).content

        path = f"/tmp/{job_id}_{i}.png"

        with open(path, "wb") as f:
            f.write(img_data)

        images.append(path)

    return images


# =============================
# VOICE GENERATION
# =============================

def generate_voice(script, job_id):

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

    video_path = f"/tmp/{job_id}.mp4"

    image_inputs = []

    for img in images:
        image_inputs.extend(["-loop", "1", "-t", "5", "-i", img])

    cmd = [
        "ffmpeg",
        *image_inputs,
        "-i", audio,
        "-filter_complex",
        "concat=n=5:v=1:a=0",
        "-shortest",
        "-s", "1080x1920",
        "-pix_fmt", "yuv420p",
        video_path
    ]

    subprocess.run(cmd)

    return video_path


# =============================
# YOUTUBE UPLOAD
# =============================

def upload_youtube(video_path, topic):

    # placeholder
    # integrate google-api-python-client here

    print("Uploading to YouTube:", video_path)

    return "youtube_video_id_placeholder"


# =============================

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
