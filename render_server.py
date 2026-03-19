from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json
import time

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID")

LEONARDO_API_KEY = os.environ.get("LEONARDO_API_KEY")

YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

LEONARDO_URL = "https://cloud.leonardo.ai/api/rest/v1/generations"


# ------------------------------------------------
# UPDATE JOB STATUS
# ------------------------------------------------

def update_status(job_id, status):

    try:

        requests.patch(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal"
            },
            json={
                "status": status
            }
        )

    except Exception as e:
        print("STATUS UPDATE ERROR", e)


# ------------------------------------------------
# GENERATE SCRIPT
# ------------------------------------------------

def generate_script(topic):

    return f"{topic}. Imagine how India could transform by 2060."


# ------------------------------------------------
# GENERATE VISUAL PROMPTS
# ------------------------------------------------

def generate_prompts(topic):

    return [

        f"futuristic India city skyline 2060, ultra realistic, cinematic lighting",

        f"advanced AI hospital in India, doctors using holographic technology",

        f"India high speed trains and smart infrastructure future technology",

        f"robotics factory in India automation manufacturing futuristic",

        f"Indian space technology satellite control futuristic command center"
    ]


# ------------------------------------------------
# GENERATE LEONARDO IMAGE
# ------------------------------------------------

def generate_leonardo_image(prompt, image_path):

    headers = {
        "Authorization": f"Bearer {LEONARDO_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "prompt": prompt,
        "width": 1080,
        "height": 1920,
        "num_images": 1
    }

    r = requests.post(LEONARDO_URL, headers=headers, json=payload)

    data = r.json()

    generation_id = data["sdGenerationJob"]["generationId"]

    time.sleep(6)

    r = requests.get(
        f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
        headers=headers
    )

    result = r.json()

    image_url = result["generations_by_pk"]["generated_images"][0]["url"]

    img = requests.get(image_url).content

    with open(image_path, "wb") as f:
        f.write(img)


# ------------------------------------------------
# GENERATE IMAGES
# ------------------------------------------------

def generate_images(prompts, job_id):

    print("IMAGES START")

    update_status(job_id, "images")

    paths = []

    for i, prompt in enumerate(prompts):

        path = f"/tmp/{job_id}_{i}.png"

        print("Generating image:", prompt)

        generate_leonardo_image(prompt, path)

        paths.append(path)

    print("IMAGES DONE")

    return paths


# ------------------------------------------------
# GENERATE VOICE
# ------------------------------------------------

def generate_voice(script, job_id):

    print("VOICE START")

    update_status(job_id, "voice")

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

    audio_path = f"/tmp/{job_id}.mp3"

    with open(audio_path, "wb") as f:
        f.write(r.content)

    print("VOICE DONE")

    return audio_path


# ------------------------------------------------
# RENDER VIDEO
# ------------------------------------------------

def render_video(images, audio, job_id):

    print("RENDER START")

    update_status(job_id, "render")

    slideshow = f"/tmp/{job_id}_slideshow.mp4"
    video_path = f"/tmp/{job_id}.mp4"

    list_file = f"/tmp/{job_id}_list.txt"

    with open(list_file, "w") as f:
        for img in images:
            f.write(f"file '{img}'\n")
            f.write("duration 4\n")

    subprocess.run([
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file,
        "-vsync", "vfr",
        "-pix_fmt", "yuv420p",
        "-s", "1080x1920",
        "-y",
        slideshow
    ])

    subprocess.run([
        "ffmpeg",
        "-i", slideshow,
        "-i", audio,
        "-shortest",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-y",
        video_path
    ])

    print("RENDER DONE")

    return video_path


# ------------------------------------------------
# YOUTUBE TOKEN
# ------------------------------------------------

def get_youtube_token():

    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": YOUTUBE_CLIENT_ID,
            "client_secret": YOUTUBE_CLIENT_SECRET,
            "refresh_token": YOUTUBE_REFRESH_TOKEN,
            "grant_type": "refresh_token"
        }
    )

    return r.json()["access_token"]


# ------------------------------------------------
# YOUTUBE UPLOAD
# ------------------------------------------------

def upload_youtube(video_path, title, job_id):

    print("UPLOAD START")

    update_status(job_id, "upload")

    token = get_youtube_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    metadata = {
        "snippet": {
            "title": title,
            "description": "Future India 2060 #shorts",
            "tags": ["india", "future", "ai"],
            "categoryId": "28"
        },
        "status": {
            "privacyStatus": "public"
        }
    }

    r = requests.post(
        "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",
        headers=headers,
        files={
            "snippet": (None, json.dumps(metadata), "application/json"),
            "video": ("video.mp4", open(video_path, "rb"), "video/mp4")
        }
    )

    print("UPLOAD DONE")

    return r.json()


# ------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------

@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():

    data = request.json

    job_id = data["job_id"]
    topic = data.get("topic", "Future India")

    print("PIPELINE START", job_id, topic)

    try:

        script = generate_script(topic)

        prompts = generate_prompts(topic)

        images = generate_images(prompts, job_id)

        audio = generate_voice(script, job_id)

        video = render_video(images, audio, job_id)

        if TEST_MODE:

            print("TEST MODE ENABLED — SKIPPING YOUTUBE UPLOAD")

        else:

            upload_youtube(video, topic, job_id)

        update_status(job_id, "complete")

        return jsonify({"status": "complete"})

    except Exception as e:

        update_status(job_id, "failed")

        print("PIPELINE FAILED", str(e))

        return jsonify({"error": str(e)})


@app.route("/")
def home():
    return {"status": "render server running"}


if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)
