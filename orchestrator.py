from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json
import time

app = Flask(__name__)

# ------------------------------------------------
# ENVIRONMENT
# ------------------------------------------------

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
LEONARDO_API_KEY = os.environ.get("LEONARDO_API_KEY")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID")

YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

TEST_MODE = os.environ.get("TEST_MODE", "true").lower() == "true"

TMP = "/tmp"

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
                "Content-Type": "application/json"
            },
            json={"status": status}
        )

    except Exception as e:

        print("STATUS UPDATE FAILED:", str(e))


# ------------------------------------------------
# OPENAI SCRIPT
# ------------------------------------------------

def generate_script(topic):

    print("SCRIPT START")

    prompt = f"""
Create a 25 second YouTube Shorts script about:

{topic}

Structure:
Hook
Context
Insight
Future
Question

Short sentences.
"""

    r = requests.post(

        "https://api.openai.com/v1/chat/completions",

        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },

        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

    )

    text = r.json()["choices"][0]["message"]["content"]

    print("SCRIPT DONE")

    return text


# ------------------------------------------------
# VISUAL PROMPTS
# ------------------------------------------------

def generate_prompts(script):

    print("PROMPT START")

    prompt = f"""
Break this script into 5 cinematic visual scenes.

Script:
{script}

Return JSON list of prompts.
"""

    r = requests.post(

        "https://api.openai.com/v1/chat/completions",

        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },

        json={
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

    )

    txt = r.json()["choices"][0]["message"]["content"]

    try:

        prompts = json.loads(txt)

    except:

        prompts = [script] * 5

    print("PROMPT DONE")

    return prompts


# ------------------------------------------------
# LEONARDO IMAGES
# ------------------------------------------------

def generate_images(prompts, job_id):

    print("IMAGES START")

    update_status(job_id, "images")

    images = []

    for i, p in enumerate(prompts):

        r = requests.post(

            "https://cloud.leonardo.ai/api/rest/v1/generations",

            headers={
                "Authorization": f"Bearer {LEONARDO_API_KEY}",
                "Content-Type": "application/json"
            },

            json={
                "prompt": p,
                "width": 1080,
                "height": 1920
            }

        )

        data = r.json()

        img_url = data["generations_by_pk"]["generated_images"][0]["url"]

        img = requests.get(img_url).content

        path = f"{TMP}/{job_id}_{i}.png"

        with open(path, "wb") as f:
            f.write(img)

        images.append(path)

    print("IMAGES DONE")

    return images


# ------------------------------------------------
# ELEVENLABS VOICE
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

    audio = f"{TMP}/{job_id}.mp3"

    with open(audio, "wb") as f:
        f.write(r.content)

    print("VOICE DONE")

    return audio


# ------------------------------------------------
# VIDEO RENDER
# ------------------------------------------------

def render_video(images, audio, job_id):

    print("RENDER START")

    update_status(job_id, "render")

    video = f"{TMP}/{job_id}.mp4"

    inputs = []

    for img in images:
        inputs += ["-loop", "1", "-t", "5", "-i", img]

    cmd = [

        "ffmpeg",
        *inputs,
        "-i", audio,

        "-filter_complex",
        f"concat=n={len(images)}:v=1:a=0",

        "-shortest",
        "-s", "1080x1920",
        "-pix_fmt", "yuv420p",
        "-y",
        video

    ]

    subprocess.run(cmd)

    print("RENDER DONE")

    return video


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

def upload_youtube(video, title, job_id):

    print("UPLOAD START")

    update_status(job_id, "upload")

    token = get_youtube_token()

    headers = {

        "Authorization": f"Bearer {token}"

    }

    metadata = {

        "snippet": {
            "title": title,
            "description": "Future India 2060",
            "tags": ["india", "future", "ai"]
        },

        "status": {
            "privacyStatus": "public"
        }

    }

    requests.post(

        "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",

        headers=headers,

        files={
            "snippet": (None, json.dumps(metadata), "application/json"),
            "video": ("video.mp4", open(video, "rb"), "video/mp4")
        }

    )

    print("UPLOAD DONE")


# ------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------

@app.route("/full-pipeline", methods=["POST"])
def pipeline():

    data = request.json

    job_id = data["job_id"]
    topic = data["topic"]

    try:

        print("PIPELINE START", job_id, topic)

        script = generate_script(topic)

        prompts = generate_prompts(script)

        images = generate_images(prompts, job_id)

        audio = generate_voice(script, job_id)

        video = render_video(images, audio, job_id)

        if not TEST_MODE:

            upload_youtube(video, topic, job_id)

        else:

            print("TEST MODE ACTIVE — SKIPPING YOUTUBE UPLOAD")

        update_status(job_id, "complete")

        return jsonify({

            "status": "complete",
            "video_path": video

        })

    except Exception as e:

        print("PIPELINE FAILED", str(e))

        update_status(job_id, "failed")

        return jsonify({"error": str(e)})


# ------------------------------------------------
# HEALTH
# ------------------------------------------------

@app.route("/")
def home():

    return {

        "status": "render server running"

    }


# ------------------------------------------------

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)
