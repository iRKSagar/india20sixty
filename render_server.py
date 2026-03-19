from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID")

YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")


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
                "status": status,
                "updated_at": "now()"
            }
        )

    except Exception as e:
        print("STATUS UPDATE ERROR", e)


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
# GENERATE PLACEHOLDER IMAGES
# ------------------------------------------------

def generate_images(job_id):

    print("IMAGES START")

    update_status(job_id, "images")

    paths = []

    for i in range(5):

        path = f"/tmp/{job_id}_{i}.png"

        subprocess.run([
            "ffmpeg",
            "-f", "lavfi",
            "-i", "color=c=blue:s=1080x1920",
            "-frames:v", "1",
            "-y",
            path
        ])

        paths.append(path)

    print("IMAGES DONE")

    return paths


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
            f.write("duration 3\n")

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
# GET YOUTUBE ACCESS TOKEN
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

        script = f"{topic}. This is how India will look in the future."

        images = generate_images(job_id)

        audio = generate_voice(script, job_id)

        video = render_video(images, audio, job_id)

        upload_youtube(video, topic, job_id)

        update_status(job_id, "complete")

        print("PIPELINE COMPLETE")

        return jsonify({
            "status": "complete"
        })

    except Exception as e:

        update_status(job_id, "failed")

        print("PIPELINE FAILED", str(e))

        return jsonify({
            "error": str(e)
        })


# ------------------------------------------------
# HEALTH CHECK
# ------------------------------------------------

@app.route("/")
def home():
    return {"status": "render server running"}


# ------------------------------------------------

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)
