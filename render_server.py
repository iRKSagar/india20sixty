import os
import time
import uuid
import requests
import subprocess
from flask import Flask, request, jsonify

app = Flask(__name__)

TMP_DIR = "/tmp/india20sixty"
IMG_DIR = f"{TMP_DIR}/images"
AUD_DIR = f"{TMP_DIR}/audio"
VID_DIR = f"{TMP_DIR}/videos"

os.makedirs(IMG_DIR, exist_ok=True)
os.makedirs(AUD_DIR, exist_ok=True)
os.makedirs(VID_DIR, exist_ok=True)

LEONARDO_API_KEY = os.environ["LEONARDO_API_KEY"]
ELEVENLABS_API_KEY = os.environ["ELEVENLABS_API_KEY"]

VOICE_ID = "EXAVITQu4vr4xnSDxMaL"


# ---------------- IMAGE GENERATION ----------------

def generate_image(prompt):

    create = requests.post(
        "https://cloud.leonardo.ai/api/rest/v1/generations",
        headers={
            "Authorization": f"Bearer {LEONARDO_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "prompt": prompt,
            "modelId": "b2614463-296c-462a-9586-aafdb8f00e36",
            "width": 1080,
            "height": 1536,
            "num_images": 1
        }
    ).json()

    generation_id = create["sdGenerationJob"]["generationId"]

    for _ in range(12):

        time.sleep(2)

        res = requests.get(
            f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
            headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"}
        ).json()

        g = res["generations_by_pk"]

        if g["status"] == "COMPLETE" and g["generated_images"]:
            return g["generated_images"][0]["url"]

    raise Exception("Leonardo generation timeout")


# ---------------- VOICE ----------------

def generate_voice(text, job_id):

    audio_path = f"{AUD_DIR}/{job_id}.mp3"

    res = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
        headers={
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "text": text,
            "model_id": "eleven_multilingual_v2"
        }
    )

    with open(audio_path, "wb") as f:
        f.write(res.content)

    return audio_path


# ---------------- VIDEO RENDER ----------------

def render_video(images, audio, job_id):

    video_path = f"{VID_DIR}/{job_id}.mp4"

    inputs = []
    filters = []

    for i, img in enumerate(images):

        img_path = f"{IMG_DIR}/{job_id}_{i}.jpg"

        r = requests.get(img)

        with open(img_path, "wb") as f:
            f.write(r.content)

        inputs.extend(["-loop", "1", "-t", "5", "-i", img_path])

        filters.append(
            f"[{i}:v]zoompan=z='min(zoom+0.0015,1.2)':d=125:s=1080x1920[v{i}]"
        )

    filter_complex = ";".join(filters)

    cmd = [
        "ffmpeg",
        "-y",
        *inputs,
        "-i", audio,
        "-filter_complex", filter_complex,
        "-map", "[v0]",
        "-map", f"{len(images)}:a",
        "-shortest",
        "-pix_fmt", "yuv420p",
        video_path
    ]

    subprocess.run(cmd)

    return video_path


# ---------------- YOUTUBE UPLOAD ----------------

def upload_to_youtube(video_path, title):

    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from google.oauth2.credentials import Credentials

    SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
    TOKEN = "/tmp/youtube_token.json"

    client_config = {
        "installed": {
            "client_id": os.environ["YOUTUBE_CLIENT_ID"],
            "project_id": os.environ["YOUTUBE_PROJECT_ID"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": os.environ["YOUTUBE_CLIENT_SECRET"],
            "redirect_uris": ["http://localhost"]
        }
    }

    creds = None

    if os.path.exists(TOKEN):
        creds = Credentials.from_authorized_user_file(TOKEN, SCOPES)

    if not creds or not creds.valid:

        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        creds = flow.run_local_server(port=0)

        with open(TOKEN, "w") as f:
            f.write(creds.to_json())

    youtube = build("youtube", "v3", credentials=creds)

    request = youtube.videos().insert(
        part="snippet,status",
        body={
            "snippet": {
                "title": title,
                "description": "Future of India 2060",
                "tags": ["India2060", "FutureTech", "AI"],
                "categoryId": "28"
            },
            "status": {
                "privacyStatus": "public"
            }
        },
        media_body=MediaFileUpload(video_path)
    )

    res = request.execute()

    return res["id"]


# ---------------- PIPELINE ----------------

@app.route("/full-pipeline", methods=["POST"])
def pipeline():

    data = request.json

    job_id = data.get("job_id", str(uuid.uuid4()))
    prompts = data["prompts"]
    script = data["script"]

    images = []

    for prompt in prompts:
        images.append(generate_image(prompt))

    narration = ". ".join(script)

    audio = generate_voice(narration, job_id)

    video = render_video(images, audio, job_id)

    youtube_id = upload_to_youtube(video, "AI Doctors in India 🇮🇳")

    return jsonify({
        "job_id": job_id,
        "video": video,
        "youtube_id": youtube_id
    })


@app.route("/")
def health():
    return jsonify({"status": "render server running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
