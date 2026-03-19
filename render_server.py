from flask import Flask, request, jsonify
import requests
import os
import uuid
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

def update_status(job_id,status):

    try:

        requests.patch(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
            headers={
                "apikey":SUPABASE_ANON_KEY,
                "Authorization":f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type":"application/json"
            },
            json={
                "status":status
            }
        )

    except:
        pass


# ------------------------------------------------
# GENERATE VOICE
# ------------------------------------------------

def generate_voice(script,job_id):

    update_status(job_id,"voice")

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"

    r = requests.post(
        url,
        headers={
            "xi-api-key":ELEVENLABS_API_KEY,
            "Content-Type":"application/json"
        },
        json={
            "text":script,
            "model_id":"eleven_multilingual_v2"
        }
    )

    audio_path=f"/tmp/{job_id}.mp3"

    with open(audio_path,"wb") as f:
        f.write(r.content)

    return audio_path


# ------------------------------------------------
# GENERATE PLACEHOLDER IMAGES
# ------------------------------------------------

def generate_images(job_id):

    update_status(job_id,"images")

    paths=[]

    for i in range(5):

        path=f"/tmp/{job_id}_{i}.png"

        subprocess.run([
            "ffmpeg",
            "-f","lavfi",
            "-i","color=c=blue:s=1080x1920",
            "-frames:v","1",
            path
        ])

        paths.append(path)

    return paths


# ------------------------------------------------
# RENDER VIDEO
# ------------------------------------------------

def render_video(images,audio,job_id):

    update_status(job_id,"render")

    video_path=f"/tmp/{job_id}.mp4"

    inputs=[]

    for img in images:
        inputs+=["-loop","1","-t","3","-i",img]

    cmd=[
        "ffmpeg",
        *inputs,
        "-i",audio,
        "-filter_complex",
        f"concat=n={len(images)}:v=1:a=0",
        "-shortest",
        "-s","1080x1920",
        "-pix_fmt","yuv420p",
        video_path
    ]

    subprocess.run(cmd)

    return video_path


# ------------------------------------------------
# YOUTUBE ACCESS TOKEN
# ------------------------------------------------

def get_youtube_token():

    r=requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":YOUTUBE_CLIENT_ID,
            "client_secret":YOUTUBE_CLIENT_SECRET,
            "refresh_token":YOUTUBE_REFRESH_TOKEN,
            "grant_type":"refresh_token"
        }
    )

    return r.json()["access_token"]


# ------------------------------------------------
# YOUTUBE UPLOAD
# ------------------------------------------------

def upload_youtube(video_path,title,job_id):

    update_status(job_id,"upload")

    token=get_youtube_token()

    headers={
        "Authorization":f"Bearer {token}"
    }

    params={
        "part":"snippet,status"
    }

    metadata={
        "snippet":{
            "title":title,
            "description":"Future India 2060 #shorts",
            "tags":["india","future","ai"],
            "categoryId":"28"
        },
        "status":{
            "privacyStatus":"public"
        }
    }

    files={
        "video":open(video_path,"rb")
    }

    r=requests.post(
        "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",
        headers=headers,
        data={"metadata":json.dumps(metadata)},
        files=files
    )

    return r.json()


# ------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------

@app.route("/full-pipeline",methods=["POST"])
def full_pipeline():

    data=request.json

    job_id=data["job_id"]
    topic=data.get("topic","Future India")

    try:

        script=f"{topic}. This is how India will look in the future."

        images=generate_images(job_id)

        audio=generate_voice(script,job_id)

        video=render_video(images,audio,job_id)

        upload_youtube(video,topic,job_id)

        update_status(job_id,"complete")

        return jsonify({
            "status":"complete"
        })

    except Exception as e:

        update_status(job_id,"failed")

        return jsonify({
            "error":str(e)
        })


# ------------------------------------------------

@app.route("/")
def home():
    return {"status":"render server running"}


if __name__=="__main__":

    port=int(os.environ.get("PORT",10000))

    app.run(host="0.0.0.0",port=port)
