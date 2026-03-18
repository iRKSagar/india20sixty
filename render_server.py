import os
import uuid
import subprocess
import requests
import boto3

from flask import Flask, request, jsonify

app = Flask(__name__)

# ----------------------------
# CONFIG
# ----------------------------

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")

VIDEO_DIR = "/tmp/india20sixty/videos"
IMAGE_DIR = "/tmp/india20sixty/images"
AUDIO_DIR = "/tmp/india20sixty/audio"

os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)

# ----------------------------
# R2 CLIENT
# ----------------------------

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

# ----------------------------
# DOWNLOAD HELPER
# ----------------------------

def download_file(url, path):
    r = requests.get(url)
    with open(path, "wb") as f:
        f.write(r.content)

# ----------------------------
# RENDER ENDPOINT
# ----------------------------

@app.route("/render", methods=["POST"])
def render():

    data = request.json

    job_id = data["job_id"]
    image_urls = data["images"]
    audio_url = data["audio"]

    # ----------------------------
    # DOWNLOAD ASSETS
    # ----------------------------

    image_paths = []

    for i, url in enumerate(image_urls):
        path = f"{IMAGE_DIR}/{job_id}_{i}.jpg"
        download_file(url, path)
        image_paths.append(path)

    audio_path = f"{AUDIO_DIR}/{job_id}.mp3"
    download_file(audio_url, audio_path)

    # ----------------------------
    # CREATE IMAGE LIST FILE
    # ----------------------------

    list_file = f"/tmp/{job_id}_images.txt"

    with open(list_file, "w") as f:
        for img in image_paths:
            f.write(f"file '{img}'\n")
            f.write("duration 4\n")

        f.write(f"file '{image_paths[-1]}'\n")

    # ----------------------------
    # RENDER VIDEO
    # ----------------------------

    output_path = f"{VIDEO_DIR}/{job_id}.mp4"

    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        list_file,
        "-i",
        audio_path,
        "-vsync",
        "vfr",
        "-pix_fmt",
        "yuv420p",
        "-vf",
        "scale=1080:1920",
        "-shortest",
        output_path,
    ]

    subprocess.run(cmd, check=True)

    # ----------------------------
    # UPLOAD TO R2
    # ----------------------------

    r2_key = f"videos/{job_id}.mp4"

    r2.upload_file(
        output_path,
        R2_BUCKET,
        r2_key,
        ExtraArgs={"ContentType": "video/mp4"},
    )

    video_url = f"{R2_ENDPOINT}/{R2_BUCKET}/{r2_key}"

    # ----------------------------
    # RESPONSE
    # ----------------------------

    return jsonify({
        "status": "rendered",
        "job_id": job_id,
        "video": video_url
    })


# ----------------------------
# HEALTH CHECK
# ----------------------------

@app.route("/")
def health():
    return jsonify({
        "service": "india20sixty-render",
        "status": "running"
    })


# ----------------------------
# RUN
# ----------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
