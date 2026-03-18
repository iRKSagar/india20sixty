import os
import subprocess
import requests
import boto3
from flask import Flask, request, jsonify

app = Flask(__name__)

# -----------------------------
# CONFIG
# -----------------------------

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")

TMP_DIR = "/tmp/india20sixty"
IMAGE_DIR = f"{TMP_DIR}/images"
AUDIO_DIR = f"{TMP_DIR}/audio"
VIDEO_DIR = f"{TMP_DIR}/videos"

os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)
os.makedirs(VIDEO_DIR, exist_ok=True)

# Background music file bundled in repo
MUSIC_FILE = "assets/music.mp3"

# -----------------------------
# R2 CLIENT
# -----------------------------

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# -----------------------------
# DOWNLOAD
# -----------------------------

def download(url, path):

    r = requests.get(url)

    with open(path, "wb") as f:
        f.write(r.content)

# -----------------------------
# RENDER
# -----------------------------

@app.route("/render", methods=["POST"])
def render():

    data = request.json

    job_id = data["job_id"]
    images = data["images"]
    audio = data["audio"]

    image_paths = []

    # -----------------------------
    # DOWNLOAD IMAGES
    # -----------------------------

    for i, img in enumerate(images):

        path = f"{IMAGE_DIR}/{job_id}_{i}.jpg"

        download(img, path)

        image_paths.append(path)

    # -----------------------------
    # DOWNLOAD VOICE
    # -----------------------------

    audio_path = f"{AUDIO_DIR}/{job_id}.mp3"

    download(audio, audio_path)

    # -----------------------------
    # CREATE VIDEO SEGMENTS
    # -----------------------------

    segments = []

    for i, img in enumerate(image_paths):

        segment = f"{VIDEO_DIR}/{job_id}_seg{i}.mp4"

        cmd = [

            "ffmpeg",
            "-y",
            "-loop", "1",
            "-i", img,
            "-t", "5",

            "-vf",

            # Ken Burns zoom
            "zoompan=z='min(zoom+0.0015,1.2)':d=125:s=1080x1920",

            "-pix_fmt", "yuv420p",
            "-c:v", "libx264",

            segment
        ]

        subprocess.run(cmd)

        segments.append(segment)

    # -----------------------------
    # CONCAT FILE
    # -----------------------------

    concat_file = f"{TMP_DIR}/{job_id}_concat.txt"

    with open(concat_file, "w") as f:

        for s in segments:

            f.write(f"file '{s}'\n")

    merged = f"{VIDEO_DIR}/{job_id}_merged.mp4"

    subprocess.run([
        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", concat_file,
        "-c", "copy",
        merged
    ])

    # -----------------------------
    # ADD AUDIO + MUSIC
    # -----------------------------

    final_video = f"{VIDEO_DIR}/{job_id}.mp4"

    subprocess.run([

        "ffmpeg",
        "-y",
        "-i", merged,
        "-i", audio_path,
        "-i", MUSIC_FILE,

        "-filter_complex",

        "[1:a]volume=1[a1];"
        "[2:a]volume=0.15[a2];"
        "[a1][a2]amix=inputs=2[aout]",

        "-map", "0:v",
        "-map", "[aout]",

        "-shortest",
        "-c:v", "copy",

        final_video
    ])

    # -----------------------------
    # UPLOAD TO R2
    # -----------------------------

    key = f"videos/{job_id}.mp4"

    r2.upload_file(

        final_video,
        R2_BUCKET,
        key,
        ExtraArgs={"ContentType": "video/mp4"}

    )

    video_url = f"{R2_ENDPOINT}/{R2_BUCKET}/{key}"

    return jsonify({

        "status": "rendered",
        "job_id": job_id,
        "video": video_url

    })


# -----------------------------
# HEALTH
# -----------------------------

@app.route("/")
def health():

    return jsonify({

        "service": "india20sixty-render",
        "status": "running"

    })


# -----------------------------
# RUN
# -----------------------------

if __name__ == "__main__":

    app.run(host="0.0.0.0", port=10000)
