import os
import subprocess
import requests
import boto3
from flask import Flask, request, jsonify

app = Flask(__name__)

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")

TMP = "/tmp/india20sixty"

IMG_DIR = f"{TMP}/images"
VID_DIR = f"{TMP}/videos"
AUD_DIR = f"{TMP}/audio"
SUB_DIR = f"{TMP}/subs"

os.makedirs(IMG_DIR, exist_ok=True)
os.makedirs(VID_DIR, exist_ok=True)
os.makedirs(AUD_DIR, exist_ok=True)
os.makedirs(SUB_DIR, exist_ok=True)

MUSIC_FILE = "assets/music.mp3"

# --------------------------------------------------
# R2 CLIENT
# --------------------------------------------------

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# --------------------------------------------------
# DOWNLOAD HELPER
# --------------------------------------------------

def download(url, path):

    r = requests.get(url)

    with open(path, "wb") as f:
        f.write(r.content)

# --------------------------------------------------
# CREATE VIDEO SEGMENT
# --------------------------------------------------

def build_segment(image, output):

    cmd = [

        "ffmpeg",
        "-y",

        "-loop", "1",
        "-i", image,

        "-t", "5",

        "-vf",

        # Ken Burns + Color grading
        "zoompan=z='min(zoom+0.0015,1.2)':d=125:s=1080x1920,"
        "eq=contrast=1.1:saturation=1.2:brightness=0.03",

        "-pix_fmt", "yuv420p",
        "-c:v", "libx264",

        output

    ]

    subprocess.run(cmd)

# --------------------------------------------------
# CONCAT SEGMENTS
# --------------------------------------------------

def concat_segments(segment_paths, output):

    list_file = f"{TMP}/segments.txt"

    with open(list_file, "w") as f:

        for seg in segment_paths:
            f.write(f"file '{seg}'\n")

    subprocess.run([

        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file,
        "-c", "copy",
        output

    ])

# --------------------------------------------------
# ADD AUDIO + MUSIC
# --------------------------------------------------

def add_audio(video, voice, output):

    subprocess.run([

        "ffmpeg",
        "-y",

        "-i", video,
        "-i", voice,
        "-i", MUSIC_FILE,

        "-filter_complex",

        "[1:a]volume=1[a1];"
        "[2:a]volume=0.15[a2];"
        "[a1][a2]amix=inputs=2[aout]",

        "-map", "0:v",
        "-map", "[aout]",

        "-shortest",

        output

    ])

# --------------------------------------------------
# ADD SUBTITLES
# --------------------------------------------------

def add_subtitles(video, subtitle_file, output):

    subprocess.run([

        "ffmpeg",
        "-y",

        "-i", video,

        "-vf", f"subtitles={subtitle_file}",

        "-c:a", "copy",

        output

    ])

# --------------------------------------------------
# RENDER ENDPOINT
# --------------------------------------------------

@app.route("/render", methods=["POST"])
def render():

    data = request.json

    job_id = data["job_id"]
    image_urls = data["images"]
    audio_url = data["audio"]
    subtitle_url = data.get("subtitles")

    # --------------------------------------------------
    # DOWNLOAD ASSETS
    # --------------------------------------------------

    image_paths = []

    for i, url in enumerate(image_urls):

        path = f"{IMG_DIR}/{job_id}_{i}.jpg"

        download(url, path)

        image_paths.append(path)

    audio_path = f"{AUD_DIR}/{job_id}.mp3"

    download(audio_url, audio_path)

    subtitle_path = None

    if subtitle_url:

        subtitle_path = f"{SUB_DIR}/{job_id}.srt"

        download(subtitle_url, subtitle_path)

    # --------------------------------------------------
    # BUILD SEGMENTS
    # --------------------------------------------------

    segments = []

    for i, img in enumerate(image_paths):

        seg = f"{VID_DIR}/{job_id}_seg{i}.mp4"

        build_segment(img, seg)

        segments.append(seg)

    # --------------------------------------------------
    # MERGE SEGMENTS
    # --------------------------------------------------

    merged = f"{VID_DIR}/{job_id}_merged.mp4"

    concat_segments(segments, merged)

    # --------------------------------------------------
    # ADD AUDIO
    # --------------------------------------------------

    with_audio = f"{VID_DIR}/{job_id}_audio.mp4"

    add_audio(merged, audio_path, with_audio)

    final_video = f"{VID_DIR}/{job_id}.mp4"

    # --------------------------------------------------
    # ADD SUBTITLES
    # --------------------------------------------------

    if subtitle_path:

        add_subtitles(with_audio, subtitle_path, final_video)

    else:

        final_video = with_audio

    # --------------------------------------------------
    # UPLOAD TO R2
    # --------------------------------------------------

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


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.route("/")
def health():

    return jsonify({

        "service": "india20sixty-render",
        "status": "running"

    })


# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------

if __name__ == "__main__":

    app.run(host="0.0.0.0", port=10000)
