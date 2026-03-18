from flask import Flask, request, jsonify
import os
import requests
import subprocess

app = Flask(__name__)

TMP_DIR = "/tmp"
VIDEO_DIR = "/tmp/videos"

os.makedirs(VIDEO_DIR, exist_ok=True)


@app.route("/")
def health():
    return {
        "service": "india20sixty-render",
        "status": "running"
    }


def download_file(url, path):
    r = requests.get(url)
    with open(path, "wb") as f:
        f.write(r.content)


@app.route("/render", methods=["POST"])
def render_video():

    data = request.json

    job_id = data["job_id"]
    images = data["images"]
    audio = data["audio"]

    image_paths = []

    # -----------------------------
    # Download Images
    # -----------------------------

    for i, img_url in enumerate(images):

        path = f"{TMP_DIR}/img{i}.png"

        download_file(img_url, path)

        image_paths.append(path)

    # -----------------------------
    # Download Audio
    # -----------------------------

    audio_path = f"{TMP_DIR}/audio.mp3"

    download_file(audio, audio_path)

    # -----------------------------
    # Create FFmpeg Input List
    # -----------------------------

    list_file = f"{TMP_DIR}/images.txt"

    with open(list_file, "w") as f:
        for path in image_paths:
            f.write(f"file '{path}'\n")
            f.write("duration 5\n")

    output_video = f"{VIDEO_DIR}/{job_id}.mp4"

    # -----------------------------
    # Run FFmpeg
    # -----------------------------

    cmd = [
        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file,
        "-i", audio_path,
        "-vsync", "vfr",
        "-pix_fmt", "yuv420p",
        "-shortest",
        output_video
    ]

    subprocess.run(cmd)

    return jsonify({
        "status": "rendered",
        "video": output_video
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
