from flask import Flask, request, jsonify
import os
import requests
import subprocess
import uuid
import traceback

app = Flask(__name__)

TMP_DIR = "/tmp/india20sixty"
VIDEO_DIR = f"{TMP_DIR}/videos"

os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(VIDEO_DIR, exist_ok=True)


# ----------------------------------------
# HEALTH CHECK
# ----------------------------------------

@app.route("/")
def health():
    return jsonify({
        "service": "india20sixty-render",
        "status": "running"
    })


# ----------------------------------------
# SAFE FILE DOWNLOAD
# ----------------------------------------

def download_file(url, path):

    try:
        r = requests.get(url, timeout=20)

        if r.status_code != 200:
            raise Exception(f"Download failed {url}")

        with open(path, "wb") as f:
            f.write(r.content)

        return True

    except Exception as e:
        return str(e)


# ----------------------------------------
# RENDER ENDPOINT
# ----------------------------------------

@app.route("/render", methods=["POST"])
def render_video():

    try:

        data = request.json

        job_id = data.get("job_id", str(uuid.uuid4()))
        images = data.get("images", [])
        audio = data.get("audio")

        if not images:
            return jsonify({"error": "No images provided"}), 400

        job_dir = f"{TMP_DIR}/{job_id}"
        os.makedirs(job_dir, exist_ok=True)

        image_paths = []

        # --------------------------------
        # DOWNLOAD IMAGES
        # --------------------------------

        for i, img_url in enumerate(images):

            img_path = f"{job_dir}/img{i}.png"

            result = download_file(img_url, img_path)

            if result is not True:
                return jsonify({
                    "error": "image download failed",
                    "url": img_url,
                    "details": result
                }), 400

            image_paths.append(img_path)

        # --------------------------------
        # DOWNLOAD AUDIO
        # --------------------------------

        audio_path = f"{job_dir}/audio.mp3"

        result = download_file(audio, audio_path)

        if result is not True:
            return jsonify({
                "error": "audio download failed",
                "details": result
            }), 400

        # --------------------------------
        # BUILD FFMPEG LIST
        # --------------------------------

        list_file = f"{job_dir}/images.txt"

        with open(list_file, "w") as f:

            for img in image_paths:
                f.write(f"file '{img}'\n")
                f.write("duration 5\n")

        output_video = f"{VIDEO_DIR}/{job_id}.mp4"

        # --------------------------------
        # RUN FFMPEG
        # --------------------------------

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

        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if process.returncode != 0:

            return jsonify({
                "error": "ffmpeg failed",
                "details": process.stderr.decode()
            }), 500

        # --------------------------------
        # SUCCESS RESPONSE
        # --------------------------------

        return jsonify({
            "status": "rendered",
            "job_id": job_id,
            "video": output_video
        })

    except Exception as e:

        return jsonify({
            "error": "server_exception",
            "message": str(e),
            "trace": traceback.format_exc()
        }), 500


# ----------------------------------------
# SERVER START
# ----------------------------------------

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
