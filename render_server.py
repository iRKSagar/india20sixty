from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/")
def health():
    return {"service": "india20sixty-render", "status": "running"}

@app.route("/render", methods=["POST"])
def render_video():

    data = request.json

    images = data.get("images", [])
    audio = data.get("audio")
    job_id = data.get("job_id")

    # placeholder for ffmpeg render
    video_path = f"videos/{job_id}.mp4"

    return jsonify({
        "status": "rendered",
        "video": video_path
    })

if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
