import requests
import uuid
import os
import boto3
from pathlib import Path

# ----------------------------------
# CONFIG
# ----------------------------------

ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")

VOICE_ID = "JsXbD9h4nEpbBMDxuEvT"

API_URL = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")

AUDIO_FOLDER = Path("/tmp/audio")
AUDIO_FOLDER.mkdir(parents=True, exist_ok=True)

# ----------------------------------
# R2 CLIENT
# ----------------------------------

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
)

# ----------------------------------
# SCRIPT → NARRATION
# ----------------------------------

def build_narration(script):

    narration = ". ".join([
        script["hook"],
        script["trend"],
        script["insight"],
        script["future"],
        script["question"]
    ])

    return narration


# ----------------------------------
# GENERATE VOICE
# ----------------------------------

def generate_voice(text, job_id):

    headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "text": text,
        "model_id": "eleven_multilingual_v2",
        "voice_settings": {
            "stability": 0.55,
            "similarity_boost": 0.75
        }
    }

    response = requests.post(API_URL, headers=headers, json=payload)

    audio_path = AUDIO_FOLDER / f"{job_id}.mp3"

    with open(audio_path, "wb") as f:
        f.write(response.content)

    return audio_path


# ----------------------------------
# UPLOAD AUDIO TO R2
# ----------------------------------

def upload_audio(audio_path, job_id):

    r2_key = f"audio/{job_id}.mp3"

    r2.upload_file(
        str(audio_path),
        R2_BUCKET,
        r2_key,
        ExtraArgs={"ContentType": "audio/mpeg"}
    )

    audio_url = f"{R2_ENDPOINT}/{R2_BUCKET}/{r2_key}"

    return audio_url


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    job_id = job["job_id"]

    script = job["script"]

    narration = build_narration(script)

    audio_file = generate_voice(narration, job_id)

    audio_url = upload_audio(audio_file, job_id)

    job["audio_voice"] = audio_url

    job["status"] = "voice_ready"

    return job


# ----------------------------------
# WORKER LOOP
# ----------------------------------

def run_worker():

    print("Voice Worker Started")

    job = {
        "job_id": str(uuid.uuid4()),
        "script": {
            "hook": "Socho agar AI doctors India mein common ho jayein.",
            "trend": "AI already hospitals mein scans analyse kar raha hai.",
            "insight": "Machines thousands of reports seconds mein process kar sakti hain.",
            "future": "2060 tak AI doctors rural India tak healthcare pahucha sakte hain.",
            "question": "Kya India ready hai AI healthcare revolution ke liye?"
        }
    }

    job = process_job(job)

    print("\nVoice Generated:")
    print(job["audio_voice"])


if __name__ == "__main__":
    run_worker()
