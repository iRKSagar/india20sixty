import requests
import json
import uuid
from pathlib import Path

# ----------------------------------
# CONFIG
# ----------------------------------

ELEVENLABS_API_KEY = "YOUR_API_KEY"

VOICE_ID = "YOUR_VOICE_ID"

API_URL = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"

AUDIO_FOLDER = Path("assets/audio")

AUDIO_FOLDER.mkdir(parents=True, exist_ok=True)


# ----------------------------------
# SCRIPT TO TEXT
# ----------------------------------

def build_narration(script):

    narration = " ".join([
        script["hook"],
        script["trend"],
        script["insight"],
        script["future"],
        script["question"]
    ])

    return narration


# ----------------------------------
# GENERATE AUDIO
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

    return str(audio_path)


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    job_id = job["job_id"]

    script = job["script"]

    narration = build_narration(script)

    audio_file = generate_voice(narration, job_id)

    job["audio_voice"] = audio_file

    job["status"] = "voice_ready"

    return job


# ----------------------------------
# WORKER LOOP
# ----------------------------------

def run_worker():

    print("Voice Worker Started")

    while True:

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

        break


if __name__ == "__main__":

    run_worker()
