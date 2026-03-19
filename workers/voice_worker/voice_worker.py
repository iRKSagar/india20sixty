import os
import requests

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID")

def process_job(job):

    script = job["script"]

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"

    r = requests.post(
        url,
        headers={
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "text": script,
            "model_id":"eleven_multilingual_v2"
        }
    )

    path = "/tmp/voice.mp3"

    with open(path,"wb") as f:
        f.write(r.content)

    job["voice"] = path

    return job
