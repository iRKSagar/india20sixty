import os
import requests
import json

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def process_job(job):

    script = job["script"]

    prompt = f"""
Break the following narration into 5 cinematic scenes.

Script:
{script}

Return JSON array of prompts.

Style:
futuristic India
cinematic lighting
ultra realistic
Indian environment
"""

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model":"gpt-4o-mini",
            "messages":[{"role":"user","content":prompt}]
        }
    )

    txt = r.json()["choices"][0]["message"]["content"]

    try:
        prompts = json.loads(txt)
    except:
        prompts = [script]*5

    job["visual_prompts"] = prompts

    return job
