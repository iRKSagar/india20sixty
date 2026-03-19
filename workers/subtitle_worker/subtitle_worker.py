import os
import requests

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def process_job(job):

    script = job["script"]

    prompt = f"""
Extract 5 short caption phrases from this narration.

Rules:
2-4 words each
Very punchy.

Script:
{script}
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

    captions = r.json()["choices"][0]["message"]["content"].split("\n")

    job["subtitles"] = captions[:5]

    return job
