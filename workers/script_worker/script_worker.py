import os
import requests

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def process_job(job):

    topic = job["topic"]

    prompt = f"""
Write a 25 second YouTube Shorts narration about:

{topic}

Structure:
Hook
Context
Insight
Future
Question

Keep sentences short and engaging.
"""

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "gpt-4o-mini",
            "messages":[{"role":"user","content":prompt}]
        }
    )

    script = r.json()["choices"][0]["message"]["content"]

    job["script"] = script

    return job
