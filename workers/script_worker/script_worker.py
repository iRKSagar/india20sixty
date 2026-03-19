import os
import requests

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def process_job(job):

    topic = job["topic"]

    prompt = f"""
Write a 25 second YouTube Shorts narration about:
import random

def process_job(job):

    topic = job["topic"]

    hooks = [

        f"Socho agar {topic} reality ban jaye...",

        f"Sach bataun... {topic} India mein possible hai",

        f"2035 tak {topic} common ho sakta hai",

        f"Kya India {topic} ke liye ready hai?"

    ]

    hook = random.choice(hooks)

    script = {

        "topic": topic,

        "hook": hook,

        "trend":
        "India mein technology rapidly evolve ho rahi hai.",

        "insight":
        f"{topic} jaise innovations already research stage mein hain.",

        "future":
        "2060 tak ye system India ke millions logon ki life change kar sakta hai.",

        "question":
        "Aapko kya lagta hai — kya India ready hoga?"

    }

    # attach structured script

    job["script"] = script

    # full narration for ElevenLabs

    narration = " ".join([

        script["hook"],
        script["trend"],
        script["insight"],
        script["future"],
        script["question"]

    ])

    job["narration"] = narration

    return job
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
