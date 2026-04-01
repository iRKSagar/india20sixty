import modal
import os
import re
import json
import random
import requests
import xml.etree.ElementTree as ET
from pathlib import Path

# ==========================================
# MODAL APP — RESEARCH
# Google News RSS + PIB RSS + GPT fact extraction.
# Lightweight — pure network, no compute.
# ==========================================

app_research = modal.App("india20sixty-research")

light_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]


@app_research.function(image=light_image, secrets=secrets, cpu=0.25, memory=256, timeout=60)
def run_research(topic: str) -> dict:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

    print(f"\n[Research] topic: {topic}")

    def fetch_google_news(query):
        try:
            encoded = requests.utils.quote(query)
            url = (f"https://news.google.com/rss/search"
                   f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en")
            r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root  = ET.fromstring(r.content)
            items = root.findall(".//item")[:5]
            return [{"headline": i.findtext("title", "").strip(),
                     "source":   i.findtext("source", "News").strip()}
                    for i in items if i.findtext("title", "").strip()]
        except Exception as e:
            print(f"  Google News [{query[:25]}]: {e}")
            return []

    def fetch_pib():
        try:
            url = "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3"
            r   = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root  = ET.fromstring(r.content)
            items = root.findall(".//item")[:8]
            return [{"headline": i.findtext("title", "").strip(), "source": "PIB India"}
                    for i in items if i.findtext("title", "").strip()]
        except Exception as e:
            print(f"  PIB: {e}")
            return []

    headlines = []
    for q in [topic, f"{topic} India 2025"]:
        headlines += fetch_google_news(q)
    headlines += fetch_pib()

    seen, unique = set(), []
    for h in headlines:
        if h["headline"] not in seen:
            seen.add(h["headline"])
            unique.append(h)

    print(f"  Headlines: {len(unique)}")
    if not unique:
        return {"found": False}

    headlines_text = "\n".join(
        f"- {h['headline']} ({h['source']})" for h in unique[:10]
    )
    prompt = f"""Find the most relevant headline to topic: "{topic}"

Headlines:
{headlines_text}

Return ONLY JSON:
{{"found": true, "headline": "...", "source": "...", "key_fact": "specific stat or number"}}

If none relevant: {{"found": false}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.2, "max_tokens": 200},
            timeout=15,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        data    = json.loads(content[content.find('{'):content.rfind('}')+1])
        if data.get("found"):
            print(f"  Fact: {data.get('key_fact', '')[:80]}")
            return data
    except Exception as e:
        print(f"  Fact extract: {e}")

    return {"found": False}
