import modal
import os
import json
import requests
import xml.etree.ElementTree as ET

# ==========================================
# MODAL APP — RESEARCH
# Fetches real headlines and extracts fact anchors.
# Pure network calls — no compute, no ffmpeg.
# ==========================================

app = modal.App("india20sixty-research")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]


# ==========================================
# MAIN RESEARCH FUNCTION
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=60)
def run_research(job_id: str, topic: str) -> dict:
    """
    Fetch real headlines for a topic and extract the best fact anchor.
    Returns: { found, headline, source, key_fact }  or  { found: False }
    """
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

    print(f"\n[Research] job={job_id} topic={topic[:60]}")

    headlines = []
    for q in [topic, f"{topic} India 2025"]:
        headlines += _fetch_google_news(q)
    headlines += _fetch_pib()

    seen, unique = set(), []
    for h in headlines:
        if h["headline"] not in seen:
            seen.add(h["headline"])
            unique.append(h)

    print(f"  Headlines: {len(unique)}")
    if not unique:
        return {"found": False}

    headlines_text = "\n".join(
        f"- {h['headline']} ({h['source']})" for h in unique[:12]
    )
    prompt = f"""Find the most relevant headline to topic: "{topic}"

Headlines:
{headlines_text}

Return ONLY JSON:
{{"found": true, "headline": "...", "source": "...", "key_fact": "specific stat or number from the headline"}}

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
        data    = json.loads(content[content.find("{"):content.rfind("}") + 1])
        if data.get("found"):
            print(f"  Fact: {data.get('key_fact', '')[:80]}")
            return data
    except Exception as e:
        print(f"  Fact extract failed: {e}")

    return {"found": False}


# ==========================================
# HELPERS
# ==========================================

def _fetch_google_news(query: str) -> list:
    try:
        encoded = requests.utils.quote(query)
        url = (
            f"https://news.google.com/rss/search"
            f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
        )
        r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        root  = ET.fromstring(r.content)
        items = root.findall(".//item")[:5]
        results = []
        for item in items:
            title  = item.findtext("title", "").strip()
            source = item.findtext("source", "").strip()
            if title:
                results.append({"headline": title, "source": source or "News"})
        return results
    except Exception as e:
        print(f"  News [{query[:25]}]: {e}")
        return []


def _fetch_pib() -> list:
    try:
        url = "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3"
        r   = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        root  = ET.fromstring(r.content)
        items = root.findall(".//item")[:8]
        return [
            {"headline": i.findtext("title", "").strip(), "source": "PIB India"}
            for i in items if i.findtext("title", "").strip()
        ]
    except Exception as e:
        print(f"  PIB: {e}")
        return []


@app.local_entrypoint()
def main():
    result = run_research.remote("test-001", "ISRO space station India")
    print("Result:", result)
