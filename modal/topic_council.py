import modal
import os
import re
import json
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# ============================================================
# India20Sixty — Topic Council Worker
# Single web endpoint handles all routes:
#   GET  /?action=health       — liveness + queue depth
#   POST /?action=replenish    — fetch headlines, score, save
#   POST /?action=full-pipeline — score + script single topic
#   POST (no action)           — defaults to replenish
# ============================================================

app = modal.App("india20sixty-topic-council")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("openai>=1.0.0", "requests>=2.31.0", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

ALL_CATS = ["AI", "Space", "Gadgets", "DeepTech", "GreenTech", "Startups"]

CATEGORY_QUERIES = {
    "AI":       ["India artificial intelligence 2026","India AI startup funding 2026","Indian AI model LLM 2026","India machine learning 2026"],
    "Space":    ["ISRO 2026 mission","India space programme 2026","Gaganyaan ISRO news","India satellite defence 2026"],
    "Gadgets":  ["India semiconductor chip 2026","Made in India electronics 2026","India EV electric vehicle 2026","India drone technology 2026"],
    "DeepTech": ["India deep tech startup 2026","IIT research breakthrough 2026","India quantum computing 2026","India robotics 3D printing 2026"],
    "GreenTech":["India solar energy 2026","India green hydrogen 2026","India clean energy 2026","India EV infrastructure 2026"],
    "Startups": ["India unicorn startup 2026","India fintech agritech 2026","India startup funding 2026","India edtech healthtech 2026"],
}

WHITELIST = ["isro","nasa","satellite","rocket","space","moon","mars","gaganyaan","ai","artificial intelligence","machine learning","llm","startup","unicorn","funding","semiconductor","chip","drone","ev","electric","solar","hydrogen","clean energy","iit","research","innovation","technology","tech","defence","quantum","biotech","robot","india","indian","bharat"]
BLACKLIST = ["cricket","ipl","football","sport","athlete","bollywood","film","movie","actor","celebrity","entertainment","politics","election","party","parliament","vote","religion","temple","mosque","stock market","sensex","nifty","crypto","murder","crime","rape","accident","death","weather","flood","earthquake","disaster","cyclone"]


@app.function(image=image, secrets=secrets, cpu=1.0, memory=1024, timeout=300)
@modal.fastapi_endpoint(method="GET")
def council():
    """Health check — GET /"""
    return _health()


@app.function(image=image, secrets=secrets, cpu=1.0, memory=1024, timeout=300)
@modal.fastapi_endpoint(method="POST")
async def council_post(request):
    """
    Single POST endpoint for all actions.
    Body: { action: 'replenish'|'full-pipeline'|'health', ...params }
    """
    from fastapi.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        body = {}

    action = body.get("action", "replenish")

    if action == "health":
        return _health()

    if action == "full-pipeline":
        from fastapi.responses import JSONResponse
        topic    = body.get("topic", "")
        category = body.get("category", "AI")
        source   = body.get("source", "manual")
        if not topic:
            return JSONResponse({"error": "Missing topic"}, status_code=400)
        r = _council_score(topic, "", category)
        if not r:
            return JSONResponse({"error": "Score below 70"}, status_code=422)
        return {
            "topic":          r["video_angle"],
            "cluster":        r["cluster"],
            "council_score":  r["council_score"],
            "script_package": r.get("script_package"),
            "source":         source,
        }

    # Default: replenish
    categories = [c for c in (body.get("categories") or ALL_CATS) if c in ALL_CATS] or ALL_CATS
    target = int(body.get("target") or 12)
    return _replenish(categories, target)


# ==========================================
# CORE FUNCTIONS
# ==========================================

def _health() -> dict:
    try:
        qd = _sb_count("topics?used=eq.false&council_score=gte.70")
        sr = _sb_count("topics?used=eq.false&council_score=gte.70&cluster=eq.Space")
        return {"status":"healthy","queue_depth":qd,"space_ready":sr,
                "time":datetime.now(timezone.utc).isoformat(),"platform":"modal"}
    except Exception as e:
        return {"status":"error","error":str(e)}


def _replenish(categories: list, target: int) -> dict:
    print(f"[Replenish] categories={categories} target={target}")

    all_headlines = []
    for cat in categories:
        for q in CATEGORY_QUERIES.get(cat, []):
            hs = _fetch_google_news(q)
            for h in hs: h["category_hint"] = cat
            all_headlines.extend(hs)
    all_headlines.extend(_fetch_pib())

    seen, unique = set(), []
    for h in all_headlines:
        k = h["title"].lower()[:60]
        if k not in seen:
            seen.add(k)
            unique.append(h)

    filtered = []
    for h in unique:
        text = (h["title"] + " " + h.get("summary","")).lower()
        if any(b in text for b in BLACKLIST): continue
        if not any(w in text for w in WHITELIST): continue
        filtered.append(h)

    print(f"  Raw: {len(all_headlines)} → Filtered: {len(filtered)}")
    if not filtered:
        return {"status":"no_headlines","added":0}

    sample = random.sample(filtered, min(len(filtered), target * 3))
    approved, added = [], 0

    for h in sample:
        if len(approved) >= target: break
        try:
            r = _council_score(h["title"], h.get("summary",""), h.get("category_hint","AI"))
            if r: approved.append({**h, **r})
        except Exception as e:
            print(f"  Score error: {e}")

    for t in approved:
        try:
            _sb_insert("topics", {
                "topic":          t["video_angle"],
                "cluster":        t["cluster"],
                "council_score":  t["council_score"],
                "script_package": t.get("script_package"),
                "source":         "google_news_rss",
                "used":           False,
                "created_at":     datetime.now(timezone.utc).isoformat(),
            })
            added += 1
        except Exception as e:
            print(f"  Insert error: {e}")

    print(f"  Added: {added}/{len(approved)} approved")
    return {"status":"ok","headlines":len(filtered),"approved":len(approved),"added":added,"categories":categories}


# ==========================================
# RSS FETCHERS
# ==========================================

def _fetch_google_news(query: str) -> list:
    import requests
    try:
        url = f"https://news.google.com/rss/search?q={query.replace(' ','+')}&hl=en-IN&gl=IN&ceid=IN:en"
        r = requests.get(url, timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        if not r.ok: return []
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:8]:
            title = re.sub(r"\s*-\s*[^-]+$","",item.findtext("title","")).strip()
            summary = item.findtext("description","").strip()
            if title:
                items.append({"title":title,"summary":summary[:300],"source":item.findtext("link","")})
        return items
    except Exception as e:
        print(f"  Google News error ({query[:30]}): {e}")
        return []


def _fetch_pib() -> list:
    import requests
    try:
        r = requests.get("https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
                         timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        if not r.ok: return []
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:10]:
            title = item.findtext("title","").strip()
            if title:
                items.append({"title":title,"summary":item.findtext("description","")[:300],
                              "source":"PIB","category_hint":"Space"})
        return items
    except Exception as e:
        print(f"  PIB error: {e}")
        return []


# ==========================================
# GPT COUNCIL SCORING
# ==========================================

def _council_score(headline: str, summary: str, category_hint: str):
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    prompt = f"""You are a content council for India20Sixty — YouTube Shorts about India's near future.
Evaluate this headline for a 25-second Short.

HEADLINE: {headline}
SUMMARY: {summary[:200]}
CATEGORY HINT: {category_hint}

Respond ONLY with valid JSON (no markdown):
{{
  "video_angle": "Compelling angle for a Short (max 120 chars)",
  "cluster": "One of: AI, Space, Gadgets, DeepTech, GreenTech, Startups",
  "key_fact": "Single most interesting verifiable fact",
  "virality_score": 0-100,
  "factual_strength": 0-100,
  "visual_potential": 0-100,
  "safety_score": 0-100,
  "relevance_score": 0-100,
  "council_score": "Integer average of all 5 scores",
  "script": {{
    "text": "55-word Hinglish narration. English sentences with 2-3 Hindi words naturally placed (Yaar/Sach mein/Desh/Bhai/Kya baat). Anchor to key_fact. Hook in first 8 words. End with question about India future.",
    "mood": "One of: cinematic_epic, breaking_news, hopeful_future, cold_tech, vibrant_pop, nostalgic_film, warm_human",
    "scene_prompts": ["Scene 1 image brief (no text/logos)", "Scene 2", "Scene 3"]
  }}
}}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.7,
        max_tokens=800,
        response_format={"type":"json_object"},
    )
    data = json.loads(response.choices[0].message.content.strip())
    score = int(data.get("council_score", 0))
    if score < 70: return None

    s = data.get("script", {})
    return {
        "video_angle":    data.get("video_angle", headline),
        "cluster":        data.get("cluster", category_hint),
        "council_score":  score,
        "key_fact":       data.get("key_fact",""),
        "script_package": {
            "text":            s.get("text",""),
            "reviewed_script": s.get("text",""),
            "mood":            s.get("mood","hopeful_future"),
            "scene_prompts":   s.get("scene_prompts",[]),
            "key_fact":        data.get("key_fact",""),
            "source":          "council",
            "word_count":      len(s.get("text","").split()),
            "generated_at":    datetime.now(timezone.utc).isoformat(),
        },
    }


# ==========================================
# SUPABASE HELPERS
# ==========================================

def _sb_headers():
    url = os.environ["SUPABASE_URL"]
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ["SUPABASE_ANON_KEY"]
    return url, {"apikey":key,"Authorization":f"Bearer {key}","Content-Type":"application/json"}


def _sb_count(endpoint: str) -> int:
    import requests
    url, headers = _sb_headers()
    headers["Prefer"] = "count=exact"
    r = requests.get(f"{url}/rest/v1/{endpoint}&select=id", headers=headers, timeout=10)
    try:
        return int(r.headers.get("content-range","*/0").split("/")[-1])
    except Exception:
        return 0


def _sb_insert(table: str, data: dict):
    import requests
    url, headers = _sb_headers()
    headers["Prefer"] = "return=minimal"
    r = requests.post(f"{url}/rest/v1/{table}", headers=headers, json=data, timeout=10)
    if not r.ok:
        raise Exception(f"INSERT {r.status_code}: {r.text[:200]}")


@app.local_entrypoint()
def main():
    print("Health:", council.remote())
