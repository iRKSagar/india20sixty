import modal
import os
import re
import json
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

app = modal.App("india20sixty-topic-council")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("openai>=1.0.0", "requests>=2.31.0", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

ALL_CATS = ["AI", "Space", "Gadgets", "DeepTech", "GreenTech", "Startups"]

CATEGORY_QUERIES = {
    "AI":       ["India AI startup 2026","India artificial intelligence launch","Indian AI model release","India machine learning breakthrough"],
    "Space":    ["ISRO mission 2026","India space launch","Gaganyaan update","India satellite launch 2026"],
    "Gadgets":  ["India semiconductor manufacturing","Made in India smartphone","India EV launch 2026","India drone policy"],
    "DeepTech": ["IIT research breakthrough","India quantum computing","India biotech startup","India deep tech funding"],
    "GreenTech":["India solar power record","India green hydrogen","India renewable energy","India EV charging"],
    "Startups": ["India startup funding 2026","India unicorn valuation","India fintech growth","India edtech agritech"],
}

# Seed headlines used when RSS fails (Modal IPs often get blocked by Google)
SEED_HEADLINES = {
    "AI": [
        {"title":"India's AI startup ecosystem raises record funding in 2026","summary":"Indian AI companies secure billions in venture capital","category_hint":"AI"},
        {"title":"IIT researchers develop new large language model for Indian languages","summary":"Multilingual AI model handles 22 Indian languages natively","category_hint":"AI"},
        {"title":"India launches national AI compute infrastructure initiative","summary":"Government announces GPU clusters for AI research institutions","category_hint":"AI"},
        {"title":"Indian AI firm beats global competitors in medical imaging accuracy","summary":"Bangalore startup wins international AI healthcare benchmark","category_hint":"AI"},
    ],
    "Space": [
        {"title":"ISRO announces Gaganyaan astronaut training completion","summary":"Four Indian astronauts complete final training phase for 2026 mission","category_hint":"Space"},
        {"title":"India's commercial space sector attracts 10 new startups in 2026","summary":"Private space companies emerge from ISRO's IN-SPACe program","category_hint":"Space"},
        {"title":"ISRO successfully tests docking technology for space station","summary":"Critical technology validated for India's 2035 space station goal","category_hint":"Space"},
        {"title":"India launches earth observation satellite for agriculture monitoring","summary":"New satellite will help 600 million farmers with crop data","category_hint":"Space"},
    ],
    "Gadgets": [
        {"title":"India's first homegrown 5G chip enters mass production","summary":"Semiconductor startup achieves milestone in chip manufacturing","category_hint":"Gadgets"},
        {"title":"India EV sales cross 1 million units monthly for first time","summary":"Electric two-wheelers lead the charge in India's EV revolution","category_hint":"Gadgets"},
        {"title":"Made in India drone achieves 200km range record","summary":"Indian drone manufacturer breaks world record for range and payload","category_hint":"Gadgets"},
        {"title":"India launches affordable 5G smartphone at Rs 8000","summary":"Domestic manufacturer targets next billion internet users","category_hint":"Gadgets"},
    ],
    "DeepTech": [
        {"title":"IIT Bombay develops room-temperature superconductor breakthrough","summary":"Indian researchers achieve milestone that could revolutionize electronics","category_hint":"DeepTech"},
        {"title":"India's quantum computing startup achieves 100-qubit milestone","summary":"Bangalore firm joins global quantum computing race","category_hint":"DeepTech"},
        {"title":"Indian biotech firm develops dengue vaccine using AI","summary":"Novel approach combines machine learning with biotechnology","category_hint":"DeepTech"},
        {"title":"India 3D printing company manufactures entire bridge in 72 hours","summary":"Construction technology startup demonstrates rapid manufacturing capability","category_hint":"DeepTech"},
    ],
    "GreenTech": [
        {"title":"India achieves 200 GW solar capacity milestone","summary":"Renewable energy target hit two years ahead of schedule","category_hint":"GreenTech"},
        {"title":"India's green hydrogen exports begin to Europe","summary":"First shipment marks India's entry into global clean fuel market","category_hint":"GreenTech"},
        {"title":"India builds world's largest floating solar farm","summary":"10,000 MW project on reservoir powers 5 million homes","category_hint":"GreenTech"},
        {"title":"India EV battery recycling industry emerges as global leader","summary":"Circular economy approach creates 50,000 jobs in battery tech","category_hint":"GreenTech"},
    ],
    "Startups": [
        {"title":"Indian fintech startup becomes youngest decacorn in Asia","summary":"Payments company valued at 10 billion dollars in Series D round","category_hint":"Startups"},
        {"title":"India agritech startup brings AI to 100 million farmers","summary":"Platform helps small farmers access credit, market prices and weather data","category_hint":"Startups"},
        {"title":"India edtech pivot to skill training creates new unicorn","summary":"Online learning platform focuses on employability over degrees","category_hint":"Startups"},
        {"title":"India health startup digitises primary care for rural areas","summary":"Telemedicine platform reaches 500 million unserved patients","category_hint":"Startups"},
    ],
}

WHITELIST = [
    "isro","nasa","satellite","rocket","space","moon","mars","gaganyaan",
    "ai","artificial intelligence","machine learning","llm","model",
    "startup","unicorn","funding","series","crore","million","billion",
    "semiconductor","chip","drone","ev","electric vehicle","solar","hydrogen",
    "clean energy","renewable","iit","research","innovation","technology","tech",
    "defence","quantum","biotech","robot","robotics","india","indian","bharat",
    "bangalore","mumbai","delhi","hyderabad","pune","chennai",
    "software","digital","internet","app","platform","data","launch",
]
BLACKLIST = [
    "cricket","ipl","football","fifa","sport","athlete","jersey","match","stadium",
    "bollywood","film","movie","actor","actress","celebrity","entertainment","oscar",
    "election","party","parliament","minister","vote","bjp","congress","aap",
    "temple","mosque","church","devotion","pilgrimage",
    "sensex","nifty","trading","forex","crypto","bitcoin",
    "murder","crime","rape","accident","casualty","obituary","funeral",
    "weather","flood","earthquake","disaster","cyclone","drought",
]


from fastapi import Request
from fastapi.responses import JSONResponse

@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=30)
@modal.fastapi_endpoint(method="GET")
def council():
    return _health()


@app.function(image=image, secrets=secrets, cpu=1.0, memory=1024, timeout=300)
@modal.fastapi_endpoint(method="POST")
async def council_post(request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    action = body.get("action", "replenish")
    print(f"[Council] action={action}")

    if action == "health":
        return _health()

    if action == "full-pipeline":
        topic    = body.get("topic", "")
        category = body.get("category", "AI")
        source   = body.get("source", "manual")
        if not topic:
            return JSONResponse({"error": "Missing topic"}, status_code=400)
        r = _council_score(topic, "", category)
        if not r:
            return JSONResponse({"error": "Score below 65"}, status_code=422)
        return {"topic": r["video_angle"], "cluster": r["cluster"],
                "council_score": r["council_score"],
                "script_package": r.get("script_package"), "source": source}

    cats   = [c for c in (body.get("categories") or ALL_CATS) if c in ALL_CATS] or ALL_CATS
    target = int(body.get("target") or 12)
    return _replenish(cats, target)


def _health() -> dict:
    try:
        qd = _sb_count("topics?used=eq.false&council_score=gte.70")
        sr = _sb_count("topics?used=eq.false&council_score=gte.70&cluster=eq.Space")
        return {"status":"healthy","queue_depth":qd,"space_ready":sr,
                "time":datetime.now(timezone.utc).isoformat(),"platform":"modal"}
    except Exception as e:
        return {"status":"error","error":str(e)}


def _replenish(categories: list, target: int) -> dict:
    print(f"[Replenish] cats={categories} target={target}")
    all_headlines = []

    # Try RSS first
    for cat in categories:
        for q in CATEGORY_QUERIES.get(cat, []):
            hs = _fetch_google_news(q)
            for h in hs: h["category_hint"] = cat
            all_headlines.extend(hs)
    pib = _fetch_pib()
    all_headlines.extend(pib)

    # Reddit RSS — no API key needed, server-friendly
    reddit = _fetch_reddit(categories)
    all_headlines.extend(reddit)
    print(f"  Total headlines: {len(all_headlines)} (Google+PIB+Reddit)")

    # If RSS failed/blocked, use seed headlines
    if len(all_headlines) < 5:
        print("  RSS blocked or empty — using seed headlines")
        for cat in categories:
            seeds = SEED_HEADLINES.get(cat, [])
            all_headlines.extend(random.sample(seeds, min(len(seeds), 3)))
        print(f"  After seeds: {len(all_headlines)} headlines")

    # Deduplicate
    seen, unique = set(), []
    for h in all_headlines:
        k = h["title"].lower()[:60]
        if k not in seen:
            seen.add(k)
            unique.append(h)

    # Filter
    filtered = []
    for h in unique:
        text = (h["title"] + " " + h.get("summary","")).lower()
        if any(b in text for b in BLACKLIST):
            continue
        if not any(w in text for w in WHITELIST):
            continue
        filtered.append(h)

    print(f"  Filtered: {len(filtered)}")
    if not filtered:
        filtered = unique  # Skip filter if everything filtered out

    sample = random.sample(filtered, min(len(filtered), target * 2))
    approved, added = [], 0

    for h in sample:
        if len(approved) >= target: break
        try:
            r = _council_score(h["title"], h.get("summary",""), h.get("category_hint","AI"))
            if r:
                approved.append({**h, **r})
                print(f"  ✓ Approved ({r['cluster']} {r['council_score']}): {r['video_angle'][:60]}")
        except Exception as e:
            print(f"  Score error: {e}")

    for t in approved:
        try:
            _sb_insert("topics", {
                "topic":          t["video_angle"],
                "cluster":        t["cluster"],
                "council_score":  t["council_score"],
                "script_package": t.get("script_package"),
                "source":         "council",
                "used":           False,
                "created_at":     datetime.now(timezone.utc).isoformat(),
            })
            added += 1
            print(f"  Saved: {t['video_angle'][:50]}")
        except Exception as e:
            print(f"  Insert error: {e}")

    print(f"  Done: {added} added")
    return {"status":"ok","rss_headlines":len(all_headlines),"filtered":len(filtered),
            "approved":len(approved),"added":added,"categories":categories}


def _fetch_google_news(query: str) -> list:
    import requests
    try:
        url = f"https://news.google.com/rss/search?q={query.replace(' ','+')}&hl=en-IN&gl=IN&ceid=IN:en"
        r = requests.get(url, timeout=12, headers={
            "User-Agent":"Mozilla/5.0 (compatible; Googlebot/2.1)",
            "Accept":"application/rss+xml,text/xml,*/*"
        })
        if not r.ok:
            print(f"  RSS {r.status_code}: {query[:30]}")
            return []
        # Detect captcha/non-XML response
        if b"<rss" not in r.content[:200] and b"<?xml" not in r.content[:200]:
            print(f"  RSS non-XML (blocked): {query[:30]}")
            return []
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:8]:
            title = re.sub(r"\s*-\s*[^-]+$","",item.findtext("title","")).strip()
            summary = re.sub(r"<[^>]+>","",item.findtext("description","")).strip()
            if title and len(title) > 10:
                items.append({"title":title,"summary":summary[:300],"source":item.findtext("link","")})
        print(f"  RSS OK ({len(items)} items): {query[:30]}")
        return items
    except Exception as e:
        print(f"  RSS error ({query[:30]}): {e}")
        return []


def _fetch_pib() -> list:
    import requests
    try:
        r = requests.get("https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
                         timeout=10, headers={"User-Agent":"Mozilla/5.0"})
        if not r.ok: return []
        if b"<?xml" not in r.content[:200] and b"<rss" not in r.content[:200]:
            return []
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:10]:
            title = item.findtext("title","").strip()
            if title:
                items.append({"title":title,"summary":item.findtext("description","")[:300],
                              "source":"PIB","category_hint":"Space"})
        print(f"  PIB: {len(items)} items")
        return items
    except Exception as e:
        print(f"  PIB error: {e}")
        return []


# Subreddits mapped to categories
REDDIT_SOURCES = {
    "Space":    ["ISRO", "india", "space"],
    "AI":       ["artificial", "MachineLearning", "developersIndia"],
    "Gadgets":  ["india", "IndianGaming", "developersIndia"],
    "DeepTech": ["india", "science", "developersIndia"],
    "GreenTech":["india", "environment", "renewable"],
    "Startups": ["india", "indianstartups", "entrepreneur"],
}

REDDIT_SEARCH = {
    "Space":    "ISRO space India 2026",
    "AI":       "India AI technology 2026",
    "Gadgets":  "India tech gadget launch 2026",
    "DeepTech": "India research innovation breakthrough",
    "GreenTech":"India solar renewable energy",
    "Startups": "India startup funding 2026",
}

def _fetch_reddit(categories: list) -> list:
    import requests
    items = []
    headers = {
        "User-Agent": "india20sixty-bot/1.0 (content curation for YouTube; contact@india20sixty.com)",
        "Accept": "application/json",
    }

    # 1. Fetch from specific subreddits top posts
    subreddits_done = set()
    for cat in categories:
        for sub in REDDIT_SOURCES.get(cat, [])[:2]:
            if sub in subreddits_done: continue
            subreddits_done.add(sub)
            try:
                url = f"https://www.reddit.com/r/{sub}/top.json?t=week&limit=10"
                r = requests.get(url, headers=headers, timeout=10)
                if not r.ok:
                    print(f"  Reddit r/{sub}: {r.status_code}")
                    continue
                posts = r.json().get("data", {}).get("children", [])
                for p in posts:
                    d = p.get("data", {})
                    title = d.get("title", "").strip()
                    selftext = d.get("selftext", "")[:200]
                    score = d.get("score", 0)
                    if title and score > 50 and not d.get("is_video"):
                        items.append({
                            "title": title,
                            "summary": selftext,
                            "source": f"reddit.com/r/{sub}",
                            "category_hint": cat,
                            "reddit_score": score,
                        })
                print(f"  Reddit r/{sub}: {len(posts)} posts")
            except Exception as e:
                print(f"  Reddit r/{sub} error: {e}")

    # 2. Search Reddit for India tech topics
    for cat in categories[:3]:  # Limit to avoid rate limits
        query = REDDIT_SEARCH.get(cat, "India technology 2026")
        try:
            url = f"https://www.reddit.com/search.json?q={query.replace(' ','+')}&sort=top&t=week&limit=8"
            r = requests.get(url, headers=headers, timeout=10)
            if r.ok:
                posts = r.json().get("data", {}).get("children", [])
                for p in posts:
                    d = p.get("data", {})
                    title = d.get("title", "").strip()
                    score = d.get("score", 0)
                    if title and score > 30:
                        items.append({
                            "title": title,
                            "summary": d.get("selftext", "")[:200],
                            "source": f"reddit.com/r/{d.get('subreddit','')}",
                            "category_hint": cat,
                        })
                print(f"  Reddit search [{cat}]: {len(posts)} posts")
        except Exception as e:
            print(f"  Reddit search error: {e}")

    print(f"  Reddit total: {len(items)} posts")
    return items


def _council_score(headline: str, summary: str, category_hint: str):
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    today = datetime.now(timezone.utc).strftime("%B %d, %Y")  # e.g. "April 05, 2026"

    prompt = f"""You are a content council for India20Sixty — YouTube Shorts about India's near future.
Today's date is {today}. Treat anything before this date as PAST history, not future news.
Evaluate this headline for a 25-second Short.

HEADLINE: {headline}
SUMMARY: {summary[:200]}
CATEGORY HINT: {category_hint}

IMPORTANT: If this headline describes something that already happened before {today}, 
the video_angle must frame it as "India did X" or "How India achieved X" — not as upcoming news.
Only use future tense if the event genuinely has not happened yet as of {today}.

Respond ONLY with valid JSON (no markdown, no extra text):
{{
  "video_angle": "Compelling angle for a Short (max 120 chars, plain English)",
  "cluster": "One of: AI, Space, Gadgets, DeepTech, GreenTech, Startups",
  "key_fact": "Single most interesting verifiable fact from this story",
  "virality_score": 75,
  "factual_strength": 80,
  "visual_potential": 75,
  "safety_score": 95,
  "relevance_score": 85,
  "council_score": 82,
  "script": {{
    "text": "Write a 50-55 word script in pure intelligent Indian English. No Hindi. No Hinglish. Confident educated Indian voice. Anchor to key_fact. First sentence hooks with a specific number or fact. End with a debate question about India's future. Count words carefully — must be 50-55.",
    "mood": "One of: cinematic_epic, breaking_news, hopeful_future, cold_tech, vibrant_pop, nostalgic_film, warm_human",
    "scene_prompts": [
      "Scene 1: photorealistic modern Indian setting, specific visual brief",
      "Scene 2: different angle, no text or logos",
      "Scene 3: wide or establishing shot"
    ]
  }}
}}"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.7,
        max_tokens=900,
        response_format={"type":"json_object"},
    )
    raw = response.choices[0].message.content.strip()
    data = json.loads(raw)
    score = int(data.get("council_score", 0))
    print(f"  GPT score={score} angle={data.get('video_angle','')[:50]}")
    if score < 65: return None  # Lowered from 70 to 65

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
            "captions":        [],
            "key_fact":        data.get("key_fact",""),
            "source":          "council",
            "word_count":      len(s.get("text","").split()),
            "generated_at":    datetime.now(timezone.utc).isoformat(),
        },
    }


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
        raise Exception(f"INSERT {r.status_code}: {r.text[:300]}")


@app.local_entrypoint()
def main():
    print(council.remote())