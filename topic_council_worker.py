"""
India20Sixty — Topic Council Worker v3.0
==========================================
Real-news pipeline with 6 categories, whitelist+blacklist filtering,
language expert pronunciation review, and emotion-tagged scripts.

Categories:
  AI         — AI, ML, automation, robotics
  Space      — ISRO, satellites, defence tech
  Gadgets    — consumer tech, chips, EVs, devices
  DeepTech   — biotech, quantum, 3D print, IIT research
  GreenTech  — solar, EV infra, clean energy, hydrogen
  Startups   — funded startups, unicorns, fintech, agritech

Endpoints:
  GET  /health
  GET  /queue-status
  POST /replenish          { target, categories? }
  POST /full-pipeline      { topic, source, category? }
"""

from flask import Flask, request, jsonify
import os
import json
import requests
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime

app = Flask(__name__)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SUPABASE_URL   = os.environ.get("SUPABASE_URL")
SUPABASE_KEY   = os.environ.get("SUPABASE_ANON_KEY")

# ==========================================
# CATEGORIES
# ==========================================

CATEGORIES = {
    "AI": {
        "label":       "AI & Machine Learning",
        "description": "LLMs, computer vision, automation, AI startups, AI in healthcare/farming/education",
        "queries": [
            "India artificial intelligence startup 2025",
            "India AI machine learning deployment",
            "India automation robotics factory",
            "India AI healthcare diagnosis",
            "India generative AI LLM",
        ],
        "whitelist": ["ai", "artificial intelligence", "machine learning", "deep learning",
                      "neural", "llm", "generative", "chatbot", "automation", "robot",
                      "computer vision", "nlp", "data science", "algorithm"],
    },
    "Space": {
        "label":       "Space & Defence Tech",
        "description": "ISRO missions, satellites, defence R&D, DRDO, aerospace startups",
        "queries": [
            "ISRO India space mission 2025",
            "India satellite launch technology",
            "India defence technology DRDO",
            "India aerospace startup",
            "Chandrayaan Gaganyaan update",
        ],
        "whitelist": ["isro", "space", "satellite", "rocket", "chandrayaan", "gaganyaan",
                      "drdo", "defence technology", "missile", "aerospace", "launch vehicle",
                      "orbit", "lunar", "mars", "astronomy"],
    },
    "Gadgets": {
        "label":       "Gadgets & Consumer Tech",
        "description": "Smartphones, chips, semiconductors, wearables, drones, new devices",
        "queries": [
            "India semiconductor chip manufacturing 2025",
            "India smartphone technology innovation",
            "India drone technology startup",
            "India wearable device health tech",
            "India consumer electronics innovation",
        ],
        "whitelist": ["gadget", "device", "smartphone", "chip", "semiconductor", "processor",
                      "wearable", "drone", "iot", "sensor", "display", "5g", "6g",
                      "broadband", "camera tech", "electric vehicle", "ev charging"],
    },
    "DeepTech": {
        "label":       "Deep Tech & Innovation",
        "description": "Biotech, quantum computing, robotics, 3D printing, IIT/IIM research",
        "queries": [
            "India quantum computing research 2025",
            "India biotech biotechnology startup",
            "India IIT research innovation breakthrough",
            "India 3D printing manufacturing",
            "India deep tech startup funding",
        ],
        "whitelist": ["biotech", "quantum", "3d print", "genomics", "crispr", "nanotech",
                      "iit", "iim", "research", "patent", "deep tech", "r&d",
                      "materials science", "photonics", "superconductor"],
    },
    "GreenTech": {
        "label":       "Green & Energy Tech",
        "description": "Solar, wind, hydrogen, batteries, clean energy, EV infrastructure",
        "queries": [
            "India solar energy renewable 2025",
            "India electric vehicle EV infrastructure",
            "India green hydrogen clean energy",
            "India battery storage technology",
            "India climate tech startup",
        ],
        "whitelist": ["solar", "renewable", "wind power", "green energy", "ev", "electric vehicle",
                      "battery", "hydrogen", "clean energy", "emission", "climate tech",
                      "net zero", "carbon", "energy storage"],
    },
    "Startups": {
        "label":       "India Startup Ecosystem",
        "description": "Funded startups, unicorns, new products, fintech, agritech, Make in India",
        "queries": [
            "India startup funding unicorn 2025",
            "India fintech UPI digital payment innovation",
            "India agritech farming technology",
            "India healthtech medical startup",
            "India edtech education technology",
        ],
        "whitelist": ["startup", "unicorn", "funding", "venture capital", "innovation",
                      "fintech", "upi", "digital payment", "agritech", "healthtech",
                      "edtech", "make in india", "d2c", "saas", "b2b tech"],
    },
}

ALL_CATEGORY_KEYS = list(CATEGORIES.keys())

# ==========================================
# FILTERS
# ==========================================

BLACKLIST_KEYWORDS = [
    # Sports
    "cricket", "hockey", "football", "soccer", "tennis", "badminton",
    "kabaddi", "basketball", "volleyball", "wrestling", "boxing",
    "olympic", "commonwealth games", "cwg", "asian games", "ipl",
    "bcci", "fifa", "icc", " match", "tournament", "trophy", "league",
    "batsman", "bowler", "goalkeeper", "wicket", "century", "runs",
    # Entertainment
    "bollywood", " movie", " film", "actor", "actress", "celebrity",
    "singer", "musician", "album", " song", "concert", "award show",
    "oscar", "filmfare", "box office", "ott release", "web series",
    # Politics
    "election", " vote", "polling", "political party", " minister",
    "parliament", "lok sabha", "rajya sabha", "chief minister",
    "governor", " mla ", " mp ", "manifesto", "rally",
    # Religion
    "temple", "mosque", "church", "gurudwara", "diwali", "eid",
    "holi", "navratri", "puja", "pilgrimage", "yatra",
    # Crime & Disasters
    "murder", "rape", "crime", "theft", "fraud", "scam", "arrested",
    "flood", "earthquake", "cyclone", "landslide", "road accident",
    # Pure Finance
    "sensex", "nifty", "stock market", "share price", "ipo listing",
    "inflation rate", "repo rate",
    # Celebrity
    "influencer", "viral video", "meme", "tiktok", "reels trend",
]

def is_blacklisted(text):
    t = text.lower()
    return any(kw in t for kw in BLACKLIST_KEYWORDS)

def is_whitelisted(text, category=None):
    t = text.lower()
    if category and category in CATEGORIES:
        return any(kw in t for kw in CATEGORIES[category]["whitelist"])
    # Check all categories
    return any(
        any(kw in t for kw in cat["whitelist"])
        for cat in CATEGORIES.values()
    )

def detect_category(text):
    """Detect best category match for a topic."""
    t = text.lower()
    scores = {}
    for key, cat in CATEGORIES.items():
        score = sum(1 for kw in cat["whitelist"] if kw in t)
        if score > 0:
            scores[key] = score
    return max(scores, key=scores.get) if scores else None

# ==========================================
# NEWS FETCHING
# ==========================================

def fetch_google_news(query, max_items=6):
    try:
        encoded = requests.utils.quote(query)
        url     = (f"https://news.google.com/rss/search"
                   f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en")
        r = requests.get(url, timeout=10,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        root  = ET.fromstring(r.content)
        items = root.findall(".//item")[:max_items]
        results = []
        for item in items:
            title   = item.findtext("title", "").strip()
            source  = item.findtext("source", "Google News").strip()
            pubdate = item.findtext("pubDate", "")[:16]
            if title:
                results.append({
                    "headline": title,
                    "source":   source,
                    "date":     pubdate,
                    "origin":   "google_news"
                })
        return results
    except Exception as e:
        print(f"  Google News [{query[:30]}]: {e}")
        return []

def fetch_pib(max_items=10):
    try:
        url = "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3"
        r   = requests.get(url, timeout=10,
                           headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        root  = ET.fromstring(r.content)
        items = root.findall(".//item")[:max_items]
        results = []
        for item in items:
            title = item.findtext("title", "").strip()
            if title:
                results.append({
                    "headline": title,
                    "source":   "PIB — Government of India",
                    "date":     item.findtext("pubDate", "")[:16],
                    "origin":   "pib"
                })
        return results
    except Exception as e:
        print(f"  PIB: {e}")
        return []

def collect_headlines_for_categories(categories):
    """Pull headlines for specific categories only."""
    print(f"SCOUT: Collecting headlines for {categories}...")
    all_headlines = []

    for cat_key in categories:
        if cat_key not in CATEGORIES:
            continue
        cat = CATEGORIES[cat_key]
        print(f"  [{cat_key}] fetching...")
        for q in cat["queries"][:3]:
            all_headlines += fetch_google_news(q, max_items=4)
            time.sleep(0.2)

    # Always add PIB
    all_headlines += fetch_pib(max_items=15)

    # Deduplicate
    seen, unique = set(), []
    for h in all_headlines:
        key = h["headline"].lower().strip()
        if key not in seen and len(h["headline"]) > 20:
            seen.add(key)
            unique.append(h)

    print(f"SCOUT: {len(unique)} unique headlines")
    return unique

# ==========================================
# TOPIC EXTRACTION
# ==========================================

def extract_topics_from_headlines(headlines, categories):
    if not headlines:
        return []

    cat_descriptions = "\n".join(
        f"- {k}: {CATEGORIES[k]['label']} — {CATEGORIES[k]['description']}"
        for k in categories if k in CATEGORIES
    )

    headlines_text = "\n".join(
        f"{i+1}. {h['headline']} ({h['source']})"
        for i, h in enumerate(headlines[:30])
    )

    prompt = f"""You are the topic director for India20Sixty — Indian YouTube Shorts about India's real near future.

ALLOWED CATEGORIES:
{cat_descriptions}

REAL HEADLINES:
{headlines_text}

Extract compelling video topics from these headlines.
Each topic MUST:
- Come from a real headline above
- Fit one of the allowed categories
- Have a specific fact, number, or stat from the headline
- Make a viewer think "I didn't know this was already happening"
- Be about tech, innovation, or India's future — NOT sports/politics/entertainment

Generate 8-12 topics.

Return ONLY valid JSON:
[
  {{
    "topic": "compelling video angle",
    "category": "AI|Space|Gadgets|DeepTech|GreenTech|Startups",
    "source_headline": "exact headline",
    "source_name": "source",
    "key_fact": "specific number/stat/fact",
    "story_angle": "why this matters to Indians"
  }}
]"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.7, "max_tokens": 1500},
            timeout=30
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        start   = content.find('[')
        end     = content.rfind(']') + 1
        topics  = json.loads(content[start:end])
        print(f"EXTRACT: {len(topics)} topics")
        return topics
    except Exception as e:
        print(f"Topic extraction failed: {e}")
        return []

# ==========================================
# LANGUAGE EXPERT
# Fixes pronunciation before sending to ElevenLabs
# ==========================================

def language_expert_review(script, topic):
    """
    Language expert pass:
    1. Fixes ElevenLabs language misdetection (German/Dutch issue)
    2. Adds pronunciation hints for Indian words
    3. Adds emotion tags <happy>, <sad>, <excited>, <whisper> where suitable
    4. Ensures text reads naturally for an English voice
    """
    prompt = f"""You are a language expert and voice coach for an Indian YouTube Shorts channel.

Your job: review this script before it goes to a text-to-speech voice engine (ElevenLabs).

KNOWN ISSUES TO FIX:
1. ElevenLabs eleven_multilingual_v2 sometimes reads English as German/Dutch
   - Fix: spell out acronyms with dots (ISRO → I.S.R.O., AI → A.I. is NOT needed, keep AI)
   - Fix: add hyphens to Indian words for syllabification (Chandrayaan → Chandra-yaan)
   - Fix: replace ₹ symbol with words (₹3,000 crore → 3 thousand crore rupees)
   - Fix: replace % with "percent" (95% → 95 percent)
   - Fix: write numbers in words for large figures (1,50,000 → 1 lakh 50 thousand)

2. PRONUNCIATION HINTS for common Indian tech words:
   - ISRO → I.S.R.O.
   - DRDO → D.R.D.O.
   - IIT → I.I.T.
   - UPI → U.P.I.
   - EV → E.V.
   - 5G → 5-G
   - Gaganyaan → Gagan-yaan
   - Chandrayaan → Chandra-yaan
   - crore → keep as-is (ElevenLabs handles well)
   - lakh → keep as-is

3. ADD EMOTION TAGS where suitable — use these exact tags:
   - <excited> ... </excited> — for breakthrough moments, proud facts
   - <happy> ... </happy> — for positive outcomes, hope
   - <sad> ... </sad> — for problems, challenges, what's missing
   - <whisper> ... </whisper> — for dramatic reveals, secrets
   Note: Only use 1-2 emotion tags per script. Don't overuse.

4. LANGUAGE CLARITY:
   - Ensure script reads as English with 2-3 Hindi words — not German
   - If any sentence looks like it could confuse a language detector, simplify it
   - Keep Hindi words like "yaar", "dekho", "lekin" — these are fine

Topic: {topic}

Original script:
{script}

Return the corrected script ONLY — no explanation, no labels, just the fixed text.
Keep the same meaning and length. Just fix pronunciation and add max 2 emotion tags."""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 400},
            timeout=20
        )
        r.raise_for_status()
        reviewed = r.json()["choices"][0]["message"]["content"].strip()
        print(f"  Language expert: {reviewed[:100]}...")
        return reviewed
    except Exception as e:
        print(f"  Language expert failed: {e}")
        # Basic fallback fixes
        script = script.replace("₹", "").replace("%", " percent")
        script = script.replace("ISRO", "I.S.R.O.").replace("DRDO", "D.R.D.O.")
        return script

# ==========================================
# TOPIC COUNCIL
# ==========================================

def council_evaluate(topic_data, perf_context=None):
    topic       = topic_data.get("topic", "")
    key_fact    = topic_data.get("key_fact", "")
    story_angle = topic_data.get("story_angle", "")
    source      = topic_data.get("source_name", "")
    category    = topic_data.get("category", "")

    perf_info = ""
    if perf_context and perf_context.get("total_videos"):
        tops = perf_context.get("top_performers", [])
        perf_info = f"\nTop performing topics: {', '.join(t.get('topic','')[:25] for t in tops[:2])}"

    prompt = f"""You are the INDIA20SIXTY TOPIC COUNCIL.

Evaluate for a 25-second YouTube Short.
Topic: "{topic}"
Category: {category}
Key fact: "{key_fact}"
Source: "{source}"
{perf_info}

Score 0-100:
1. VIRALITY — will Indians share this?
2. FACTUAL_STRENGTH — is the anchor specific and credible?
3. VISUAL_POTENTIAL — can AI generate compelling images?
4. EMOTIONAL_HOOK — pride, wonder, urgency?
5. SAFETY — platform safe, no controversy?

Return ONLY JSON:
{{
  "virality":         {{"score": 85}},
  "factual_strength": {{"score": 90}},
  "visual_potential": {{"score": 80}},
  "emotional_hook":   {{"score": 85}},
  "safety":           {{"score": 95}},
  "council_score":    87,
  "recommendation":   "APPROVE",
  "improved_topic":   "better angle if applicable",
  "hook_suggestion":  "suggested opening line"
}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 400},
            timeout=30
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        start   = content.find('{')
        end     = content.rfind('}') + 1
        ev      = json.loads(content[start:end])

        score    = ev.get("council_score", 0)
        approved = (
            ev.get("recommendation") == "APPROVE"
            and score >= 70
            and ev.get("safety", {}).get("score", 0) >= 80
        )

        return {
            "topic":          ev.get("improved_topic", topic),
            "original_topic": topic,
            "category":       category,
            "council_score":  score,
            "approved":       approved,
            "evaluation":     ev,
            "fact_package": {
                "found":     True,
                "headline":  topic_data.get("source_headline", ""),
                "source":    source,
                "key_fact":  key_fact,
                "relevance": story_angle
            }
        }
    except Exception as e:
        print(f"Council failed for '{topic[:40]}': {e}")
        return {"topic": topic, "category": category,
                "council_score": 0, "approved": False,
                "evaluation": {}, "fact_package": {"found": False}}

# ==========================================
# SCRIPT ARCHITECT
# ==========================================

def architect_script(topic, council_result):
    fact_pkg     = council_result.get("fact_package", {})
    key_fact     = fact_pkg.get("key_fact", "")
    source       = fact_pkg.get("source", "")
    hook_sug     = council_result.get("evaluation", {}).get("hook_suggestion", "")
    category     = council_result.get("category", "")

    fact_section = f"""
REAL FACT ANCHOR:
Fact: {key_fact}
Source: {source}
Hook suggestion: {hook_sug}""" if key_fact else ""

    prompt = f"""Write a 25-second voiceover for India20Sixty — Indian YouTube Shorts.

Topic: {topic}
Category: {category}
{fact_section}

STRICT LENGTH: Maximum 55 words. Count every word. Stop at 55.

LANGUAGE: Mostly English. Use 2-3 natural Hindi/Urdu words only.
Good ones: yaar, dekho, lekin, bas, toh, soch lo, wahi, abhi

STRUCTURE — 6 punchy sentences:
1. Hook with real fact
2. What is happening right now
3. Scale — numbers, money, reach
4. What this means for Indians
5. The challenge or twist
6. Debate question

THEN: Language expert will clean pronunciation and add emotion tags.

Return ONLY valid JSON:
{{
  "title": "SEO title under 60 chars with emoji",
  "hook": "3-second scroll stopper",
  "script_lines": ["line1","line2","line3","line4","line5","line6"],
  "full_script": "all lines as one paragraph",
  "cta": "comment call to action",
  "hashtags": ["tag1","tag2","tag3","tag4","tag5"],
  "estimated_duration_sec": 25
}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.85, "max_tokens": 600},
            timeout=30
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        start   = content.find('{')
        end     = content.rfind('}') + 1
        pkg     = json.loads(content[start:end])

        # Language expert pass on the script
        if pkg.get("full_script"):
            pkg["full_script"] = language_expert_review(pkg["full_script"], topic)

        return pkg
    except Exception as e:
        print(f"Script architect failed: {e}")
        fallback_script = f"Dekho — {key_fact or topic}. This is already happening in India. The scale is massive. Yaar, this changes everything for regular Indians. But there's one challenge nobody is talking about. What do you think — are we ready?"
        return {
            "title":                f"🚀 {topic[:50]}",
            "hook":                 f"Dekho — {key_fact or topic}",
            "script_lines":         [fallback_script],
            "full_script":          language_expert_review(fallback_script, topic),
            "cta":                  "Comment below! 👇",
            "hashtags":             ["IndiaFuture", "FutureTech", "India", "Shorts"],
            "estimated_duration_sec": 25
        }

# ==========================================
# SUPABASE
# ==========================================

def sb_get(endpoint):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers={"apikey": SUPABASE_KEY,
                 "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=10
    )
    r.raise_for_status()
    return r.json()

def sb_insert(table, data):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={"apikey": SUPABASE_KEY,
                 "Authorization": f"Bearer {SUPABASE_KEY}",
                 "Content-Type": "application/json",
                 "Prefer": "return=representation"},
        json=data, timeout=10
    )
    r.raise_for_status()
    return r.json()

def sb_patch(endpoint, data):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers={"apikey": SUPABASE_KEY,
                 "Authorization": f"Bearer {SUPABASE_KEY}",
                 "Content-Type": "application/json",
                 "Prefer": "return=minimal"},
        json=data, timeout=10
    )
    return r.ok

def get_queue_depth(categories=None):
    try:
        endpoint = "topics?used=eq.false&council_score=gte.70&select=id,cluster"
        rows     = sb_get(endpoint)
        if categories:
            rows = [r for r in rows if r.get("cluster") in categories]
        return len(rows)
    except Exception:
        return 0

def get_performance_context():
    try:
        rows = sb_get("system_state?id=eq.main&select=council_context")
        if rows and rows[0].get("council_context"):
            return json.loads(rows[0]["council_context"])
    except Exception:
        pass
    return {}

def save_topic(topic, council_score, category, script_package, fact_package):
    data = {
        "cluster":        category,
        "topic":          topic,
        "used":           False,
        "council_score":  council_score,
        "script_package": {**script_package, "fact_anchor": fact_package},
        "source":         "real_news_v3",
    }
    try:
        result = sb_insert("topics", data)
        print(f"  Saved [{category}]: {topic[:50]} (score {council_score})")
        return result[0] if result else None
    except Exception as e:
        print(f"  Save failed: {e}")
        return None

# ==========================================
# REPLENISHMENT PIPELINE
# ==========================================

def run_replenishment(target=12, categories=None):
    categories = categories or ALL_CATEGORY_KEYS
    print(f"\n{'='*60}")
    print(f"REPLENISHMENT v3.0 | categories: {categories} | target: {target}")
    print(f"{'='*60}\n")

    perf_context = get_performance_context()

    # Phase 1: Headlines
    headlines = collect_headlines_for_categories(categories)
    if not headlines:
        return []

    # Phase 2: Topic extraction
    raw_topics = extract_topics_from_headlines(headlines, categories)
    if not raw_topics:
        return []

    # Phase 3: Filter + Council
    approved = []
    print(f"\nCOUNCIL: Evaluating {len(raw_topics)} topics...")
    for i, t in enumerate(raw_topics):
        topic_str = t.get("topic", "")
        headline  = t.get("source_headline", "")

        if is_blacklisted(topic_str) or is_blacklisted(headline):
            print(f"  [{i+1}] BLACKLISTED: {topic_str[:50]}")
            continue

        if not is_whitelisted(topic_str, t.get("category")) and \
           not is_whitelisted(headline):
            print(f"  [{i+1}] NOT RELEVANT: {topic_str[:50]}")
            continue

        # Auto-detect category if missing
        if not t.get("category") or t["category"] not in CATEGORIES:
            detected = detect_category(topic_str + " " + headline)
            t["category"] = detected or "AI"

        result = council_evaluate(t, perf_context)
        score  = result["council_score"]
        status = "APPROVED" if result["approved"] else "REJECTED"
        print(f"  [{i+1}] {status} [{t['category']}] score={score} | {topic_str[:45]}")
        if result["approved"]:
            approved.append({**t, **result})
        time.sleep(0.4)

    print(f"\nCOUNCIL: {len(approved)}/{len(raw_topics)} approved")

    # Phase 4: Script + Language expert + Save
    saved = []
    for result in approved[:target]:
        topic_str     = result["topic"]
        council_score = result["council_score"]
        category      = result.get("category", "AI")
        fact_pkg      = result.get("fact_package", {})

        print(f"\nARCHITECT [{category}]: {topic_str[:50]}...")
        script_pkg = architect_script(topic_str, result)
        print(f"  Title: {script_pkg.get('title','')[:55]}")

        record = save_topic(topic_str, council_score, category,
                            script_pkg, fact_pkg)
        if record:
            saved.append(record)
        time.sleep(0.4)

    print(f"\nREPLENISHMENT COMPLETE: {len(saved)} topics saved")
    return saved

# ==========================================
# FLASK ROUTES
# ==========================================

@app.route("/health")
def health():
    depth = get_queue_depth()
    cat_depths = {}
    try:
        rows = sb_get("topics?used=eq.false&council_score=gte.70&select=cluster")
        for key in ALL_CATEGORY_KEYS:
            cat_depths[key] = sum(1 for r in rows if r.get("cluster") == key)
    except Exception:
        pass
    return jsonify({
        "status":       "topic-council-worker v3.0",
        "queue_depth":  depth,
        "by_category":  cat_depths,
        "needs_refill": depth < 5,
        "categories":   {k: v["label"] for k, v in CATEGORIES.items()}
    })


@app.route("/queue-status")
def queue_status():
    try:
        rows = sb_get(
            "topics?used=eq.false&order=council_score.desc"
            "&select=id,topic,council_score,cluster,source"
        )
        by_cat = {k: [] for k in ALL_CATEGORY_KEYS}
        for r in rows:
            cat = r.get("cluster", "AI")
            if cat in by_cat:
                by_cat[cat].append(r)
        return jsonify({
            "total":       len(rows),
            "by_category": {k: len(v) for k, v in by_cat.items()},
            "topics":      rows[:10]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/replenish", methods=["POST"])
def replenish():
    data       = request.json or {}
    target     = data.get("target", 12)
    categories = data.get("categories", ALL_CATEGORY_KEYS)

    # Validate categories
    categories = [c for c in categories if c in CATEGORIES]
    if not categories:
        categories = ALL_CATEGORY_KEYS

    depth = get_queue_depth(categories)
    print(f"REPLENISH: queue={depth} | categories={categories} | target={target}")

    if depth >= target:
        return jsonify({
            "status":  "queue_sufficient",
            "depth":   depth,
            "message": f"Already have {depth} topics"
        })

    needed = max(target - depth, 5)
    saved  = run_replenishment(target=needed, categories=categories)

    return jsonify({
        "status":      "replenished",
        "added":       len(saved),
        "categories":  categories,
        "queue_depth": get_queue_depth(),
        "topics":      [s.get("topic", "") for s in saved if s]
    })


@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():
    data     = request.json or {}
    topic    = data.get("topic", "Future of AI in India")
    source   = data.get("source", "manual")
    category = data.get("category")

    # Try to find a real headline
    headlines  = fetch_google_news(f"{topic} India", max_items=5)
    topic_data = None

    if headlines:
        raw = extract_topics_from_headlines(headlines,
              [category] if category else ALL_CATEGORY_KEYS)
        if raw:
            topic_data = next(
                (t for t in raw if topic.lower()[:20] in t.get("topic","").lower()),
                raw[0]
            )

    if not topic_data:
        topic_data = {
            "topic":           topic,
            "category":        category or detect_category(topic) or "AI",
            "source_headline": "",
            "source_name":     "manual",
            "key_fact":        "",
            "story_angle":     "manually submitted"
        }

    if is_blacklisted(topic):
        return jsonify({"status": "rejected", "reason": "blacklisted", "topic": topic})

    perf_context = get_performance_context()
    result       = council_evaluate(topic_data, perf_context)

    if result["approved"]:
        script_pkg = architect_script(result["topic"], result)
        result["script"] = script_pkg
        if source == "manual":
            save_topic(result["topic"], result["council_score"],
                       result.get("category", "AI"),
                       script_pkg, result.get("fact_package", {"found": False}))

    return jsonify({
        "status":     "approved" if result["approved"] else "rejected",
        "topic":      result["topic"],
        "category":   result.get("category"),
        "evaluation": result.get("evaluation", {}),
        "script":     result.get("script"),
        "fact_found": result.get("fact_package", {}).get("found", False)
    })


@app.route("/")
def home():
    return jsonify({"status": "topic-council-worker v3.0"})


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "replenish":
        cats   = sys.argv[2].split(",") if len(sys.argv) > 2 else ALL_CATEGORY_KEYS
        target = int(sys.argv[3]) if len(sys.argv) > 3 else 12
        results = run_replenishment(target=target, categories=cats)
        print(f"\nDone: {len(results)} topics added")
    else:
        port = int(os.environ.get("PORT", 10001))
        app.run(host="0.0.0.0", port=port, threaded=True)
