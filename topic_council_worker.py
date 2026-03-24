"""
India20Sixty — Topic Council Worker v2.0
=========================================
Real-news-first pipeline. Every topic is anchored to an actual
headline from Google News RSS or PIB before entering the queue.

Endpoints:
  GET  /health          — status + queue depth
  POST /replenish       — full scout→council→save pipeline
  POST /full-pipeline   — evaluate single topic (called by Cloudflare)
  GET  /queue-status    — how many approved topics remain
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

CHANNEL_BRIEF = """
INDIA20SIXTY — India's future through technology and innovation.

IDENTITY: Every video covers something real happening in India right now —
a funded startup, a government program, a scientific breakthrough, a new gadget,
a policy that changes how Indians will live in the next 5-10 years.

ALLOWED TOPICS — must fit one of these:
- AI, machine learning, robotics, automation
- Space tech, ISRO missions, satellites, defence tech
- Healthcare innovation, medical AI, biotech, pharma
- EVs, renewable energy, solar, clean tech, batteries
- Gadgets, consumer electronics, chips, semiconductors
- Smart cities, infrastructure, 5G, connectivity
- Agritech, food technology, precision farming
- Edtech, skill development, digital literacy
- Startups, deep tech, R&D, IIT/IIM innovations
- Fintech, UPI, digital payments, blockchain applications
- Manufacturing tech, Industry 4.0, Make in India

HARD RULES:
- Every topic must have ONE real fact anchor — a number, a funding amount, a timeline, a stat
- No sports, no entertainment, no politics, no religion
- No vague "India is growing" topics — must be specific and real
- Timeframe: next 5-10 years — never "by 2060"
"""

# ==========================================
# REAL NEWS SOURCES
# ==========================================

WHITELIST_KEYWORDS = [
    # AI & Machine Learning
    "ai", "artificial intelligence", "machine learning", "deep learning",
    "neural", "llm", "generative", "chatbot", "automation", "robot",
    # Space & Defence Tech
    "isro", "space", "satellite", "rocket", "chandrayaan", "gaganyaan",
    "drdo", "defence technology", "missile", "aerospace",
    # Healthcare & Biotech
    "health", "medical", "hospital", "doctor", "cancer", "diagnosis",
    "biotech", "pharma", "drug", "vaccine", "genomics", "telemedicine",
    # Energy & Climate Tech
    "solar", "renewable", "energy", "wind power", "green", "ev", "electric vehicle",
    "battery", "hydrogen", "climate", "emission", "clean energy",
    # Infrastructure & Smart Cities
    "smart city", "infrastructure", "metro", "highway", "bullet train",
    "hyperloop", "5g", "6g", "broadband", "internet", "connectivity",
    # Gadgets & Consumer Tech
    "gadget", "device", "smartphone", "chip", "semiconductor", "processor",
    "wearable", "drone", "iot", "sensor", "display", "camera tech",
    # Agriculture & Food Tech
    "agri", "farm", "crop", "irrigation", "food tech", "precision farming",
    "vertical farm", "hydroponics", "seed", "fertilizer tech",
    # Education & Skill Tech
    "edtech", "education technology", "skill", "learning", "coding",
    "online education", "upskilling", "digital literacy",
    # Startup & Innovation Ecosystem
    "startup", "unicorn", "funding", "venture", "innovation", "incubator",
    "deep tech", "r&d", "research", "iit", "iim", "patent",
    # Manufacturing & Industry
    "manufacturing", "factory", "3d print", "semiconductor fab",
    "make in india", "industry 4.0", "supply chain tech",
    # Fintech
    "fintech", "upi", "digital payment", "blockchain", "crypto tech",
    "neobank", "insurtech", "digital rupee",
]

BLACKLIST_KEYWORDS = [
    # Sports — all of them
    "cricket", "hockey", "football", "soccer", "tennis", "badminton",
    "kabaddi", "basketball", "volleyball", "wrestling", "boxing",
    "olympic", "commonwealth games", "cwg", "asian games", "ipl",
    "bcci", "fifa", "icc", "match", "tournament", "trophy", "league",
    "player", "batsman", "bowler", "goalkeeper", "wicket", "century",
    # Entertainment
    "bollywood", "movie", "film", "actor", "actress", "celebrity",
    "singer", "musician", "album", "song", "concert", "award show",
    "oscar", "filmfare", "box office", "ott release", "web series",
    # Politics
    "election", "vote", "polling", "political party", "minister",
    "parliament", "lok sabha", "rajya sabha", "cm ", "chief minister",
    "governor", "mla", "mp ", "manifesto", "rally",
    # Religion & Culture (non-tech)
    "temple", "mosque", "church", "gurudwara", "diwali", "eid",
    "holi", "navratri", "puja", "pilgrimage", "yatra",
    # Crime & Disasters
    "murder", "rape", "crime", "theft", "fraud", "scam", "arrested",
    "flood", "earthquake", "cyclone", "landslide", "accident", "crash",
    # Pure Finance (no tech angle)
    "sensex", "nifty", "stock market", "share price", "ipo listing",
    "inflation rate", "gdp growth", "rbi rate", "repo rate",
    # Celebrity/Influencer
    "viral video", "meme", "influencer", "youtuber", "instagram reel",
    "tiktok", "reels trend",
]

def is_allowed_topic(topic_text, headline_text=""):
    """Whitelist check — must match tech/future/innovation."""
    combined = (topic_text + " " + headline_text).lower()
    return any(kw in combined for kw in WHITELIST_KEYWORDS)

def is_banned_topic(topic_text, headline_text=""):
    """Blacklist check — hard block regardless of whitelist."""
    combined = (topic_text + " " + headline_text).lower()
    return any(kw in combined for kw in BLACKLIST_KEYWORDS)
    """Google News RSS — free, no API key, real headlines."""
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
        print(f"Google News [{query[:30]}]: {e}")
        return []


def fetch_pib(max_items=10):
    """PIB — official Indian government press releases, free RSS."""
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
        print(f"PIB fetch: {e}")
        return []


def fetch_isro_news(max_items=5):
    """ISRO news via Google News RSS."""
    return fetch_google_news("ISRO India space 2025", max_items)


def collect_all_headlines():
    """Pull headlines from all sources and deduplicate."""
    print("SCOUT: Collecting real headlines...")

    # Tech and innovation focused queries only
    queries = [
        "India AI artificial intelligence startup 2025",
        "ISRO India space technology mission",
        "India electric vehicle EV technology",
        "India renewable energy solar startup",
        "India healthcare medical AI technology",
        "India semiconductor chip manufacturing",
        "India fintech UPI digital payment innovation",
        "India drone robotics automation startup",
        "India 5G smart city infrastructure",
        "India deep tech startup funding 2025",
    ]

    all_headlines = []

    # Google News — parallel queries
    for q in queries[:5]:  # limit to 5 to stay fast
        all_headlines += fetch_google_news(q, max_items=5)
        time.sleep(0.3)

    # PIB official releases
    all_headlines += fetch_pib(max_items=15)

    # ISRO specific
    all_headlines += fetch_isro_news(max_items=5)

    # Deduplicate by headline text
    seen, unique = set(), []
    for h in all_headlines:
        key = h["headline"].lower().strip()
        if key not in seen and len(h["headline"]) > 20:
            seen.add(key)
            unique.append(h)

    print(f"SCOUT: {len(unique)} unique headlines collected")
    return unique


# ==========================================
# TOPIC EXTRACTION FROM HEADLINES
# ==========================================

def extract_topics_from_headlines(headlines):
    """
    Use GPT to turn real headlines into India20Sixty video topics.
    Each topic is anchored to a real headline.
    """
    if not headlines:
        return []

    headlines_text = "\n".join(
        f"{i+1}. [{h['origin']}] {h['headline']} — {h['source']}"
        for i, h in enumerate(headlines[:30])
    )

    prompt = f"""You are the topic director for India20Sixty, an Indian YouTube Shorts channel about India's real near future.

CHANNEL BRIEF:
{CHANNEL_BRIEF}

REAL HEADLINES FROM TODAY:
{headlines_text}

Your job: Turn these real headlines into compelling YouTube Short topics.

Rules:
- Each topic MUST be derived from a real headline above
- Topic should be the STORY ANGLE, not just a headline rewrite
- Make it emotionally compelling — what does this mean for regular Indians?
- Include the real anchor (stat, source, fact) from the headline
- Avoid politics, religion, or controversy
- Generate 8-12 topics from the most interesting headlines

Return ONLY valid JSON array:
[
  {{
    "topic": "compelling video title/topic",
    "source_headline": "exact headline from the list",
    "source_name": "source organization",
    "key_fact": "the specific number, stat, or fact from the headline",
    "story_angle": "what makes this emotionally compelling for Indian viewers",
    "category": "Space|AI|Healthcare|Infrastructure|Energy|Education|Economy"
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
        print(f"EXTRACT: {len(topics)} topics from headlines")
        return topics
    except Exception as e:
        print(f"Topic extraction failed: {e}")
        return []


# ==========================================
# TOPIC COUNCIL
# ==========================================

def council_evaluate(topic_data, perf_context=None):
    """
    Score a topic on virality, factual strength, visual potential.
    Returns score + recommendation + improved angle.
    """
    topic        = topic_data.get("topic", "")
    key_fact     = topic_data.get("key_fact", "")
    story_angle  = topic_data.get("story_angle", "")
    source       = topic_data.get("source_name", "")

    perf_prompt = ""
    if perf_context and perf_context.get("total_videos"):
        avg  = perf_context.get("avg_score", 0)
        tops = perf_context.get("top_performers", [])
        perf_prompt = f"\nChannel avg score: {avg:,} | "
        if tops:
            perf_prompt += f"Top topics: {', '.join(t.get('topic','')[:30] for t in tops[:2])}"

    prompt = f"""You are the INDIA20SIXTY TOPIC COUNCIL.

Evaluate this real-news-anchored topic for a 25-second YouTube Short.

TOPIC: "{topic}"
KEY FACT: "{key_fact}"
SOURCE: "{source}"
STORY ANGLE: "{story_angle}"

CHANNEL BRIEF:
{CHANNEL_BRIEF}
{perf_prompt}

Score 0-100 on each dimension:
1. VIRALITY — Will Indians share this? Does the fact surprise them?
2. FACTUAL_STRENGTH — Is the anchor fact specific and credible?
3. VISUAL_POTENTIAL — Can AI generate compelling images for this?
4. EMOTIONAL_HOOK — Does this create pride, wonder, or urgency?
5. SAFETY — Platform-safe, no politics/religion controversy?

Return ONLY valid JSON:
{{
  "virality":          {{"score": 85, "reason": "..."}},
  "factual_strength":  {{"score": 90, "reason": "..."}},
  "visual_potential":  {{"score": 80, "reason": "..."}},
  "emotional_hook":    {{"score": 85, "reason": "..."}},
  "safety":            {{"score": 95, "flags": "none"}},
  "council_score":     87,
  "recommendation":    "APPROVE",
  "improved_topic":    "better angle if applicable, else same",
  "hook_suggestion":   "suggested opening line for the script"
}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 500},
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
            "topic":         ev.get("improved_topic", topic),
            "original_topic": topic,
            "council_score": score,
            "approved":      approved,
            "evaluation":    ev,
            "fact_package": {
                "found":     True,
                "headline":  topic_data.get("source_headline", ""),
                "source":    source,
                "key_fact":  key_fact,
                "relevance": story_angle
            }
        }
    except Exception as e:
        print(f"Council eval failed for '{topic[:40]}': {e}")
        return {
            "topic":         topic,
            "council_score": 0,
            "approved":      False,
            "evaluation":    {},
            "fact_package":  {"found": False}
        }


# ==========================================
# SCRIPT ARCHITECT (pre-generates script package)
# ==========================================

BLUEPRINTS = {
    "real_news":      ["SHOCKING FACT HOOK", "CONTEXT", "WHAT THIS MEANS", "NEAR FUTURE", "YOUR VERDICT"],
    "already_happening": ["REVEAL", "PROOF", "SCALE", "IMPACT", "DEBATE"],
    "india_leads":    ["BOLD CLAIM", "EVIDENCE", "COMPARISON", "PRIDE MOMENT", "QUESTION"],
}

def architect_script(topic, council_result):
    """
    Pre-generate a script package using the fact anchor.
    Saved with the topic so pipeline can use it directly.
    """
    fact_pkg = council_result.get("fact_package", {})
    key_fact = fact_pkg.get("key_fact", "")
    source   = fact_pkg.get("source", "")
    hook_sug = council_result.get("evaluation", {}).get("hook_suggestion", "")

    fact_section = f"""
REAL FACT ANCHOR — MUST be used in the script:
Fact: {key_fact}
Source: {source}
Hook suggestion: {hook_sug}""" if key_fact else ""

    prompt = f"""You are a passionate Indian storyteller for India20Sixty YouTube Shorts.

Topic: {topic}
{fact_section}

Write a 25-second flowing narration — NOT bullet points.
Sound like an excited young Indian talking to friends.

Rules:
- Open with the real fact to shock and hook
- Build the story around real information, not invented claims  
- 70% English + 30% Hinglish naturally mixed
- Use "..." for dramatic pauses
- Do NOT say "by 2060" — say "already happening", "in 5 years", "by 2030"
- 8 flowing sentences that build on each other
- End with a debate-sparking question

Return ONLY valid JSON:
{{
  "title": "SEO title under 60 chars with emoji",
  "hook": "first 3 seconds — the scroll-stopper",
  "script_lines": ["line1", "line2", "line3", "line4", "line5", "line6", "line7", "line8"],
  "full_script": "all lines as one flowing paragraph",
  "cta": "comment/share call to action",
  "hashtags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
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
        return pkg
    except Exception as e:
        print(f"Script architect failed: {e}")
        return {
            "title":                f"{topic} 🇮🇳",
            "hook":                 f"Yeh suno — {key_fact or topic}",
            "script_lines":         [f"Yeh suno — {topic}.",
                                     "Yeh already ho raha hai India mein.",
                                     "Sirf 5 saalon mein sab kuch badal jayega.",
                                     key_fact or "Real change aa rahi hai.",
                                     "Duniya dekh rahi hai, India lead kar raha hai.",
                                     "Yeh sirf shuruaat hai.",
                                     "Hamare engineers aur scientists iss par kaam kar rahe hain.",
                                     "Kya aap ready hain? Comment karo."],
            "full_script":          f"Yeh suno — {topic}. Already ho raha hai. {key_fact}",
            "cta":                  "Comment karo! 👇",
            "hashtags":             ["IndiaFuture", "FutureTech", "India", "Shorts"],
            "estimated_duration_sec": 25
        }


# ==========================================
# SAVE TO SUPABASE
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


def get_queue_depth():
    try:
        rows = sb_get("topics?used=eq.false&council_score=gte.70&select=id")
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


def save_topic(topic, source_data, council_score, script_package, fact_package):
    data = {
        "cluster":        fact_package.get("category", "AI_Future"),
        "topic":          topic,
        "used":           False,
        "council_score":  council_score,
        "script_package": {
            **script_package,
            "fact_anchor": fact_package
        },
        "source":         "real_news_v2",
    }
    try:
        result = sb_insert("topics", data)
        print(f"  Saved: {topic[:50]} (score {council_score})")
        return result[0] if result else None
    except Exception as e:
        print(f"  Save failed: {e}")
        return None


# ==========================================
# FULL REPLENISHMENT PIPELINE
# ==========================================

def run_replenishment(target=12):
    print(f"\n{'='*60}")
    print(f"REPLENISHMENT v2.0 — real news first — target: {target}")
    print(f"{'='*60}\n")

    perf_context = get_performance_context()
    if perf_context.get("total_videos"):
        print(f"Performance context: {perf_context['total_videos']} videos, "
              f"avg score {perf_context.get('avg_score',0):,}")

    # Phase 1: Collect real headlines
    headlines = collect_all_headlines()
    if not headlines:
        print("No headlines collected — aborting")
        return []

    # Phase 2: Extract topics from headlines
    raw_topics = extract_topics_from_headlines(headlines)
    if not raw_topics:
        print("No topics extracted — aborting")
        return []

    print(f"\nCOUNCIL: Evaluating {len(raw_topics)} real-news topics...")

    # Phase 3: Whitelist + Blacklist filter + Council evaluation
    approved = []
    for i, t in enumerate(raw_topics):
        topic_str   = t.get("topic", "")
        headline    = t.get("source_headline", "")

        # Blacklist — hard block first
        if is_banned_topic(topic_str, headline):
            print(f"  [{i+1}] BLACKLISTED: {topic_str[:55]}")
            continue

        # Whitelist — must match allowed categories
        if not is_allowed_topic(topic_str, headline):
            print(f"  [{i+1}] NOT RELEVANT: {topic_str[:55]}")
            continue
        topic_str = t.get("topic", "")
        print(f"  [{i+1}/{len(raw_topics)}] {topic_str[:55]}...")
        result = council_evaluate(t, perf_context)
        score  = result["council_score"]
        status = "APPROVED" if result["approved"] else "REJECTED"
        print(f"    {status} | score={score} | fact={t.get('key_fact','')[:40]}")
        if result["approved"]:
            approved.append({**t, **result})
        time.sleep(0.5)

    print(f"\nCOUNCIL: {len(approved)}/{len(raw_topics)} approved")

    # Phase 4: Generate script packages + save
    saved = []
    for result in approved[:target]:
        topic_str    = result["topic"]
        council_score = result["council_score"]
        fact_pkg     = result.get("fact_package", {})
        fact_pkg["category"] = result.get("category", "AI_Future")

        print(f"\nARCHITECT: {topic_str[:55]}...")
        script_pkg = architect_script(topic_str, result)
        print(f"  Title: {script_pkg.get('title','')[:55]}")
        print(f"  Hook:  {script_pkg.get('hook','')[:60]}")

        record = save_topic(
            topic_str,
            result.get("source_headline", ""),
            council_score,
            script_pkg,
            fact_pkg
        )
        if record:
            saved.append(record)
        time.sleep(0.5)

    print(f"\nREPLENISHMENT COMPLETE: {len(saved)} fact-anchored topics saved")
    return saved


# ==========================================
# FLASK ROUTES
# ==========================================

@app.route("/health")
def health():
    depth = get_queue_depth()
    return jsonify({
        "status":       "topic-council-worker v2.0 running",
        "version":      "2.0-real-news",
        "queue_depth":  depth,
        "needs_refill": depth < 5,
        "sources":      ["google_news_rss", "pib_gov_in"]
    })


@app.route("/queue-status")
def queue_status():
    try:
        rows = sb_get(
            "topics?used=eq.false&order=council_score.desc"
            "&select=id,topic,council_score,source"
        )
        return jsonify({
            "approved_topics": len(rows),
            "topics":          rows[:10]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/replenish", methods=["POST"])
def replenish():
    data   = request.json or {}
    target = data.get("target", 12)

    depth = get_queue_depth()
    print(f"REPLENISH called. Queue: {depth} | Target: {target}")

    if depth >= target:
        return jsonify({
            "status":  "queue_sufficient",
            "message": f"Already have {depth} topics",
            "depth":   depth
        })

    needed = max(target - depth, 5)
    saved  = run_replenishment(target=needed)

    return jsonify({
        "status":      "replenished",
        "added":       len(saved),
        "queue_depth": get_queue_depth(),
        "topics":      [s.get("topic", "") for s in saved if s]
    })


@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():
    """Called by Cloudflare worker for on-demand topic evaluation."""
    data   = request.json or {}
    topic  = data.get("topic", "Future of AI in India")
    source = data.get("source", "manual")

    # For manual topics, try to find a real headline first
    headlines = fetch_google_news(f"{topic} India", max_items=5)

    if headlines:
        raw_topics = extract_topics_from_headlines(headlines)
        # Find closest match or use the first one
        topic_data = next(
            (t for t in raw_topics
             if topic.lower()[:20] in t.get("topic","").lower()),
            raw_topics[0] if raw_topics else None
        )
    else:
        topic_data = None

    if not topic_data:
        # No real headline found — evaluate as-is with lower factual score
        topic_data = {
            "topic":           topic,
            "source_headline": "",
            "source_name":     "manual",
            "key_fact":        "",
            "story_angle":     "manually submitted topic",
            "category":        "AI_Future"
        }

    perf_context = get_performance_context()
    result       = council_evaluate(topic_data, perf_context)

    if result["approved"]:
        script_pkg = architect_script(result["topic"], result)
        result["script"] = script_pkg

        if source == "manual":
            save_topic(
                result["topic"],
                topic_data.get("source_headline", ""),
                result["council_score"],
                script_pkg,
                result.get("fact_package", {"found": False})
            )

    return jsonify({
        "status":     "approved" if result["approved"] else "rejected",
        "topic":      result["topic"],
        "evaluation": result.get("evaluation", {}),
        "script":     result.get("script"),
        "fact_found": result.get("fact_package", {}).get("found", False),
        "source":     source
    })


@app.route("/")
def home():
    return jsonify({"status": "topic-council-worker v2.0 running"})


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "replenish":
        target  = int(sys.argv[2]) if len(sys.argv) > 2 else 12
        results = run_replenishment(target=target)
        print(f"\nDone: {len(results)} topics added")
    else:
        port = int(os.environ.get("PORT", 10001))
        app.run(host="0.0.0.0", port=port, threaded=True)