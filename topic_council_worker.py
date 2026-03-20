"""
India20Sixty — Topic Council Worker
====================================
Unified Flask service that runs on Render as `topic-council-worker`.

Endpoints:
  GET  /health              — status + queue depth
  POST /full-pipeline       — evaluate single topic (called by Cloudflare worker)
  POST /replenish           — run full scout→council→architect→save pipeline
  GET  /queue-status        — how many approved topics remain

Cron: Cloudflare worker should call /replenish daily when queue < 5 topics.
"""

from flask import Flask, request, jsonify
import os
import json
import requests
import re
import time
from datetime import datetime

app = Flask(__name__)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SUPABASE_URL   = os.environ.get("SUPABASE_URL")
SUPABASE_KEY   = os.environ.get("SUPABASE_ANON_KEY")

CHANNEL_PHILOSOPHY = """
INDIA20SIXTY CHANNEL PHILOSOPHY:
- Optimistic but realistic vision of India's near future (2030-2045)
- Bridges tradition with cutting-edge technology
- Appeals to Indian youth (18-34) and diaspora
- Educational but entertaining (edutainment)
- Visually stunning, shareable, conversation-starting
- Never political party references, never religious controversy
- Do NOT use "2060" — use "near future", "by 2035", "soon", "within a decade"
- Celebrates Indian innovation while acknowledging challenges
"""

# ==========================================
# SUPABASE HELPERS
# ==========================================

def sb_get(endpoint):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{endpoint}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=10
    )
    r.raise_for_status()
    return r.json()


def sb_insert(table, data):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        },
        json=data, timeout=10
    )
    r.raise_for_status()
    return r.json()


def get_queue_depth():
    try:
        rows = sb_get("topics?used=eq.false&council_score=gte.70&select=id")
        return len(rows)
    except Exception:
        return 0


def get_performance_context():
    """Fetch analytics context from system_state for council calibration."""
    try:
        rows = sb_get("system_state?id=eq.main&select=council_context")
        if rows and rows[0].get("council_context"):
            return json.loads(rows[0]["council_context"])
    except Exception:
        pass
    return {}

# ==========================================
# TREND SCOUT
# ==========================================

def scout_trends():
    """Generate candidate topics from multiple simulated sources."""
    print("SCOUT: Collecting trends...")

    prompts = [
        # Google Trends-style
        """Generate 8 trending search topics in India about future technology, AI, space, smart cities, healthcare.
Return ONLY a JSON array of strings.""",
        # Reddit-style content gaps
        """What future India topics would go viral on YouTube Shorts but are UNDERSERVED?
Topics that are surprising, visual, and thought-provoking.
Return ONLY a JSON array of 6 strings.""",
        # YouTube gap analysis
        """What are the most engaging "Future India" content ideas that no big channel has covered well?
Focus on specific, concrete predictions rather than vague topics.
Return ONLY a JSON array of 5 strings."""
    ]

    all_topics = []
    sources    = ["google_trends", "reddit_gap", "youtube_gap"]

    for prompt, source in zip(prompts, sources):
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.8, "max_tokens": 300
                },
                timeout=30
            )
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
            start   = content.find('[')
            end     = content.rfind(']') + 1
            topics  = json.loads(content[start:end])
            for t in topics:
                all_topics.append({"topic": t, "source": source})
            print(f"  {source}: {len(topics)} topics")
        except Exception as e:
            print(f"  {source} failed: {e}")

    # Deduplicate
    seen   = set()
    unique = []
    for t in all_topics:
        key = t["topic"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(t)

    print(f"SCOUT: {len(unique)} unique topics")
    return unique

# ==========================================
# TOPIC COUNCIL
# ==========================================

def council_evaluate(topic_str, source, perf_context):
    """Evaluate a single topic. Returns dict with score + status."""

    perf_prompt = ""
    if perf_context.get("total_videos"):
        avg   = perf_context.get("avg_score", 0)
        total = perf_context.get("total_videos", 0)
        tops  = perf_context.get("top_performers", [])
        flops = perf_context.get("worst_performers", [])

        perf_prompt = f"\n── CHANNEL PERFORMANCE DATA ──\n"
        perf_prompt += f"Total videos: {total} | Avg score: {avg:,}\n"
        if tops:
            perf_prompt += "TOP performers (model these):\n"
            for t in tops[:3]:
                perf_prompt += f"  • \"{t.get('topic','')}\" → score {t.get('analytics_score',0):,}\n"
        if flops:
            perf_prompt += "WORST performers (avoid these patterns):\n"
            for t in flops[:3]:
                perf_prompt += f"  • \"{t.get('topic','')}\" → score {t.get('analytics_score',0):,}\n"
        perf_prompt += "── END DATA ──\n"

    prompt = f"""You are the INDIA20SIXTY TOPIC COUNCIL. Evaluate this topic for a 25-second YouTube Short.

TOPIC: "{topic_str}"
SOURCE: {source}

CHANNEL PHILOSOPHY:
{CHANNEL_PHILOSOPHY}
{perf_prompt}
Score on 5 dimensions (0-100 each):
1. VIRALITY — curiosity gap, shareability, comment potential
2. YOUTUBE_FIT — hook potential, retention, algorithm keywords
3. INSTAGRAM_FIT — visual stopping power, sound-off appeal
4. SAFETY — platform compliance, brand safety (100=safest)
5. VISUAL_EASE — can AI generate compelling images for this?

Return ONLY valid JSON:
{{
  "virality": {{"score": 85, "reason": "..."}},
  "youtube_fit": {{"score": 90, "reason": "..."}},
  "instagram_fit": {{"score": 75, "reason": "..."}},
  "safety": {{"score": 95, "flags": "none"}},
  "visual_ease": {{"score": 80, "reason": "..."}},
  "council_score": 85,
  "recommendation": "APPROVE",
  "improved_title": "better version if applicable"
}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3, "max_tokens": 400
            },
            timeout=30
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        start   = content.find('{')
        end     = content.rfind('}') + 1
        ev      = json.loads(content[start:end])
        return {
            "topic":          ev.get("improved_title", topic_str),
            "original_topic": topic_str,
            "source":         source,
            "council_score":  ev.get("council_score", 0),
            "approved":       ev.get("recommendation") == "APPROVE" and ev.get("council_score", 0) >= 70,
            "evaluation":     ev
        }
    except Exception as e:
        print(f"  Council eval failed for '{topic_str[:40]}': {e}")
        return {"topic": topic_str, "source": source, "council_score": 0, "approved": False, "evaluation": {}}

# ==========================================
# SCRIPT ARCHITECT
# ==========================================

BLUEPRINTS = {
    "shock_and_explain": ["HOOK", "CONTEXT", "MECHANISM", "IMPLICATION", "CTA"],
    "imagine_future":    ["VISUAL HOOK", "PRESENT PROBLEM", "FUTURE SOLUTION", "TIMELINE", "QUESTION"],
    "hidden_truth":      ["SECRET REVEAL", "PROOF", "CONSEQUENCE", "ACTION", "DEBATE"]
}

def select_blueprint(topic):
    t = topic.lower()
    if any(w in t for w in ["ai", "robot", "automation", "quantum", "tech", "digital"]):
        return "shock_and_explain"
    elif any(w in t for w in ["city", "infrastructure", "transport", "farm", "energy", "solar"]):
        return "imagine_future"
    else:
        return "hidden_truth"


def architect_script(topic_str, council_score):
    """Generate complete script package for approved topic."""
    blueprint_name = select_blueprint(topic_str)
    blueprint      = BLUEPRINTS[blueprint_name]

    prompt = f"""Create a VIRAL YouTube Shorts script for India20Sixty.

TOPIC: {topic_str}
BLUEPRINT: {blueprint_name} — {' → '.join(blueprint)}

RULES:
- First 3 seconds must stop the scroll
- Every line under 10 words
- Include 1 surprising prediction or statistic
- End with question that sparks debate
- 70% English + 30% Hinglish naturally
- Do NOT say "2060" — say "near future", "by 2035", "soon", "within a decade"
- No generic phrases like "Imagine a world where..."

Return ONLY valid JSON:
{{
  "title": "SEO title under 60 chars with emoji",
  "hook": "3-second scroll stopper",
  "script_lines": ["line1", "line2", "line3", "line4", "line5", "line6", "line7", "line8"],
  "full_script": "all lines joined",
  "cta": "comment/share call to action",
  "hashtags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
  "visual_scenes": ["scene1 description", "scene2 description", "scene3 description"],
  "estimated_duration_sec": 25
}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.8, "max_tokens": 600
            },
            timeout=30
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        start   = content.find('{')
        end     = content.rfind('}') + 1
        pkg     = json.loads(content[start:end])
        return pkg
    except Exception as e:
        print(f"  Script architect failed: {e}")
        return {
            "title":                f"{topic_str} 🇮🇳",
            "hook":                 f"Yeh soch lo — {topic_str}.",
            "script_lines":         [f"Yeh soch lo — {topic_str} reality ban raha hai.",
                                     "India ka future ab sirf sapna nahi.",
                                     "Hamare engineers iss par kaam kar rahe hain.",
                                     "Ek dasak mein sab kuch badal jayega.",
                                     "Technology aur tradition ka perfect blend.",
                                     "Duniya dekh rahi hai, hum lead kar rahe hain.",
                                     "Yeh sirf shuruaat hai.",
                                     "Kya aap ready hain? Comment karo!"],
            "full_script":          f"Yeh soch lo — {topic_str} reality ban raha hai. India ka future ab sirf sapna nahi.",
            "cta":                  "Comment karo! 👇",
            "hashtags":             ["IndiaFuture", "FutureTech", "India", "Shorts", "AI"],
            "visual_scenes":        ["futuristic Indian city", "Indian engineers with holograms", "aerial green India"],
            "estimated_duration_sec": 25
        }

# ==========================================
# SAVE TO SUPABASE
# ==========================================

def save_topic(topic_str, source, council_score, script_package):
    """Insert approved topic into topics table."""
    data = {
        "cluster":        "AI_Future",
        "topic":          topic_str,
        "used":           False,
        "council_score":  council_score,
        "script_package": script_package,
        "source":         source
    }
    try:
        result = sb_insert("topics", data)
        print(f"  Saved: {topic_str[:50]}... (score {council_score})")
        return result[0] if result else None
    except Exception as e:
        print(f"  Save failed: {e}")
        return None

# ==========================================
# FULL REPLENISHMENT PIPELINE
# ==========================================

def run_replenishment(target=10):
    """
    Scout → Council → Architect → Save
    Returns list of saved topic records.
    """
    print(f"\n{'='*60}")
    print(f"REPLENISHMENT START — target: {target} topics")
    print(f"{'='*60}\n")

    # Get performance context for council calibration
    perf_context = get_performance_context()
    if perf_context.get("total_videos"):
        print(f"Using performance data: {perf_context['total_videos']} videos, avg score {perf_context.get('avg_score',0):,}")

    # Phase 1: Scout
    raw_topics = scout_trends()

    # Phase 2: Council evaluation
    approved = []
    print(f"\nCOUNCIL: Evaluating {len(raw_topics)} topics...")
    for i, t in enumerate(raw_topics):
        print(f"  [{i+1}/{len(raw_topics)}] {t['topic'][:50]}...")
        result = council_evaluate(t["topic"], t["source"], perf_context)
        score  = result["council_score"]
        status = "APPROVED" if result["approved"] else "REJECTED"
        print(f"    {status} | score={score}")
        if result["approved"]:
            approved.append(result)
        time.sleep(0.5)  # Rate limit

    print(f"\nCOUNCIL: {len(approved)}/{len(raw_topics)} approved")

    # Phase 3: Script architect
    saved = []
    for result in approved[:target]:
        topic_str     = result["topic"]
        council_score = result["council_score"]
        source        = result["source"]

        print(f"\nARCHITECT: {topic_str[:50]}...")
        script_pkg = architect_script(topic_str, council_score)
        print(f"  Blueprint: {select_blueprint(topic_str)} | Duration: {script_pkg.get('estimated_duration_sec',25)}s")

        # Phase 4: Save
        record = save_topic(topic_str, source, council_score, script_pkg)
        if record:
            saved.append(record)

        time.sleep(0.5)

    print(f"\nREPLENISHMENT COMPLETE: {len(saved)} topics saved")
    return saved

# ==========================================
# FLASK ROUTES
# ==========================================

@app.route("/health")
def health():
    depth = get_queue_depth()
    return jsonify({
        "status":       "topic-council-worker running",
        "queue_depth":  depth,
        "needs_refill": depth < 5
    })


@app.route("/queue-status")
def queue_status():
    try:
        rows = sb_get("topics?used=eq.false&order=council_score.desc&select=id,topic,council_score,source")
        return jsonify({"approved_topics": len(rows), "topics": rows[:10]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/replenish", methods=["POST"])
def replenish():
    """
    Run full scout→council→architect→save pipeline.
    Called by Cloudflare worker cron when queue runs low.
    """
    data   = request.json or {}
    target = data.get("target", 10)

    depth = get_queue_depth()
    print(f"REPLENISH called. Current queue: {depth} | Target: {target}")

    if depth >= target:
        return jsonify({
            "status":  "queue_sufficient",
            "message": f"Already have {depth} approved topics",
            "depth":   depth
        })

    needed = target - depth
    saved  = run_replenishment(target=needed)

    return jsonify({
        "status":     "replenished",
        "added":      len(saved),
        "queue_depth": get_queue_depth(),
        "topics":     [s.get("topic", "") for s in saved if s]
    })


@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():
    """
    Evaluate a single topic — called by Cloudflare worker
    when it needs an on-demand topic evaluation.
    """
    data   = request.json or {}
    topic  = data.get("topic", "Future of AI in India")
    source = data.get("source", "manual")

    perf_context = get_performance_context()
    result       = council_evaluate(topic, source, perf_context)

    if result["approved"]:
        # Also generate script package
        script_pkg = architect_script(result["topic"], result["council_score"])
        result["script"] = script_pkg

        # Save to DB if from manual generation
        if source == "manual":
            save_topic(result["topic"], source, result["council_score"], script_pkg)

    return jsonify({
        "status":     "approved" if result["approved"] else "rejected",
        "topic":      result["topic"],
        "evaluation": result.get("evaluation", {}),
        "script":     result.get("script"),
        "source":     source
    })


@app.route("/")
def home():
    return jsonify({"status": "topic-council-worker running"})


if __name__ == "__main__":
    import sys

    # Allow running as a script to manually replenish: python topic_council_worker.py replenish
    if len(sys.argv) > 1 and sys.argv[1] == "replenish":
        target = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        results = run_replenishment(target=target)
        print(f"\nDone: {len(results)} topics added to queue")
    else:
        port = int(os.environ.get("PORT", 10001))
        app.run(host="0.0.0.0", port=port, threaded=True)
