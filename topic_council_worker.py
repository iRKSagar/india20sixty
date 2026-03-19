from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime

app = Flask(__name__)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

# ==========================================
# COUNCIL EVALUATION ENDPOINT
# ==========================================

@app.route("/evaluate", methods=["POST"])
def evaluate_topic():
    """
    Council evaluates a single topic
    Input: {"topic": "...", "source": "..."}
    Output: Council evaluation with scores
    """
    data = request.json
    topic = data.get("topic")
    source = data.get("source", "manual")
    
    if not topic:
        return jsonify({"error": "topic required"}), 400
    
    evaluation = run_council(topic, source)
    
    return jsonify(evaluation)

@app.route("/batch-evaluate", methods=["POST"])
def batch_evaluate():
    """
    Evaluate multiple topics, return only approved
    Input: {"topics": [{"topic": "...", "source": "..."}, ...]}
    Output: {"approved": [...], "rejected": [...]}
    """
    data = request.json
    topics = data.get("topics", [])
    min_score = data.get("min_score", 70)
    
    approved = []
    rejected = []
    
    for t in topics:
        result = run_council(t.get("topic"), t.get("source", "unknown"))
        if result["council_score"] >= min_score and result["recommendation"] == "APPROVE":
            approved.append(result)
        else:
            rejected.append(result)
    
    # Sort by score
    approved.sort(key=lambda x: x["council_score"], reverse=True)
    
    return jsonify({
        "approved": approved,
        "rejected": rejected,
        "total": len(topics),
        "approved_count": len(approved)
    })

@app.route("/generate-script", methods=["POST"])
def generate_script():
    """
    Generate viral script for approved topic
    Input: {"topic": "...", "council_data": {...}}
    Output: Complete script package
    """
    data = request.json
    topic = data.get("topic")
    
    if not topic:
        return jsonify({"error": "topic required"}), 400
    
    script = architect_script(topic)
    
    return jsonify(script)

@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():
    """
    Complete pipeline: topic → council → script → save to DB
    Input: {"topic": "...", "source": "..."}
    Output: Complete production-ready topic
    """
    data = request.json
    topic = data.get("topic")
    source = data.get("source", "api")
    
    # Step 1: Council evaluation
    council_result = run_council(topic, source)
    
    if council_result["recommendation"] != "APPROVE":
        return jsonify({
            "status": "rejected",
            "reason": "Council rejected",
            "evaluation": council_result
        }), 200
    
    # Step 2: Generate script
    script = architect_script(topic)
    
    # Step 3: Save to Supabase
    saved = save_to_supabase(topic, council_result, script)
    
    return jsonify({
        "status": "approved",
        "topic_id": saved.get("id") if saved else None,
        "evaluation": council_result,
        "script": script
    })

@app.route("/health")
def health():
    return jsonify({"status": "topic council running"})

# ==========================================
# COUNCIL LOGIC
# ==========================================

def run_council(topic, source):
    """AI Council evaluates topic"""
    
    prompt = f"""You are the INDIA20SIXTY TOPIC COUNCIL. Evaluate:

TOPIC: "{topic}"
SOURCE: {source}

Evaluate (score 0-100 each):

1. VIRALITY: Will people share this? Curiosity gap strength?
2. YOUTUBE_FIT: 3-second hook potential, retention prediction
3. INSTAGRAM_FIT: Visual stopping power, shareability
4. SAFETY: Platform compliance (0=dangerous, 100=safe)
5. VISUAL_EASE: Can AI generate compelling images?

Return ONLY JSON:
{{"virality": 85, "youtube_fit": 90, "instagram_fit": 75, "safety": 95, "visual_ease": 80, "council_score": 85, "recommendation": "APPROVE", "improved_title": "better version"}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3
            },
            timeout=30
        )
        
        content = r.json()["choices"][0]["message"]["content"]
        start = content.find('{')
        end = content.rfind('}') + 1
        eval_data = json.loads(content[start:end])
        
        return {
            "topic": topic,
            "source": source,
            **eval_data
        }
        
    except Exception as e:
        return {
            "topic": topic,
            "source": source,
            "council_score": 0,
            "recommendation": "REJECT",
            "error": str(e)
        }

def architect_script(topic):
    """Generate viral script"""
    
    prompt = f"""Create viral YouTube Shorts script for: {topic}

RULES:
- Hook in first 3 seconds (pattern interrupt)
- Short sentences (under 8 words)
- 1 shocking fact/statistic
- End with comment-driving question
- Use Hinglish naturally

Return JSON:
{{"title": "...", "hook": "...", "script_lines": ["..."], "full_script": "...", "cta": "...", "hashtags": ["..."], "visual_scenes": ["..."], "duration_sec": 25}}"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.8
            },
            timeout=30
        )
        
        content = r.json()["choices"][0]["message"]["content"]
        start = content.find('{')
        end = content.rfind('}') + 1
        return json.loads(content[start:end])
        
    except Exception as e:
        return {
            "title": f"{topic} 🇮🇳",
            "hook": f"What if {topic} became reality?",
            "script_lines": [f"Socho agar {topic} sach ho jaye.", "Future is closer than you think."],
            "full_script": f"What if {topic} became reality? This future isn't far.",
            "cta": "Comment your thoughts! 👇",
            "hashtags": ["India2060", "FutureTech", "India"],
            "visual_scenes": ["futuristic Indian city", "technology in action"],
            "duration_sec": 20,
            "error": str(e)
        }

def save_to_supabase(topic, council, script):
    """Save approved topic to database"""
    
    try:
        data = {
            "cluster": "AI_Future",  # Could be smarter
            "topic": council.get("improved_title", topic),
            "used": False,
            "council_score": council["council_score"],
            "script_package": script,
            "source": council["source"]
        }
        
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/topics",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            },
            json=data,
            timeout=10
        )
        
        if r.status_code == 201:
            return r.json()[0]
        return None
        
    except Exception as e:
        print(f"Save error: {e}")
        return None

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
