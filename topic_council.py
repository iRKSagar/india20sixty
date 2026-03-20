import os
import json
import requests
from typing import List, Dict


class TopicCouncil:
    """
    AI Council with performance feedback loop.
    Analytics from YouTube train the council to approve
    topics that match patterns of high-performing videos.
    """

    def __init__(self):
        self.openai_key  = os.environ.get("OPENAI_API_KEY")
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_ANON_KEY")

        self.channel_philosophy = """
INDIA20SIXTY CHANNEL PHILOSOPHY:
- Optimistic but realistic vision of India's future (2030-2060)
- Bridges tradition with cutting-edge technology
- Appeals to Indian youth (18-34) and diaspora
- Educational but entertaining (edutainment)
- Visually stunning, shareable, conversation-starting
- Never political, never religious controversy
- Celebrates Indian innovation while acknowledging challenges
"""

    # --------------------------------------------------
    # PERFORMANCE CONTEXT — reads from Supabase
    # --------------------------------------------------

    def get_performance_context(self) -> dict:
        """Fetch council_context from system_state table."""
        if not self.supabase_url or not self.supabase_key:
            return {}

        try:
            r = requests.get(
                f"{self.supabase_url}/rest/v1/system_state?id=eq.main&select=council_context",
                headers={
                    "apikey": self.supabase_key,
                    "Authorization": f"Bearer {self.supabase_key}"
                },
                timeout=5
            )
            rows = r.json()
            if rows and rows[0].get("council_context"):
                return json.loads(rows[0]["council_context"])
        except Exception as e:
            print(f"Performance context fetch failed: {e}")

        return {}

    def build_performance_prompt(self, context: dict) -> str:
        """Convert analytics context into a prompt section."""
        if not context or not context.get("total_videos"):
            return ""

        avg   = context.get("avg_score", 0)
        total = context.get("total_videos", 0)
        tops  = context.get("top_performers", [])
        flops = context.get("worst_performers", [])

        lines = [
            "\n── CHANNEL PERFORMANCE DATA (use this to calibrate your scores) ──",
            f"Total videos published: {total}",
            f"Average virality score: {avg:,}  (views×1 + likes×50 + comments×30)",
        ]

        if tops:
            lines.append("\nTOP PERFORMING videos — identify what made these work:")
            for t in tops:
                lines.append(f"  • \"{t.get('topic','?')}\"  →  score {t.get('analytics_score',0):,}  (council approved at {t.get('council_score',0)})")

        if flops:
            lines.append("\nWORST PERFORMING videos — avoid these patterns:")
            for t in flops:
                lines.append(f"  • \"{t.get('topic','?')}\"  →  score {t.get('analytics_score',0):,}  (council approved at {t.get('council_score',0)})")

        lines += [
            "\nUse this data to:",
            "1. Boost virality score for topics matching patterns of top performers",
            "2. Penalise topics matching patterns of worst performers",
            "3. Calibrate your council_score so approved topics are LIKELY to beat the average",
            "── END PERFORMANCE DATA ──\n"
        ]

        return "\n".join(lines)

    # --------------------------------------------------
    # TOPIC EVALUATION
    # --------------------------------------------------

    def judge_topic(self, topic: Dict) -> Dict:
        """Single topic evaluation by the Council."""

        perf_context = self.get_performance_context()
        perf_prompt  = self.build_performance_prompt(perf_context)

        prompt = f"""You are the INDIA20SIXTY TOPIC COUNCIL. Evaluate this topic for a 25-second YouTube Short.

TOPIC: "{topic['topic']}"
SOURCE: {topic['source']}

CHANNEL PHILOSOPHY:
{self.channel_philosophy}
{perf_prompt}
Evaluate on these 5 dimensions (score 0-100):

1. VIRALITY QUOTIENT
   - Curiosity gap, shareability, comment potential
   - Score: __/100  Reason: ___

2. YOUTUBE SHORTS FIT
   - 3-second hook potential, retention, algorithm keywords
   - Score: __/100  Reason: ___

3. INSTAGRAM REELS FIT
   - Visual stopping power, sound-off appeal
   - Score: __/100  Reason: ___

4. PLATFORM SAFETY
   - YouTube + Instagram compliance (100 = perfectly safe)
   - Score: __/100  Flags: ___

5. VISUAL PRODUCTION EASE
   - Can Leonardo AI generate compelling images for this?
   - Score: __/100  Reason: ___

OVERALL COUNCIL SCORE: weighted average
RECOMMENDATION: APPROVE (>=70) / REJECT (<70) / REVISE

Return ONLY valid JSON:
{{
  "virality":      {{"score": 85, "reason": "..."}},
  "youtube_fit":   {{"score": 90, "reason": "..."}},
  "instagram_fit": {{"score": 75, "reason": "..."}},
  "safety":        {{"score": 95, "flags": "none"}},
  "visual_ease":   {{"score": 80, "reason": "..."}},
  "council_score": 85,
  "recommendation": "APPROVE",
  "revision_suggestion": null,
  "improved_title": "better version if applicable",
  "performance_prediction": "expected to score above/below channel average and why"
}}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3
                },
                timeout=30
            )

            content = r.json()["choices"][0]["message"]["content"]
            start = content.find('{')
            end   = content.rfind('}') + 1
            evaluation = json.loads(content[start:end])

            return {
                **topic,
                "council_evaluation": evaluation,
                "council_score":  evaluation.get("council_score", 0),
                "council_status": evaluation.get("recommendation", "REJECT"),
                "final_topic":    evaluation.get("improved_title", topic["topic"]),
                "performance_prediction": evaluation.get("performance_prediction", "")
            }

        except Exception as e:
            print(f"Council error for '{topic['topic']}': {e}")
            return {
                **topic,
                "council_evaluation": {"error": str(e)},
                "council_score":  0,
                "council_status": "REJECT"
            }

    # --------------------------------------------------
    # COUNCIL SESSION
    # --------------------------------------------------

    def council_session(self, topics: List[Dict], min_score=70) -> List[Dict]:
        """Run full council session — logs performance context being used."""

        ctx = self.get_performance_context()
        if ctx.get("total_videos"):
            print(f"\n  Using performance data: {ctx['total_videos']} videos, avg score {ctx.get('avg_score', 0):,}")
        else:
            print("\n  No performance data yet — using baseline evaluation")

        print(f"\nCOUNCIL SESSION: Evaluating {len(topics)} topics...")
        print("=" * 60)

        evaluated = []
        for i, topic in enumerate(topics, 1):
            print(f"\n  [{i}/{len(topics)}] Judging: {topic['topic'][:50]}...")
            result = self.judge_topic(topic)
            score  = result["council_score"]
            status = result["council_status"]
            pred   = result.get("performance_prediction", "")

            emoji = "✅" if status == "APPROVE" else "⚠️" if status == "REVISE" else "❌"
            print(f"      {emoji} Score: {score}/100 | Verdict: {status}")
            if pred:
                print(f"         Prediction: {pred[:80]}")

            evaluated.append(result)

        evaluated.sort(key=lambda x: x["council_score"], reverse=True)
        approved = [t for t in evaluated if t["council_status"] == "APPROVE" and t["council_score"] >= min_score]

        print(f"\n{'=' * 60}")
        print(f"COUNCIL RESULTS: {len(approved)}/{len(evaluated)} approved")
        if approved:
            print(f"Top topic: {approved[0]['final_topic']}")

        return approved


# --------------------------------------------------
# FLASK APP
# --------------------------------------------------

from flask import Flask, request, jsonify

app = Flask(__name__)
council = TopicCouncil()


@app.route("/full-pipeline", methods=["POST"])
def full_pipeline():
    data  = request.json or {}
    topic = data.get("topic", "Future of AI in India")
    source = data.get("source", "api")

    print(f"\nCOUNCIL REQUEST: '{topic}' from {source}")

    result = council.judge_topic({"topic": topic, "source": source})

    status = "approved" if result["council_status"] == "APPROVE" else "rejected"

    return jsonify({
        "status":     status,
        "topic":      result["final_topic"],
        "evaluation": result["council_evaluation"],
        "script":     result.get("script_package"),
        "source":     source
    })


@app.route("/health")
def health():
    ctx = council.get_performance_context()
    return jsonify({
        "status":         "topic-council-worker running",
        "has_perf_data":  bool(ctx.get("total_videos")),
        "total_videos":   ctx.get("total_videos", 0),
        "avg_score":      ctx.get("avg_score", 0)
    })


@app.route("/")
def home():
    return jsonify({"status": "topic-council-worker running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10001))
    app.run(host="0.0.0.0", port=port)
