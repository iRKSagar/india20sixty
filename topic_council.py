import os
import json
import requests
from typing import List, Dict

class TopicCouncil:
    """
    AI Council that evaluates topics through multiple lenses:
    1. Virality Quotient (will it spread?)
    2. Platform Compliance (YouTube/Instagram safe?)
    3. Channel Philosophy (fits India20Sixty?)
    4. Visual Potential (can we illustrate it?)
    """
    
    def __init__(self):
        self.openai_key = os.environ.get("OPENAI_API_KEY")
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
    
    def judge_topic(self, topic: Dict) -> Dict:
        """Single topic evaluation by the Council"""
        
        prompt = f"""You are the INDIA20SIXTY TOPIC COUNCIL. Evaluate this topic:

TOPIC: "{topic['topic']}"
SOURCE: {topic['source']}

CHANNEL PHILOSOPHY:
{self.channel_philosophy}

Evaluate on these 5 dimensions (score 0-100, explain briefly):

1. VIRALITY QUOTIENT
   - Curiosity gap strength
   - Shareability (would someone send this to friends?)
   - Comment potential (would people argue/agree?)
   - Score: __/100
   - Reason: ___

2. YOUTUBE SHORTS FIT
   - 3-second hook potential
   - Retention prediction (will they watch to end?)
   - Algorithm-friendly (trending keywords?)
   - Score: __/100
   - Reason: ___

3. INSTAGRAM REELS FIT
   - Visual stopping power
   - Sound-on vs sound-off appeal
   - Share to story potential
   - Score: __/100
   - Reason: ___

4. PLATFORM SAFETY
   - YouTube Community Guidelines risk (0=safe, 100=dangerous)
   - Instagram policy compliance
   - Brand safety for future sponsors
   - Score: __/100 (higher = safer)
   - Flags: ___

5. VISUAL PRODUCTION EASE
   - Can Leonardo AI generate compelling images?
   - Ken Burns motion potential
   - Cinematic quality achievable
   - Score: __/100
   - Reason: ___

OVERALL COUNCIL SCORE: Average of above (0-100)
RECOMMENDATION: APPROVE / REJECT / REVISE
SUGGESTED_REVISION: (if REVISE, how to improve)

Return ONLY valid JSON:
{{
  "virality": {{"score": 85, "reason": "..."}},
  "youtube_fit": {{"score": 90, "reason": "..."}},
  "instagram_fit": {{"score": 75, "reason": "..."}},
  "safety": {{"score": 95, "flags": "none"}},
  "visual_ease": {{"score": 80, "reason": "..."}},
  "council_score": 85,
  "recommendation": "APPROVE",
  "revision_suggestion": null,
  "improved_title": "better version if applicable"
}}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3  # Lower for consistent scoring
                },
                timeout=30
            )
            
            content = r.json()["choices"][0]["message"]["content"]
            
            # Extract JSON
            start = content.find('{')
            end = content.rfind('}') + 1
            evaluation = json.loads(content[start:end])
            
            # Merge with original topic
            return {
                **topic,
                "council_evaluation": evaluation,
                "council_score": evaluation.get("council_score", 0),
                "council_status": evaluation.get("recommendation", "REJECT"),
                "final_topic": evaluation.get("improved_title", topic["topic"])
            }
            
        except Exception as e:
            print(f"Council error for '{topic['topic']}': {e}")
            return {
                **topic,
                "council_evaluation": {"error": str(e)},
                "council_score": 0,
                "council_status": "REJECT"
            }
    
    def council_session(self, topics: List[Dict], min_score=70) -> List[Dict]:
        """Run full council session on all topics"""
        
        print(f"\n🏛️ COUNCIL SESSION: Evaluating {len(topics)} topics...")
        print("=" * 60)
        
        evaluated = []
        for i, topic in enumerate(topics, 1):
            print(f"\n  [{i}/{len(topics)}] Judging: {topic['topic'][:50]}...")
            result = self.judge_topic(topic)
            score = result["council_score"]
            status = result["council_status"]
            
            emoji = "✅" if status == "APPROVE" else "⚠️" if status == "REVISE" else "❌"
            print(f"      {emoji} Score: {score}/100 | Verdict: {status}")
            
            evaluated.append(result)
        
        # Sort by score
        evaluated.sort(key=lambda x: x["council_score"], reverse=True)
        
        # Filter approved only
        approved = [t for t in evaluated if t["council_status"] == "APPROVE" and t["council_score"] >= min_score]
        
        print(f"\n" + "=" * 60)
        print(f"COUNCIL RESULTS:")
        print(f"  Total evaluated: {len(evaluated)}")
        print(f"  Approved (>{min_score}): {len(approved)}")
        print(f"  Top topic: {approved[0]['final_topic'] if approved else 'None'}")
        
        return approved
    
    def batch_council(self, topics: List[Dict]) -> List[Dict]:
        """Optimized batch evaluation (cheaper API calls)"""
        
        # For cost efficiency, batch 5 topics at once
        batches = [topics[i:i+5] for i in range(0, len(topics), 5)]
        all_approved = []
        
        for batch in batches:
            batch_approved = self.council_session(batch)
            all_approved.extend(batch_approved)
        
        # Re-sort all approved
        all_approved.sort(key=lambda x: x["council_score"], reverse=True)
        return all_approved[:10]  # Top 10 only


if __name__ == "__main__":
    # Test
    test_topics = [
        {"topic": "AI replacing doctors in India", "source": "google"},
        {"topic": "Modi government space policy", "source": "reddit"},
        {"topic": "Future of Indian cricket with AI", "source": "youtube_gap"}
    ]
    
    council = TopicCouncil()
    approved = council.council_session(test_topics)
    print(json.dumps(approved, indent=2))
