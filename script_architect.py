import os
import json
import requests

class ScriptArchitect:
    """
    Creates viral Shorts scripts from approved topics.
    Optimized for retention, shares, and algorithm.
    """
    
    def __init__(self):
        self.openai_key = os.environ.get("OPENAI_API_KEY")
        
        self.viral_hooks = [
            "Pattern Interrupt: Start with impossible claim",
            "Curiosity Gap: They know something you don't",
            "Identity Challenge: 'If you're Indian...'",
            "Time Pressure: 'By 2030, this will be gone...'",
            "Social Proof: 'ISRO just confirmed...'",
            "Contrarian: 'Everyone is wrong about...'"
        ]
        
        self.script_blueprints = {
            "shock_and_explain": {
                "structure": ["HOOK (shocking)", "CONTEXT", "MECHANISM", "IMPLICATION", "CTA"],
                "duration": "25-30 sec",
                "best_for": "technology, predictions"
            },
            "imagine_future": {
                "structure": ["VISUAL HOOK", "PRESENT PROBLEM", "FUTURE SOLUTION", "TIMELINE", "QUESTION"],
                "duration": "20-25 sec",
                "best_for": "infrastructure, cities"
            },
            "hidden_truth": {
                "structure": ["SECRET REVEAL", "PROOF", "CONSEQUENCE", "ACTION", "DEBATE"],
                "duration": "30-35 sec",
                "best_for": "policy, economics"
            }
        }
    
    def select_blueprint(self, topic, council_eval):
        """Choose best script structure for topic"""
        
        topic_lower = topic.lower()
        
        if any(word in topic_lower for word in ["ai", "robot", "automation", "tech"]):
            return "shock_and_explain"
        elif any(word in topic_lower for word in ["city", "infrastructure", "transport", "building"]):
            return "imagine_future"
        else:
            return "hidden_truth"
    
    def generate_script(self, approved_topic: dict) -> dict:
        """Generate complete script package"""
        
        topic = approved_topic["final_topic"]
        blueprint_name = self.select_blueprint(topic, approved_topic["council_evaluation"])
        blueprint = self.script_blueprints[blueprint_name]
        
        prompt = f"""Create a VIRAL YouTube Shorts script for India20Sixty.

TOPIC: {topic}
BLUEPRINT: {blueprint_name}
STRUCTURE: {' → '.join(blueprint['structure'])}
TARGET DURATION: {blueprint['duration']}

RULES:
- FIRST 3 SECONDS must stop the scroll (use pattern interrupt)
- Every line under 8 words
- Include 1 shocking statistic or prediction
- End with question that sparks comments
- Use Hinglish where natural (appeals to Indian youth)
- No generic phrases like "Imagine a world where..."

FORMAT:
TITLE: (SEO-optimized, under 60 chars, include emoji)
HOOK: (3 seconds, scroll-stopper)
SCRIPT:
(line 1)
(line 2)
...
(line n)
CTA: (call to comment/share)
HASHTAGS: (5-7 tags)

VISUAL DIRECTIONS:
Scene 1: (for image generation)
Scene 2: 
Scene 3:
Scene 4:
Scene 5:

Return as JSON:
{{
  "title": "...",
  "hook": "...",
  "script_lines": ["...", "..."],
  "full_script": "...",
  "cta": "...",
  "hashtags": ["...", "..."],
  "visual_scenes": ["...", "..."],
  "estimated_duration_sec": 25,
  "viral_elements": ["...", "..."]
}}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_key}"},
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
            script_package = json.loads(content[start:end])
            
            return {
                **approved_topic,
                "script_package": script_package,
                "blueprint_used": blueprint_name,
                "ready_for_production": True
            }
            
        except Exception as e:
            print(f"Script generation failed: {e}")
            # Fallback script
            return {
                **approved_topic,
                "script_package": {
                    "title": f"{topic} 🇮🇳",
                    "hook": f"What if {topic} became reality tomorrow?",
                    "script_lines": [f"Socho agar {topic} sach ho jaye.", "Ye future door nahi.", "India 2060 mein yeh normal hoga.", "Aapko kya lagta?"],
                    "full_script": f"What if {topic} became reality tomorrow? This future isn't far. By 2060, this will be normal in India. What do you think?",
                    "cta": "Comment your thoughts! 👇",
                    "hashtags": ["India2060", "FutureTech", "India", "Shorts", "AI"],
                    "visual_scenes": ["futuristic Indian city", "technology in action", "people using tech", "wide cityscape", "Indian flag with tech"],
                    "estimated_duration_sec": 20
                },
                "blueprint_used": "fallback",
                "ready_for_production": True
            }
    
    def generate_batch(self, approved_topics: list) -> list:
        """Generate scripts for all approved topics"""
        
        print(f"\n🎬 SCRIPT ARCHITECT: Creating {len(approved_topics)} scripts...")
        print("=" * 60)
        
        scripts = []
        for i, topic in enumerate(approved_topics, 1):
            print(f"\n  [{i}/{len(approved_topics)}] Architecting: {topic['final_topic'][:40]}...")
            script = self.generate_script(topic)
            scripts.append(script)
            print(f"      ✅ Blueprint: {script['blueprint_used']}")
            print(f"      📝 Duration: {script['script_package']['estimated_duration_sec']}s")
        
        return scripts


# ==========================================
# INTEGRATION: Full Pipeline
# ==========================================

class TopicPipeline:
    """Complete pipeline from trends to production-ready scripts"""
    
    def __init__(self):
        self.scout = TrendScout()
        self.council = TopicCouncil()
        self.architect = ScriptArchitect()
    
    def run_full_pipeline(self, target_count=5):
        """
        1. Scout trends from internet
        2. Council judges and filters
        3. Architect writes scripts
        4. Save to Supabase topics table
        """
        print("=" * 70)
        print("🚀 INDIA20SIXTY TOPIC PIPELINE")
        print("=" * 70)
        
        # Phase 1: Discovery
        raw_topics = self.scout.collect_all_sources()
        
        # Phase 2: Judgement
        approved_topics = self.council.batch_council(raw_topics)
        
        if len(approved_topics) < target_count:
            print(f"⚠️ Only {len(approved_topics)} approved, need {target_count}")
            # Could trigger more scouting here
        
        # Phase 3: Creation
        production_scripts = self.architect.generate_batch(approved_topics[:target_count])
        
        # Phase 4: Save to database
        saved = self.save_to_supabase(production_scripts)
        
        print(f"\n" + "=" * 70)
        print(f"✅ PIPELINE COMPLETE: {len(saved)} topics ready for production")
        print("=" * 70)
        
        return saved
    
    def save_to_supabase(self, scripts):
        """Save approved scripts to Supabase topics table"""
        
        import requests
        
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_ANON_KEY")
        
        saved = []
        for script in scripts:
            # Extract cluster from evaluation or default
            cluster = "AI_Future"  # Could be smarter
            
            topic_data = {
                "cluster": cluster,
                "topic": script["final_topic"],
                "used": False,
                "council_score": script["council_score"],
                "script_package": script["script_package"],
                "blueprint": script["blueprint_used"],
                "source": script["source"]
            }
            
            try:
                r = requests.post(
                    f"{supabase_url}/rest/v1/topics",
                    headers={
                        "apikey": supabase_key,
                        "Authorization": f"Bearer {supabase_key}",
                        "Content-Type": "application/json",
                        "Prefer": "return=representation"
                    },
                    json=topic_data,
                    timeout=10
                )
                if r.status_code == 201:
                    saved.append(r.json()[0])
                    print(f"  💾 Saved: {script['final_topic'][:40]}...")
                else:
                    print(f"  ❌ Save failed: {r.text}")
            except Exception as e:
                print(f"  ❌ Save error: {e}")
        
        return saved


if __name__ == "__main__":
    pipeline = TopicPipeline()
    results = pipeline.run_full_pipeline(target_count=5)
    
    # Print summary
    print(f"\n📋 PRODUCTION QUEUE:")
    for r in results:
        pkg = r.get("script_package", {})
        print(f"\n  🎬 {pkg.get('title', 'Untitled')}")
        print(f"     Hook: {pkg.get('hook', 'N/A')[:50]}...")
        print(f"     Duration: {pkg.get('estimated_duration_sec', 0)}s")
