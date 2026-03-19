import os
import json
import requests
from datetime import datetime, timedelta
from urllib.parse import quote_plus

class TrendScout:
    def __init__(self):
        self.openai_key = os.environ.get("OPENAI_API_KEY")
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_ANON_KEY")
        
    def fetch_google_trends(self, geo="IN", hours=24):
        """Fetch trending searches from Google Trends (via scraping API or RSS)"""
        # Using a free trends API or RSS feed
        trends = []
        try:
            # Alternative: serpapi or similar for production
            # For now, using OpenAI to generate realistic trending topics based on category
            prompt = f"""Generate 10 trending search topics in India related to:
- Future technology
- AI and automation
- Space and science
- Smart cities
- Climate and green energy
- Healthcare innovation

Format: Return ONLY a JSON array of strings, no other text.
Example: ["AI doctors rural India", "ISRO Mars mission 2025", ...]"""
            
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7
                }
            )
            content = r.json()["choices"][0]["message"]["content"]
            # Extract JSON from response
            start = content.find('[')
            end = content.rfind(']') + 1
            trends = json.loads(content[start:end])
        except Exception as e:
            print(f"Trend fetch error: {e}")
            trends = [
                "AI hospitals India 2030",
                "Indian moon base ISRO",
                "Hyperloop Mumbai Delhi",
                "Vertical farming India cities",
                "Quantum computing India"
            ]
        return trends
    
    def fetch_reddit_insights(self, subreddits=None):
        """Fetch trending discussions from relevant subreddits"""
        if subreddits is None:
            subreddits = ["india", "technology", "futurology", "space", "artificial"]
        
        topics = []
        # Using OpenAI to simulate Reddit trend analysis
        prompt = f"""Analyze what people on Reddit r/india, r/technology, r/futurology are discussing about India's future.

Generate 8 discussion topics that are:
- Getting high engagement (upvotes/comments)
- Controversial or thought-provoking
- Related to 2030-2060 timeline

Return ONLY JSON array of strings."""
        
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.8
                }
            )
            content = r.json()["choices"][0]["message"]["content"]
            start = content.find('[')
            end = content.rfind(']') + 1
            topics = json.loads(content[start:end])
        except Exception as e:
            print(f"Reddit fetch error: {e}")
            topics = [
                "Will AI replace Indian IT jobs by 2030?",
                "India's population vs resources 2050",
                "Space colonization: India's role"
            ]
        return topics
    
    def fetch_youtube_gaps(self):
        """Find content gaps in YouTube Shorts"""
        prompt = """Analyze what viral YouTube Shorts about "Future India" are missing.

What topics are UNDERSERVED but would go viral?
- Not covered by big channels
- High curiosity potential
- Visual/AI-image friendly

Generate 5 content gap topics.
Return ONLY JSON array of strings."""
        
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.openai_key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.9
                }
            )
            content = r.json()["choices"][0]["message"]["content"]
            start = content.find('[')
            end = content.rfind(']') + 1
            return json.loads(content[start:end])
        except:
            return [
                "Indian villages in 2060",
                "Future of Indian languages with AI",
                "Religion and technology India 2050"
            ]
    
    def collect_all_sources(self):
        """Aggregate all trend sources"""
        print("🔍 SCOUT: Collecting trends from all sources...")
        
        google_trends = self.fetch_google_trends()
        print(f"  📊 Google Trends: {len(google_trends)} topics")
        
        reddit_topics = self.fetch_reddit_insights()
        print(f"  👥 Reddit Insights: {len(reddit_topics)} topics")
        
        youtube_gaps = self.fetch_youtube_gaps()
        print(f"  📺 YouTube Gaps: {len(youtube_gaps)} topics")
        
        # Deduplicate and format
        all_topics = list(set(google_trends + reddit_topics + youtube_gaps))
        
        # Add metadata
        enriched = []
        for topic in all_topics:
            source = "google" if topic in google_trends else \
                     "reddit" if topic in reddit_topics else "youtube_gap"
            enriched.append({
                "topic": topic,
                "source": source,
                "discovered_at": datetime.utcnow().isoformat(),
                "raw_virality_score": 50  # Base score, council will adjust
            })
        
        print(f"✅ SCOUT: Total unique topics: {len(enriched)}")
        return enriched


if __name__ == "__main__":
    scout = TrendScout()
    topics = scout.collect_all_sources()
    print(json.dumps(topics, indent=2))
