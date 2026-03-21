import modal
import os
import subprocess
import json
import time
import shutil
import uuid
import traceback
import re
import requests
import random
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP DEFINITION
# ==========================================

app = modal.App("india20sixty")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg", "fonts-liberation", "fonts-dejavu-core", "fonts-noto")
    .pip_install("requests", "flask", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR    = "/tmp/india20sixty"
IMG_WIDTH  = 864
IMG_HEIGHT = 1536
OUT_WIDTH  = 1080
OUT_HEIGHT = 1920
FPS        = 25
XFADE_DUR  = 0.5

LEONARDO_MODELS = [
    "aa77f04e-3eec-4034-9c07-d0f619684628",
    "1e60896f-3c26-4296-8ecc-53e2afecc132",
    "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
]

# ==========================================
# EFFECT LIBRARY
# ==========================================

# Color grades — eq + unsharp only (proven safe on Modal debian ffmpeg)
SCENE_GRADES = [
    {"eq": "eq=contrast=1.18:brightness=0.03:saturation=1.35",
     "sharp": "unsharp=5:5:0.8:3:3:0.0", "label": "warm"},
    {"eq": "eq=contrast=1.12:brightness=0.0:saturation=1.1",
     "sharp": "unsharp=3:3:1.0:3:3:0.0", "label": "cool"},
    {"eq": "eq=contrast=1.08:brightness=0.05:saturation=1.45",
     "sharp": "unsharp=5:5:0.6:3:3:0.0", "label": "golden"},
]

# Ken Burns — scale 110%, crop from 3 different positions per scene
KB_SCALE_W = int(OUT_WIDTH  * 1.10)
KB_SCALE_H = int(OUT_HEIGHT * 1.10)
KB_DX      = KB_SCALE_W - OUT_WIDTH
KB_DY      = KB_SCALE_H - OUT_HEIGHT

CROP_POSITIONS = [
    (0,          0,          "top-left"),
    (KB_DX,      KB_DY // 2, "mid-right"),
    (KB_DX // 2, KB_DY,      "bottom-center"),
]

XFADE_TRANSITIONS = ["dissolve", "fade", "wipeleft", "wiperight",
                     "slideleft", "slideright", "fadeblack"]

# Visual variety pools
VISUAL_STYLES = [
    "cinematic ultra-realistic photography, golden hour, warm saffron palette, 8K",
    "dramatic cinematic lighting, deep shadows, vivid neon accents, photorealistic",
    "aerial drone perspective, sweeping wide angle, vibrant saturated colors",
    "close-up editorial photography, shallow depth of field, soft bokeh",
    "epic establishing shot, atmospheric haze and mist, moody cinematic film grain",
    "futuristic neon-lit India, rain-slicked streets, warm orange glow",
    "bright optimistic daylight, clean futuristic architecture, hopeful vibrant",
    "golden sunset silhouettes, dust particles, emotionally powerful cinematic",
    "blue hour twilight, city lights reflecting, serene futuristic, ultra sharp",
    "dramatic overcast sky, god rays breaking through clouds, epic and hopeful",
]

SHOT_TYPES = [
    ["extreme wide establishing shot", "dramatic low angle hero shot",
     "sweeping panoramic", "epic aerial wide shot"],
    ["medium shot close-up detail", "intimate human-scale scene",
     "detailed technological environment", "focused mid-shot with depth"],
    ["soaring aerial overview", "wide hopeful landscape",
     "golden hour wide establishing", "emotional cinematic close-up"],
]

SCENE_TEMPLATES_FALLBACK = [
    "futuristic Indian megacity at golden hour, lotus-shaped towers, electric air taxis, saffron teal palette, cinematic ultra-realistic photography",
    "Indian scientists in smart traditional attire, holographic data displays, temple architecture meets research campus, dramatic cinematic lighting",
    "aerial view transformed green India, solar farms vertical gardens, diverse communities, Indian tricolor, hopeful sunrise, epic cinematic wide shot"
]

# ==========================================
# WEB ENDPOINTS
# ==========================================

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="POST")
def trigger(data: dict):
    job_id      = data.get("job_id") or str(uuid.uuid4())
    topic       = data.get("topic", "Future India")
    webhook_url = data.get("webhook_url", "")
    print(f"Trigger: {job_id} | {topic}")
    run_pipeline.spawn(job_id=job_id, topic=topic, webhook_url=webhook_url)
    return {"status": "started", "job_id": job_id, "topic": topic}


@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="GET")
def health():
    return {
        "status":   "healthy",
        "platform": "modal",
        "version":  "3.0-factual",
        "effects":  ["scale_lanczos", "kb_crop", "eq", "unsharp",
                     "xfade", "captions", "watermark", "loudnorm"],
        "sources":  ["google_news_rss", "pib_rss", "reddit"],
        "out":      f"{OUT_WIDTH}x{OUT_HEIGHT}",
        "memory":   "2GB",
    }


# ==========================================
# MAIN PIPELINE
# ==========================================

@app.function(image=image, secrets=secrets, cpu=2.0, memory=2048, timeout=600)
def run_pipeline(job_id: str, topic: str, webhook_url: str = ""):

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    LEONARDO_API_KEY      = os.environ["LEONARDO_API_KEY"]
    ELEVENLABS_API_KEY    = os.environ["ELEVENLABS_API_KEY"]
    VOICE_ID              = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
    SUPABASE_URL          = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY     = os.environ["SUPABASE_ANON_KEY"]
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    TEST_MODE             = os.environ.get("TEST_MODE", "true").lower() == "true"

    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"TIME:  {datetime.utcnow().isoformat()}")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"{'='*60}\n")

    # ── HELPERS ──────────────────────────────────────────────────

    def update_status(status, data=None):
        try:
            payload = {"status": status, "updated_at": datetime.utcnow().isoformat()}
            if data:
                payload.update(data)
            requests.patch(
                f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json",
                         "Prefer": "return=minimal"},
                json=payload, timeout=10
            )
        except Exception as e:
            print(f"STATUS UPDATE FAILED: {e}")

    def log_to_db(message):
        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/render_logs",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json"},
                json={"job_id": job_id, "message": str(message)[:500]},
                timeout=5
            )
        except Exception:
            pass

    def get_audio_duration(path):
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", path],
                capture_output=True, text=True, timeout=10
            )
            return float(r.stdout.strip())
        except Exception:
            return 27.0

    def run_ffmpeg(cmd, label, timeout=300):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            print(f"  ffmpeg [{label}] FAILED:")
            print(result.stderr[-600:])
            raise Exception(f"{label} failed: {result.stderr[-200:]}")
        return result

    def escape_dt(text):
        text = text.replace('\\', '\\\\')
        text = text.replace("'", "\u2019")
        text = text.replace(':', '\\:')
        text = text.replace('%', '\\%')
        return text

    # ==========================================
    # REAL SOURCE FETCHING
    # Pulls actual news headlines before scripting
    # so every video is anchored to real facts
    # ==========================================

    def fetch_google_news_rss(query):
        """Fetch real headlines from Google News RSS — no API key needed."""
        try:
            encoded = requests.utils.quote(query)
            url     = f"https://news.google.com/rss/search?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
            r       = requests.get(url, timeout=10,
                                   headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root    = ET.fromstring(r.content)
            items   = root.findall(".//item")[:5]
            results = []
            for item in items:
                title  = item.findtext("title", "").strip()
                source = item.findtext("source", "").strip()
                pubdate = item.findtext("pubDate", "").strip()
                if title:
                    results.append({
                        "headline": title,
                        "source":   source or "Google News",
                        "date":     pubdate[:16] if pubdate else ""
                    })
            print(f"  Google News: {len(results)} headlines")
            return results
        except Exception as e:
            print(f"  Google News failed: {e}")
            return []

    def fetch_pib_rss():
        """Fetch official Indian government press releases from PIB."""
        try:
            url = "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3"
            r   = requests.get(url, timeout=10,
                               headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root  = ET.fromstring(r.content)
            items = root.findall(".//item")[:5]
            results = []
            for item in items:
                title = item.findtext("title", "").strip()
                if title:
                    results.append({
                        "headline": title,
                        "source":   "PIB — Press Information Bureau of India",
                        "date":     item.findtext("pubDate", "")[:16]
                    })
            print(f"  PIB: {len(results)} releases")
            return results
        except Exception as e:
            print(f"  PIB failed: {e}")
            return []

    def extract_fact_anchor(topic, headlines):
        """
        Use GPT to extract the most relevant fact + stat
        from real headlines for use in the script.
        """
        if not headlines:
            return None

        headlines_text = "\n".join(
            f"- {h['headline']} ({h['source']})"
            for h in headlines[:8]
        )

        prompt = f"""You are a fact-checker for an Indian YouTube channel about India's future.

Topic we are covering: "{topic}"

Real headlines from today:
{headlines_text}

Find the MOST RELEVANT headline to our topic.
Extract a factual anchor we can use in the script.

Return ONLY valid JSON:
{{
  "headline": "exact headline text",
  "source": "source name",
  "key_fact": "one specific fact, number, or stat from this headline",
  "relevance": "why this is relevant to our topic",
  "found": true
}}

If NO headline is relevant, return: {{"found": false}}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.2, "max_tokens": 300},
                timeout=20
            )
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"].strip()
            start   = content.find('{')
            end     = content.rfind('}') + 1
            data    = json.loads(content[start:end])
            if data.get("found"):
                print(f"  Fact anchor: {data.get('key_fact', '')[:80]}")
                return data
        except Exception as e:
            print(f"  Fact extraction failed: {e}")
        return None

    def research_topic(topic):
        """
        Pull real sources and extract a fact anchor for the script.
        Returns fact_package dict or None if nothing relevant found.
        """
        print("\n[Research]")

        # Search multiple angles
        queries   = [topic, f"{topic} India", f"{topic} 2025 India government"]
        headlines = []
        for q in queries:
            headlines += fetch_google_news_rss(q)
            if len(headlines) >= 8:
                break

        # Also pull PIB for government announcements
        pib_items = fetch_pib_rss()
        headlines += pib_items

        # Deduplicate by headline text
        seen, unique = set(), []
        for h in headlines:
            if h["headline"] not in seen:
                seen.add(h["headline"])
                unique.append(h)

        print(f"  Total unique headlines: {len(unique)}")

        if not unique:
            print("  No headlines found — proceeding without fact anchor")
            return None

        return extract_fact_anchor(topic, unique)

    # ── SCRIPT ───────────────────────────────────────────────────

    def generate_script(fact_package=None):
        print("SCRIPT START")

        # Build fact anchor section for prompt
        if fact_package and fact_package.get("found"):
            fact_section = f"""
REAL FACT ANCHOR — you MUST use this in the script:
Headline: {fact_package['headline']}
Source: {fact_package['source']}
Key fact: {fact_package['key_fact']}

Ground your story in this real fact. Do not contradict it.
Do not invent statistics beyond what this provides."""
        else:
            fact_section = """
No specific headline found. Use general knowledge about this topic.
Only make claims you are confident are accurate.
Avoid specific numbers you are not sure about."""

        prompt = f"""You are writing a voiceover for India20Sixty — Indian YouTube Shorts about India's real near future.

Topic: {topic}
{fact_section}

MANDATORY LANGUAGE RULE — READ CAREFULLY:
You MUST write in Hinglish. This is non-negotiable.
At least 35% of all words must be Hindi or Urdu words.
If your output is mostly English sentences — you have FAILED this task.

Natural code-switching — NOT translated Hindi. The way a smart Delhi/Mumbai 25-year-old actually talks:
Use these Hindi/Urdu words naturally: dekho, yaar, soch lo, matlab, iska matlab, lekin, woh bhi,
abhi, pehle, phir, toh, kyunki, jaldi, bohot, sirf, bilkul, arre, kya hua, ho raha hai,
ban raha hai, aa raha hai, kar rahe hain, aapke liye, poore desh mein, gaon mein, sheher mein,
ab tak, kitna, kuch, koi, sab, haan, nahi, zaroor, seedha, sach mein

EXAMPLE — study this and match this energy:
"Dekho — AIIMS Delhi mein ek AI ne sirf 90 seconds mein cancer pakad liya... 95% accuracy ke saath, yaar. Sarkar ne already ₹3,000 crore approve kar diye hain — 1.5 lakh gawon tak pohunchane ke liye. Iska matlab aapke gaon mein bhi, jaldi hi... koi waiting list nahi, koi shahar nahi jaana. Lekin ek sawal hai — kya infrastructure ready hai? Technology toh aa gayi, lekin baaki cheezein? Comment mein batao."

NARRATION STRUCTURE — 8 flowing sentences:
1. Shocking hook with real fact — Hinglish from word one
2. What is actually happening right now
3. Scale — numbers, money, reach
4. What this means for regular Indians — vivid and specific
5. The challenge or twist — honest, not all rosy
6. Near future — "5 saal mein", "by 2030", "already testing kar rahe hain"
7. Emotional moment — pride or urgency in Hindi
8. Specific debate question — not generic

Rules: use "..." for pauses, no "2060", no invented stats, 8 sentences flowing NOT a list.
Write for: {topic}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.88, "max_tokens": 450},
                timeout=30
            )
            r.raise_for_status()
            raw   = r.json()["choices"][0]["message"]["content"].strip()
            lines = [re.sub(r'^[\d]+[.)]\s*|^[-•]\s*', '', l.strip())
                     for l in raw.split('\n') if l.strip()]
            script = ' '.join(lines)
            print(f"SCRIPT DONE ({len(lines)} lines): {script[:120]}...")
            return script, lines
        except Exception as e:
            print(f"SCRIPT FAILED: {e}")
            fallback = [
                f"Socho agar kal subah, {topic} India mein reality ban jaye...",
                "Yeh sirf kisi science fiction ki kahani nahi.",
                "Desh bhar ke scientists aur engineers iss sapne ko haqeeqat bana rahe hain.",
                "Already kai projects shuru ho chuke hain — aur results aa rahe hain.",
                "Jo aaj impossible lagta hai, woh kal normal ho jayega.",
                "India sirf follow nahi karta — ab India lead karta hai.",
                "Hamare daadi-nani ne bullock carts dekhe, humne smartphones dekhe.",
                "Aapko kya lagta hai — kya hum ready hain? Comment karo."
            ]
            return ' '.join(fallback), fallback

    # ── CAPTIONS ─────────────────────────────────────────────────

    def extract_captions(script_lines):
        full = ' '.join(script_lines)
        prompt = f"""Extract exactly 9 ultra-short caption phrases from this script.
Rules: 3-5 words each, ALL CAPS, punchy, in order, no punctuation except ! or ?
Output exactly 9 lines, one phrase per line only.
Script: {full}"""
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.3, "max_tokens": 200},
                timeout=20
            )
            r.raise_for_status()
            raw      = r.json()["choices"][0]["message"]["content"].strip()
            captions = [re.sub(r'^[\d]+[.)]\s*', '', l.strip()).upper()
                        for l in raw.split('\n') if l.strip()]
            captions = captions[:9]
            while len(captions) < 9:
                captions.append(captions[-1] if captions else "INDIA KA FUTURE")
            print(f"CAPTIONS: {captions}")
            return captions
        except Exception as e:
            print(f"CAPTION FAILED: {e}")
            words = full.upper().split()
            caps, step = [], max(1, len(words) // 9)
            for i in range(9):
                chunk = words[i * step: i * step + 4]
                caps.append(' '.join(chunk) if chunk else "INDIA KA FUTURE")
            return caps[:9]

    # ── SCENE PROMPTS ─────────────────────────────────────────────

    def generate_scene_prompts(fact_package=None):
        # Scenes 2+3 use randomised styles for variety
        style_insight = random.choice(VISUAL_STYLES)
        style_ending  = random.choice([s for s in VISUAL_STYLES if s != style_insight])
        shot_insight  = random.choice(SHOT_TYPES[1])
        shot_ending   = random.choice(SHOT_TYPES[2])

        fact_hint = ""
        if fact_package and fact_package.get("found"):
            fact_hint = f"\nReal context to incorporate: {fact_package.get('key_fact','')}"

        # Scene 1 — HOOK IMAGE: always a dedicated showstopper
        # High contrast, dramatic, instantly communicates the topic
        # Must make viewer stop scrolling and rewatch
        hook_prompt_request = f"""Create ONE ultra-dramatic showstopper image prompt for a YouTube Short hook frame.

Topic: "{topic}"{fact_hint}

This image appears in the FIRST 2 SECONDS. It must:
- Be visually SHOCKING or deeply CONTRASTING — dark vs light, old vs new, small vs massive
- Immediately communicate what the topic is WITHOUT text
- Create a feeling of AWE, URGENCY, or CURIOSITY in 0.5 seconds
- Be hyper-realistic cinematic photography quality
- Feature real Indian visual elements (people, places, colors)
- Use EXTREME contrast — either very dark dramatic lighting OR blinding bright colors
- Include ONE dominant subject that fills 70% of the frame
- Think: the kind of image that makes you stop mid-scroll and say "what IS this?"

Style: ultra high contrast cinematic photography, extreme dramatic lighting, 
8K hyperdetailed, film grain, shot on ARRI Alexa, award-winning photojournalism

Return ONLY the image prompt as a single string — no explanation, no labels."""

        scene2_3_request = f"""Create 2 cinematic image prompts for scenes 2 and 3 of a YouTube Short about: "{topic}"{fact_hint}

Scene 2 (Insight — the technology/change in action):
- Style: {style_insight}
- Shot: {shot_insight}
- Show the real innovation happening, human scale

Scene 3 (Ending — hopeful wide shot):
- Style: {style_ending}  
- Shot: {shot_ending}
- Emotional, wide, hopeful — India leading the future

Rules: SPECIFIC to "{topic}", Indian visual elements, 20-35 words each

Return ONLY: ["scene2_prompt", "scene3_prompt"]"""

        hook_prompt  = None
        scene_2_3    = None

        try:
            # Generate hook image prompt
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": hook_prompt_request}],
                      "temperature": 0.95, "max_tokens": 200},
                timeout=20
            )
            r.raise_for_status()
            hook_prompt = r.json()["choices"][0]["message"]["content"].strip().strip('"')
            print(f"  Hook image: {hook_prompt[:100]}...")
        except Exception as e:
            print(f"  Hook prompt failed: {e}")
            hook_prompt = (
                f"Extreme cinematic contrast — ancient Indian village on left half, "
                f"futuristic {topic} technology on right half, split-frame composition, "
                f"dramatic ARRI lighting, hyperdetailed 8K, film grain, award-winning photography"
            )

        try:
            # Generate scenes 2+3
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": scene2_3_request}],
                      "temperature": 0.9, "max_tokens": 250},
                timeout=20
            )
            r.raise_for_status()
            content  = r.json()["choices"][0]["message"]["content"].strip()
            start    = content.find('[')
            end      = content.rfind(']') + 1
            scene_2_3 = json.loads(content[start:end])
            print(f"  Scene 2: {scene_2_3[0][:80]}...")
            print(f"  Scene 3: {scene_2_3[1][:80]}...")
        except Exception as e:
            print(f"  Scene 2+3 prompts failed: {e}")
            scene_2_3 = [
                f"{topic} technology in action, Indian engineers, {style_insight}",
                f"Future of {topic} in India — {SCENE_TEMPLATES_FALLBACK[2]}"
            ]

        return [
            hook_prompt,
            scene_2_3[0] if scene_2_3 else SCENE_TEMPLATES_FALLBACK[1],
            scene_2_3[1] if scene_2_3 and len(scene_2_3) > 1 else SCENE_TEMPLATES_FALLBACK[2],
        ]

    # ── IMAGES ───────────────────────────────────────────────────

    def poll_for_image(generation_id, output_path):
        for poll in range(80):
            time.sleep(3)
            if poll % 5 == 0:
                print(f"  Polling {poll*3}s...")
            try:
                r = requests.get(
                    f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                    timeout=15
                )
                r.raise_for_status()
                gen    = r.json().get("generations_by_pk", {})
                status = gen.get("status", "UNKNOWN")
                if status == "FAILED":
                    raise Exception("Leonardo FAILED")
                if status == "COMPLETE":
                    images = gen.get("generated_images", [])
                    if not images:
                        raise Exception("COMPLETE but no images")
                    img_r = requests.get(images[0]["url"], timeout=30)
                    img_r.raise_for_status()
                    with open(output_path, "wb") as f:
                        f.write(img_r.content)
                    size = os.path.getsize(output_path)
                    print(f"  Saved: {size // 1024}KB")
                    return True
            except Exception as e:
                if "FAILED" in str(e) or "COMPLETE" in str(e):
                    raise
        raise Exception("Timeout: no image after 240s")

    def generate_image(scene_prompt, output_path):
        last_error = None
        for model_id in LEONARDO_MODELS:
            try:
                print(f"  Model: {model_id[:8]}...")
                r = requests.post(
                    "https://cloud.leonardo.ai/api/rest/v1/generations",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}",
                             "Content-Type": "application/json"},
                    json={"prompt": scene_prompt, "modelId": model_id,
                          "width": IMG_WIDTH, "height": IMG_HEIGHT,
                          "num_images": 1, "presetStyle": "CINEMATIC"},
                    timeout=30
                )
                if r.status_code != 200:
                    raise Exception(f"{r.status_code}: {r.text[:200]}")
                data = r.json()
                if "sdGenerationJob" not in data:
                    raise Exception("No sdGenerationJob")
                gen_id = data["sdGenerationJob"]["generationId"]
                print(f"  Gen ID: {gen_id}")
                return poll_for_image(gen_id, output_path)
            except Exception as e:
                last_error = e
                print(f"  Model failed: {str(e)[:100]}")
                time.sleep(5)
        raise Exception(f"All models failed: {last_error}")

    def generate_all_images(fact_package=None):
        print("\n[Generating scene prompts]")
        scene_prompts = generate_scene_prompts(fact_package)
        image_paths   = []
        for i, scene_prompt in enumerate(scene_prompts):
            update_status("images")
            print(f"\n[Image {i+1}/3]")
            path = f"{TMP_DIR}/{job_id}_{i}.png"
            if i > 0:
                time.sleep(8)
            try:
                generate_image(scene_prompt, path)
                image_paths.append(path)
            except Exception as e:
                print(f"Image {i+1} failed: {e}")
                if image_paths:
                    shutil.copy(image_paths[-1], path)
                else:
                    subprocess.run([
                        "ffmpeg", "-y", "-f", "lavfi",
                        "-i", f"color=c=0x0d1117:s={IMG_WIDTH}x{IMG_HEIGHT}:d=1",
                        "-frames:v", "1", path
                    ], capture_output=True, timeout=15)
                image_paths.append(path)
        return image_paths

    # ── VOICE ─────────────────────────────────────────────────────

    def generate_voice(script):
        update_status("voice")
        print("\n[Voice]")

        # Convert "..." to ElevenLabs break tag for natural pauses
        speech_text = script.replace("...", "<break time='0.4s'/>")

        r = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
            headers={"xi-api-key": ELEVENLABS_API_KEY,
                     "Content-Type": "application/json"},
            json={
                "text": speech_text,
                "model_id": "eleven_multilingual_v2",
                "voice_settings": {
                    "stability":         0.35,  # Lower = more expressive
                    "similarity_boost":  0.85,  # Stays true to voice character
                    "style":             0.45,  # Emotional range
                    "use_speaker_boost": True   # Clarity and presence
                }
            },
            timeout=60
        )
        r.raise_for_status()

        raw_path   = f"{TMP_DIR}/{job_id}_raw.mp3"
        audio_path = f"{TMP_DIR}/{job_id}.mp3"
        with open(raw_path, "wb") as f:
            f.write(r.content)

        duration = get_audio_duration(raw_path)
        print(f"  Raw: {duration:.1f}s")

        if duration < 24.0:
            run_ffmpeg([
                "ffmpeg", "-y", "-i", raw_path,
                "-af", f"apad=pad_dur={25.0-duration}", "-t", "25", audio_path
            ], "voice-pad", timeout=20)
            os.remove(raw_path)
        else:
            os.rename(raw_path, audio_path)

        print(f"  Final: {get_audio_duration(audio_path):.1f}s")
        return audio_path

    # ── RENDER ────────────────────────────────────────────────────

    def render_scene_clip(img_path, duration, scene_idx, captions):
        """
        Per-clip effect stack:
        1. zoompan  — slow zoom in from center (simple, reliable)
        2. eq       — contrast/brightness/saturation per scene
        3. unsharp  — cinematic sharpness
        4. hue      — subtle color tint (warm/cool/golden) per scene
        5. noise    — film grain overlay (dynamic texture)
        6. drawtext — timed captions
        """
        clip_path  = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
        third      = duration / 3.0
        grade      = SCENE_GRADES[scene_idx % 3]
        n_frames   = int(duration * FPS)
        cap_y      = int(OUT_HEIGHT * 0.73)
        cap_size   = 58

        # Zoom speed varies per scene — hook is fastest (most energy)
        zoom_speeds = [0.0018, 0.0012, 0.0008]
        zoom_speed  = zoom_speeds[scene_idx % 3]

        # Hue tint per scene — no quotes, safe values
        # scene 0: warm  (hue shift +8, saturation boost)
        # scene 1: cool  (hue shift -12)
        # scene 2: golden (hue shift +4, strong saturation)
        hue_filters = [
            "hue=h=8:s=1.2",
            "hue=h=-12:s=1.05",
            "hue=h=4:s=1.35",
        ]

        # Film grain — noise filter
        # alls=1 = apply to all channels, c0s=18 = luma grain strength
        # allf=t = temporal (changes each frame = animated grain)
        grain = "noise=c0s=14:c0f=t+u"

        print(f"  Clip {scene_idx}: zoompan[{zoom_speed}] | {grade['label']} | grain")

        # Step 1: zoompan — zoom in from center, simple expression
        # z starts at 1.0, increases by zoom_speed per frame, capped at 1.5
        # x/y keeps subject centered throughout zoom
        zoompan = (
            f"zoompan="
            f"z='min(zoom+{zoom_speed},1.5)':"
            f"x='iw/2-(iw/zoom/2)':"
            f"y='ih/2-(ih/zoom/2)':"
            f"d={n_frames}:"
            f"s={OUT_WIDTH}x{OUT_HEIGHT}:"
            f"fps={FPS}"
        )

        vf_parts = [
            # Scale to output first (zoompan works best on already-sized input)
            f"scale={OUT_WIDTH}:{OUT_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos",
            f"crop={OUT_WIDTH}:{OUT_HEIGHT}",
            # Zoompan motion
            zoompan,
            # Color grade
            grade["eq"],
            # Hue/saturation tint
            hue_filters[scene_idx % 3],
            # Sharpness
            grade["sharp"],
            # Film grain (dynamic per-frame texture)
            grain,
            "setsar=1",
        ]

        # Captions
        scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
        while len(scene_caps) < 3:
            scene_caps.append("")

        for ci, cap in enumerate(scene_caps):
            if not cap.strip():
                continue
            escaped = escape_dt(cap)
            t_start = ci * third
            t_end   = (ci + 1) * third
            vf_parts.append(
                f"drawtext=text='{escaped}'"
                f":fontsize={cap_size}:fontcolor=white"
                f":borderw=5:bordercolor=black@0.85"
                f":x=(w-text_w)/2:y={cap_y}"
                f":enable='between(t,{t_start:.3f},{t_end:.3f})'"
            )

        vf_str = ",".join(vf_parts)
        print(f"  vf: {vf_str[:140]}...")

        run_ffmpeg([
            "ffmpeg", "-y",
            "-loop", "1", "-r", str(FPS),
            "-i", img_path,
            "-vf", vf_str,
            "-t", str(duration),
            "-r", str(FPS),
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-pix_fmt", "yuv420p",
            clip_path
        ], f"clip-{scene_idx}", timeout=300)

        size = os.path.getsize(clip_path)
        print(f"  Clip {scene_idx}: {size // 1024}KB")
        return clip_path

    def apply_xfade(clip_paths, scene_dur):
        if len(clip_paths) == 1:
            return clip_paths[0]
        transition  = random.choice(XFADE_TRANSITIONS)
        output_path = f"{TMP_DIR}/{job_id}_xfaded.mp4"
        n           = len(clip_paths)
        inputs      = []
        for cp in clip_paths:
            inputs += ["-i", cp]

        fc_parts = []
        offset   = scene_dur - XFADE_DUR
        fc_parts.append(
            f"[0:v][1:v]xfade=transition={transition}"
            f":duration={XFADE_DUR}:offset={offset:.3f}[xf0]"
        )
        for i in range(2, n):
            offset  += scene_dur - XFADE_DUR
            prev     = f"[xf{i-2}]" if i > 2 else "[xf0]"
            fc_parts.append(
                f"{prev}[{i}:v]xfade=transition={transition}"
                f":duration={XFADE_DUR}:offset={offset:.3f}[xf{i-1}]"
            )

        print(f"\n  xfade: {transition}")
        try:
            run_ffmpeg([
                "ffmpeg", "-y", *inputs,
                "-filter_complex", ";".join(fc_parts),
                "-map", f"[xf{n-2}]",
                "-c:v", "libx264", "-preset", "fast", "-crf", "21",
                "-pix_fmt", "yuv420p", output_path
            ], "xfade", timeout=120)
            print(f"  xfaded: {os.path.getsize(output_path)//1024}KB")
            return output_path
        except Exception as e:
            print(f"  xfade failed ({e}), using concat")
            list_path   = f"{TMP_DIR}/{job_id}_list.txt"
            concat_path = f"{TMP_DIR}/{job_id}_concat.mp4"
            with open(list_path, "w") as f:
                for cp in clip_paths:
                    f.write(f"file '{cp}'\n")
            run_ffmpeg([
                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", list_path, "-c", "copy", concat_path
            ], "concat", timeout=60)
            try: os.remove(list_path)
            except Exception: pass
            return concat_path

    def render_video(images, audio, captions):
        update_status("render")
        print("\n[Render]")

        audio_dur  = get_audio_duration(audio)
        total_dur  = max(audio_dur, 25.0)
        scene_dur  = total_dur / len(images)
        video_path = f"{TMP_DIR}/{job_id}.mp4"

        print(f"  Audio: {audio_dur:.1f}s | {len(images)} scenes x {scene_dur:.1f}s")

        # Render clips
        clip_paths = []
        for i, img in enumerate(images):
            clip = render_scene_clip(img, scene_dur, i, captions)
            clip_paths.append(clip)
            try: os.remove(img)
            except Exception: pass

        # xfade transitions
        transitioned = apply_xfade(clip_paths, scene_dur)
        for cp in clip_paths:
            try: os.remove(cp)
            except Exception: pass

        # Final mux: watermark + fade out + loudnorm
        fade_out_st = total_dur - 0.5
        wm          = escape_dt("@India20Sixty")

        # Watermark: bottom-left, large font, thick black border
        # NO box= parameter (not in all ffmpeg builds — fails silently)
        watermark_vf = (
            f"drawtext=text='{wm}'"
            f":fontsize=46"
            f":fontcolor=white@0.92"
            f":borderw=4:bordercolor=black@0.9"
            f":x=28:y=h-90"
        )

        try:
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", transitioned, "-i", audio,
                "-filter_complex",
                f"[0:v]fade=t=out:st={fade_out_st:.2f}:d=0.5,"
                f"{watermark_vf}[vout];"
                f"[1:a]loudnorm=I=-16:TP=-1.5:LRA=11[aout]",
                "-map", "[vout]", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "128k",
                "-shortest", "-movflags", "+faststart", video_path
            ], "final-mux", timeout=120)
        except Exception as e:
            print(f"  loudnorm failed ({e}), retrying simple")
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", transitioned, "-i", audio,
                "-vf", f"fade=t=out:st={fade_out_st:.2f}:d=0.5,{watermark_vf}",
                "-map", "0:v", "-map", "1:a",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "128k",
                "-shortest", "-movflags", "+faststart", video_path
            ], "final-mux-simple", timeout=120)

        try: os.remove(transitioned)
        except Exception: pass

        size = os.path.getsize(video_path)
        if size < 100_000:
            raise Exception(f"Video too small: {size}")
        print(f"  Final video: {size // 1024}KB")
        return video_path

    # ── YOUTUBE ───────────────────────────────────────────────────

    def upload_to_youtube(video_path, title, script, fact_package=None):
        update_status("upload")
        print("\n[Upload]")
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id": YOUTUBE_CLIENT_ID,
                  "client_secret": YOUTUBE_CLIENT_SECRET,
                  "refresh_token": YOUTUBE_REFRESH_TOKEN,
                  "grant_type": "refresh_token"},
            timeout=10
        )
        r.raise_for_status()
        token = r.json()["access_token"]

        # Include source credit in description if we have a fact anchor
        source_line = ""
        if fact_package and fact_package.get("found"):
            source_line = f"\nSource: {fact_package.get('source', '')}\n"

        description = (
            f"{script}\n\n"
            f"{source_line}"
            "India20Sixty - India's near future, explained.\n\n"
            "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
        )

        metadata = {
            "snippet": {
                "title":       title[:100],
                "description": description[:5000],
                "tags":        ["Future India", "India innovation", "AI",
                                "Technology", "Shorts", "India2035"],
                "categoryId":  "28"
            },
            "status": {
                "privacyStatus":          "public",
                "selfDeclaredMadeForKids": False
            }
        }

        with open(video_path, "rb") as vf:
            r = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos"
                "?uploadType=multipart&part=snippet,status",
                headers={"Authorization": f"Bearer {token}"},
                files={"snippet": (None, json.dumps(metadata), "application/json"),
                       "video":   ("video.mp4", vf, "video/mp4")},
                timeout=300
            )
        r.raise_for_status()
        video_id = r.json()["id"]
        print(f"  YouTube: https://youtube.com/watch?v={video_id}")
        return video_id

    # ── RUN ───────────────────────────────────────────────────────

    try:
        update_status("processing", {"topic": topic})
        log_to_db("Pipeline started v3.0-factual")

        # PHASE 1: Research — pull real sources
        fact_package = research_topic(topic)
        log_to_db(f"Research: {'found' if fact_package else 'no anchor'}")

        # PHASE 2: Script — anchored to real facts
        script, script_lines = generate_script(fact_package)
        log_to_db(f"Script: {script[:80]}")

        # PHASE 3: Captions
        captions = extract_captions(script_lines)
        log_to_db(f"Captions: {captions[:3]}")

        # PHASE 4: Images — topic-specific prompts
        images = generate_all_images(fact_package)
        log_to_db(f"Images: {len(images)}")

        # PHASE 5: Voice — natural narration settings
        audio = generate_voice(script)
        log_to_db("Voice done")

        # PHASE 6: Render — effects + watermark
        video = render_video(images, audio, captions)
        log_to_db("Video rendered")

        # PHASE 7: Upload
        if TEST_MODE:
            print(f"\nTEST MODE — skipping upload")
            video_id, final_status = "TEST_MODE", "test_complete"
        else:
            title        = f"{topic} | India20Sixty #Shorts"
            video_id     = upload_to_youtube(video, title, script, fact_package)
            final_status = "complete"
            log_to_db(f"Uploaded: {video_id}")

        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/videos",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json",
                         "Prefer": "return=minimal"},
                json={"job_id": job_id, "topic": topic,
                      "youtube_url": f"https://youtube.com/watch?v={video_id}"
                                     if video_id != "TEST_MODE" else None},
                timeout=10
            )
        except Exception as e:
            print(f"videos insert (non-fatal): {e}")

        update_status(final_status, {
            "youtube_id":     video_id,
            "script_package": {
                "text":         script,
                "lines":        script_lines,
                "captions":     captions,
                "fact_anchor":  fact_package,
                "generated_at": datetime.utcnow().isoformat()
            }
        })

        for f in [audio, video]:
            try: os.remove(f)
            except Exception: pass

        print(f"\nPIPELINE COMPLETE: {video_id}\n")

    except Exception as e:
        msg = str(e)
        print(f"\nPIPELINE FAILED: {msg}\n{traceback.format_exc()}")
        log_to_db(f"FAILED: {msg[:400]}")
        update_status("failed", {"error": msg[:400]})
        raise


# ==========================================
# LOCAL TEST
# ==========================================

@app.local_entrypoint()
def main():
    print("Running v3.0 factual pipeline test...")
    run_pipeline.remote(
        job_id="test-v3-001",
        topic="ISRO building Indias first space station by 2035",
        webhook_url=""
    )
