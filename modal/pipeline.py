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
# MODAL APP
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
XFADE_DUR  = 0.3

LEONARDO_MODELS = [
    "aa77f04e-3eec-4034-9c07-d0f619684628",
    "1e60896f-3c26-4296-8ecc-53e2afecc132",
    "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
]

# ==========================================
# FULL EFFECTS LIBRARY
# Every confirmed-safe effect on Modal debian ffmpeg 5.x
# Visual Director picks from these based on script energy
# ==========================================

# ── MOTION LIBRARY ────────────────────────────────────────────
# Each entry: (label, x_expr_fn, y_expr_fn, headroom_pct)
# x_expr_fn / y_expr_fn take (dx, dy, n_frames) → string expression
# headroom_pct = how much bigger to scale the image (1.25 = 25% bigger)

MOTIONS = {
    "pan_right_slow":     {"hpct": 1.20, "x": lambda dx,dy,n: f"{dx}*n/{n}",       "y": lambda dx,dy,n: f"{dy//2}",              "label": "pan right slow"},
    "pan_right_fast":     {"hpct": 1.28, "x": lambda dx,dy,n: f"{dx}*n/{n}",       "y": lambda dx,dy,n: f"{dy//3}",              "label": "pan right fast"},
    "pan_left_slow":      {"hpct": 1.20, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",  "y": lambda dx,dy,n: f"{dy//2}",              "label": "pan left slow"},
    "pan_left_fast":      {"hpct": 1.28, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",  "y": lambda dx,dy,n: f"{dy//3}",              "label": "pan left fast"},
    "pan_up":             {"hpct": 1.22, "x": lambda dx,dy,n: f"{dx//2}",           "y": lambda dx,dy,n: f"{dy}-{dy}*n/{n}",     "label": "pan up"},
    "pan_down":           {"hpct": 1.22, "x": lambda dx,dy,n: f"{dx//2}",           "y": lambda dx,dy,n: f"{dy}*n/{n}",          "label": "pan down"},
    "diagonal_tl_br":     {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}*n/{n}",        "y": lambda dx,dy,n: f"{dy}*n/{n}",          "label": "diagonal tl-br"},
    "diagonal_tr_bl":     {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",   "y": lambda dx,dy,n: f"{dy}*n/{n}",          "label": "diagonal tr-bl"},
    "diagonal_bl_tr":     {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}*n/{n}",        "y": lambda dx,dy,n: f"{dy}-{dy}*n/{n}",     "label": "diagonal bl-tr"},
    "diagonal_br_tl":     {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",   "y": lambda dx,dy,n: f"{dy}-{dy}*n/{n}",     "label": "diagonal br-tl"},
    "zoom_in_sim":        {"hpct": 1.35, "x": lambda dx,dy,n: f"{dx//2}-{dx//4}*n/{n}", "y": lambda dx,dy,n: f"{dy//2}-{dy//4}*n/{n}", "label": "zoom in sim"},
    "pull_back_sim":      {"hpct": 1.35, "x": lambda dx,dy,n: f"{dx//4}+{dx//4}*n/{n}", "y": lambda dx,dy,n: f"{dy//4}+{dy//4}*n/{n}", "label": "pull back sim"},
    "drift_slow":         {"hpct": 1.12, "x": lambda dx,dy,n: f"{dx//3}*n/{n}",     "y": lambda dx,dy,n: f"{dy//4}",              "label": "drift slow"},
    "static_hold":        {"hpct": 1.08, "x": lambda dx,dy,n: f"{dx//2}",           "y": lambda dx,dy,n: f"{dy//2}",              "label": "static hold"},
}

# ── COLOR GRADE LIBRARY ───────────────────────────────────────
GRADES = {
    "warm_golden": {
        "eq": "eq=contrast=1.18:brightness=0.03:saturation=1.35",
        "hue": "hue=h=8:s=1.2",
        "sharp": "unsharp=5:5:0.8:3:3:0.0",
        "noise": "noise=c0s=12:c0f=t+u",
        "label": "warm_golden"
    },
    "cool_blue": {
        "eq": "eq=contrast=1.15:brightness=-0.02:saturation=1.1",
        "hue": "hue=h=-15:s=1.1",
        "sharp": "unsharp=3:3:1.0:3:3:0.0",
        "noise": "noise=c0s=10:c0f=t+u",
        "label": "cool_blue"
    },
    "high_contrast_noir": {
        "eq": "eq=contrast=1.45:brightness=-0.05:saturation=0.75",
        "hue": "hue=h=0:s=0.8",
        "sharp": "unsharp=7:7:1.2:3:3:0.0",
        "noise": "noise=c0s=18:c0f=t+u",
        "label": "high_contrast_noir"
    },
    "desaturated_serious": {
        "eq": "eq=contrast=1.25:brightness=0.0:saturation=0.5",
        "hue": "hue=h=0:s=0.55",
        "sharp": "unsharp=5:5:0.9:3:3:0.0",
        "noise": "noise=c0s=15:c0f=t+u",
        "label": "desaturated_serious"
    },
    "vivid_pop": {
        "eq": "eq=contrast=1.08:brightness=0.05:saturation=1.65",
        "hue": "hue=h=5:s=1.4",
        "sharp": "unsharp=3:3:0.6:3:3:0.0",
        "noise": "noise=c0s=8:c0f=t+u",
        "label": "vivid_pop"
    },
    "bleach_bypass": {
        "eq": "eq=contrast=1.35:brightness=0.02:saturation=0.65",
        "hue": "hue=h=-5:s=0.7",
        "sharp": "unsharp=5:5:1.0:3:3:0.0",
        "noise": "noise=c0s=20:c0f=t+u",
        "label": "bleach_bypass"
    },
}

# ── TRANSITION LIBRARY ────────────────────────────────────────
# All confirmed working on Modal debian ffmpeg 5.x
TRANSITIONS = {
    "hard_cut":   None,          # no xfade — instant cut (most energetic)
    "wipe_left":  "wipeleft",
    "wipe_right": "wiperight",
    "slide_left": "slideleft",
    "slide_right":"slideright",
    "dissolve":   "dissolve",
    "fade_black": "fadeblack",
    "fade":       "fade",
}

# ── ENERGY → DEFAULTS ────────────────────────────────────────
# Fallback if Visual Director fails — map energy to sensible defaults
ENERGY_DEFAULTS = {
    "high":   {"motion_a": "diagonal_tl_br", "motion_b": "diagonal_br_tl", "transition": "wipe_right", "grade": "high_contrast_noir"},
    "medium": {"motion_a": "pan_right_fast",  "motion_b": "pan_up",         "transition": "slide_left",  "grade": "warm_golden"},
    "low":    {"motion_a": "drift_slow",       "motion_b": "zoom_in_sim",    "transition": "dissolve",    "grade": "bleach_bypass"},
}

# Legacy compatibility
SCENE_GRADES      = [GRADES["warm_golden"], GRADES["cool_blue"], GRADES["vivid_pop"]]
XFADE_TRANSITIONS = list(v for v in TRANSITIONS.values() if v)

VISUAL_STYLES = [
    "cinematic ultra-realistic photography, golden hour, warm saffron and ochre palette, 8K India",
    "dramatic cinematic lighting, deep shadows, vivid neon accents, photorealistic Indian urban setting",
    "aerial drone perspective, sweeping wide angle, vibrant India, bustling cityscape below",
    "close-up editorial photography, shallow depth of field, Indian faces, soft bokeh, expressive",
    "epic establishing shot, atmospheric haze, moody cinematic film grain, Indian landscape",
    "futuristic neon-lit Indian megacity, rain-slicked streets, autorickshaws and flying drones",
    "bright optimistic daylight, clean futuristic Indian architecture, hopeful vibrant, lotus motifs",
    "golden sunset silhouettes over Indian skyline, dust particles, emotionally powerful cinematic",
    "blue hour twilight, Indian city lights reflecting in water, serene futuristic, ultra sharp",
    "dramatic overcast monsoon sky, god rays breaking through clouds, epic Indian landscape, hopeful",
    "hyperrealistic Indian scientist or engineer, modern lab with traditional motifs, ARRI cinematic",
    "vibrant street-level India, mix of ancient and ultra-modern, people of all ages, optimistic",
]

SHOT_TYPES = [
    ["extreme wide establishing shot of Indian city or landscape", "dramatic low angle hero shot of Indian technology",
     "sweeping panoramic view of India from above", "epic aerial wide shot of Indian megaproject"],
    ["medium shot of Indian engineers or workers in action", "intimate human-scale scene in Indian context",
     "detailed view of Indian technology or infrastructure", "focused mid-shot with Indian faces and depth"],
    ["soaring aerial overview of transformed Indian landscape", "wide hopeful scene of India's future",
     "golden hour wide shot of Indian achievement", "emotional cinematic close-up of Indian people"],
]

SCENE_TEMPLATES_FALLBACK = [
    "futuristic Indian megacity at golden hour, lotus-shaped towers, electric air taxis, marigold hues, cinematic",
    "Indian scientists in smart traditional attire, holographic displays, IIT-style research campus, temple meets lab",
    "aerial view of transformed green India, solar farms, villages with fibre internet, diverse communities, hopeful sunrise"
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
    image_urls  = data.get("image_urls") or []   # pre-selected images from library
    print(f"Trigger: {job_id} | {topic} | images: {'library' if image_urls else 'generate'}")
    run_pipeline.spawn(job_id=job_id, topic=topic, webhook_url=webhook_url,
                       image_urls=image_urls)
    return {"status": "started", "job_id": job_id, "topic": topic}


@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="GET")
def health():
    voice_mode = "unknown"
    try:
        import os
        SUPABASE_URL      = os.environ.get("SUPABASE_URL","")
        SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY","")
        if SUPABASE_URL:
            import requests as req
            r = req.get(f"{SUPABASE_URL}/rest/v1/system_state?id=eq.main&select=voice_mode",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                timeout=3)
            if r.status_code == 200 and r.json():
                voice_mode = r.json()[0].get("voice_mode","ai")
    except Exception:
        pass
    return {
        "status":     "healthy",
        "platform":   "modal",
        "version":    "4.0",
        "voice_mode": voice_mode,
        "effects":    ["pan_motion", "eq", "hue", "unsharp", "grain",
                       "xfade", "captions", "watermark", "fade_out"],
        "council":    ["trend_scout", "topic_council", "script_architect",
                       "language_expert"],
        "out":        f"{OUT_WIDTH}x{OUT_HEIGHT}",
    }


# ==========================================
# MAIN PIPELINE
# ==========================================

@app.function(image=image, secrets=secrets, cpu=2.0, memory=2048, timeout=600)
def run_pipeline(job_id: str, topic: str, webhook_url: str = "",
                 image_urls: list = None):

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
    LEONARDO_API_KEY      = os.environ["LEONARDO_API_KEY"]
    ELEVENLABS_API_KEY    = os.environ.get("ELEVENLABS_API_KEY", "")
    VOICE_ID              = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
    SUPABASE_URL          = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY     = os.environ["SUPABASE_ANON_KEY"]
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    TEST_MODE             = os.environ.get("TEST_MODE", "true").lower() == "true"
    R2_ACCOUNT_ID         = os.environ.get("R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY_ID      = os.environ.get("R2_ACCESS_KEY_ID", "")
    R2_SECRET_ACCESS_KEY  = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET             = os.environ.get("R2_BUCKET", "india20sixty")
    R2_BASE_URL           = os.environ.get("R2_BASE_URL", "")  # public bucket URL

    print(f"\n{'='*60}")
    print(f"PIPELINE v4.0 START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"TIME:  {datetime.utcnow().isoformat()}")
    print(f"TEST:  {TEST_MODE}")
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
            print(f"STATUS: {e}")

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
            return 25.0

    def run_ffmpeg(cmd, label, timeout=300):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            print(f"  ffmpeg [{label}] FAILED:")
            print(result.stderr[-500:])
            raise Exception(f"{label}: {result.stderr[-150:]}")
        return result

    def escape_dt(text):
        text = text.replace('\\', '\\\\')
        text = text.replace("'", "\u2019")
        text = text.replace(':', '\\:')
        text = text.replace('%', '\\%')
        return text

    # ── PHASE 1: RESEARCH ─────────────────────────────────────────

    def fetch_google_news_rss(query):
        try:
            encoded = requests.utils.quote(query)
            url     = (f"https://news.google.com/rss/search"
                       f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en")
            r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root    = ET.fromstring(r.content)
            items   = root.findall(".//item")[:5]
            results = []
            for item in items:
                title  = item.findtext("title", "").strip()
                source = item.findtext("source", "").strip()
                if title:
                    results.append({"headline": title, "source": source or "News"})
            return results
        except Exception as e:
            print(f"  News [{query[:25]}]: {e}")
            return []

    def fetch_pib_rss():
        try:
            url = "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3"
            r   = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root  = ET.fromstring(r.content)
            items = root.findall(".//item")[:8]
            return [{"headline": i.findtext("title","").strip(),
                     "source":   "PIB India"}
                    for i in items if i.findtext("title","").strip()]
        except Exception as e:
            print(f"  PIB: {e}")
            return []

    def research_topic():
        print("\n[Research]")
        queries   = [topic, f"{topic} India 2025"]
        headlines = []
        for q in queries:
            headlines += fetch_google_news_rss(q)
        headlines += fetch_pib_rss()

        seen, unique = set(), []
        for h in headlines:
            if h["headline"] not in seen:
                seen.add(h["headline"])
                unique.append(h)

        print(f"  Headlines: {len(unique)}")
        if not unique:
            return None

        headlines_text = "\n".join(
            f"- {h['headline']} ({h['source']})" for h in unique[:10]
        )
        prompt = f"""Find the most relevant headline to topic: "{topic}"

Headlines:
{headlines_text}

Return ONLY JSON:
{{"found": true, "headline": "...", "source": "...", "key_fact": "specific stat or number"}}

If none relevant: {{"found": false}}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.2, "max_tokens": 200},
                timeout=15
            )
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"].strip()
            data    = json.loads(content[content.find('{'):content.rfind('}')+1])
            if data.get("found"):
                print(f"  Fact: {data.get('key_fact','')[:80]}")
                return data
        except Exception as e:
            print(f"  Fact extract: {e}")
        return None

    # ── PHASE 2: SCRIPT ───────────────────────────────────────────

    def generate_script(fact_package=None):
        print("\nSCRIPT START")

        fact_section = ""
        if fact_package and fact_package.get("found"):
            fact_section = f"""
REAL FACT ANCHOR — use this:
Fact: {fact_package['key_fact']}
Source: {fact_package['source']}"""

        prompt = f"""Write a YouTube Shorts voiceover script for India20Sixty — India's near future channel.

Topic: {topic}
{fact_section}

STRICT RULES:
- Maximum 55 words total. Count every word before returning.
- Language: Indian English. Clear, confident, modern. NOT American or British tone.
  Indian English sounds direct and warm. Example: "This is actually happening" not "This is literally happening".
  No slang. No desi broken English. Proper grammar. But distinctly Indian in context and reference.
- NO Hindi words. NO Hinglish. Pure English only.
- Every sentence must be short and punchy — maximum 12 words per sentence.
- Open with a fact that stops the scroll.

6 sentences:
1. Hook — the real fact or number, make it land hard
2. What is happening right now in India
3. The scale — money, reach, jobs, impact
4. What this means for ordinary Indians
5. The honest challenge or twist
6. One debate question to drive comments

Return ONLY the script as plain text. No labels. No JSON. No Markdown."""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.75, "max_tokens": 200},
                timeout=30
            )
            r.raise_for_status()
            raw   = r.json()["choices"][0]["message"]["content"].strip()
            lines = [re.sub(r'^[\d]+[.)]\s*|^[-•]\s*', '', l.strip())
                     for l in raw.split('\n') if l.strip()]
            script = ' '.join(lines)
            print(f"SCRIPT DONE ({len(script.split())} words): {script[:100]}...")
            return script, lines
        except Exception as e:
            print(f"SCRIPT FAILED: {e}")
            fallback = f"India is building something that will change everything. {topic} is no longer a dream — work has already started. The government has committed serious money and a real deadline. Thousands of skilled jobs will follow. But execution is the real test. Will India deliver on time?"
            return fallback, [fallback]

    # ── PHASE 3: PRONUNCIATION FIX ────────────────────────────────
    # Pure deterministic find-and-replace — NO GPT, NO rewriting
    # Just swap known problem words. Script content is never changed.

    def language_expert_review(script):
        print("\n[Pronunciation Fix]")
        fixed = script

        # Acronyms — spell out so ElevenLabs reads letter by letter
        acronyms = [
            ("ISRO",        "I.S.R.O."),
            ("DRDO",        "D.R.D.O."),
            ("DRDO's",      "D.R.D.O.'s"),
            ("ISRO's",      "I.S.R.O.'s"),
            ("IIT",         "I.I.T."),
            ("IITs",        "I.I.T.s"),
            ("IIM",         "I.I.M."),
            ("AIIMS",       "A.I.I.M.S."),
            ("UPI",         "U.P.I."),
            ("NDTV",        "N.D.T.V."),
            ("NASSCOM",     "NAS-com"),
            ("SEBI",        "SEE-bi"),
            # EV is fine as-is, AI is fine as-is
        ]
        for wrong, right in acronyms:
            fixed = fixed.replace(wrong, right)

        # Indian mission names — add hyphens for syllabification
        missions = [
            ("Chandrayaan",  "Chandra-yaan"),
            ("Gaganyaan",    "Gagan-yaan"),
            ("Mangalyaan",   "Mangal-yaan"),
            ("Aditya-L1",    "Aditya L-one"),
        ]
        for wrong, right in missions:
            fixed = fixed.replace(wrong, right)

        # Symbols → words
        fixed = fixed.replace("₹", "rupees ")
        fixed = fixed.replace("%", " percent")
        fixed = fixed.replace("&", " and ")
        fixed = fixed.replace("→", " to ")
        fixed = fixed.replace("~", " approximately ")

        # Large Indian number formats → readable
        import re as _re
        # 1,00,000 → 1 lakh | 10,00,000 → 10 lakh | 1,00,00,000 → 1 crore
        def fix_numbers(text):
            text = _re.sub(r'(\d+),00,00,000', lambda m: m.group(1)+' crore', text)
            text = _re.sub(r'(\d+),00,000',    lambda m: m.group(1)+' lakh', text)
            text = _re.sub(r'(\d+),000',        lambda m: m.group(1)+' thousand', text)
            return text
        fixed = fix_numbers(fixed)

        # Add emotion tags at specific structural positions
        # Only at sentence boundaries — never mid-sentence
        # Rule: first sentence gets <excited> if it has a number/stat
        sentences = fixed.split('. ')
        if len(sentences) >= 1 and any(c.isdigit() for c in sentences[0]):
            sentences[0] = '<excited>' + sentences[0] + '</excited>'
        # Last question sentence gets <happy>
        if len(sentences) >= 2 and sentences[-1].strip().endswith('?'):
            sentences[-1] = '<happy>' + sentences[-1].strip() + '</happy>'
        fixed = '. '.join(sentences)

        print(f"  Fixed: {fixed[:120]}...")
        return fixed

    # ── PHASE 4: CAPTIONS ─────────────────────────────────────────

    def extract_captions(script_lines):
        full = ' '.join(script_lines)
        # Strip emotion tags for caption extraction
        clean = re.sub(r'<[^>]+>', '', full)
        prompt = f"""Extract exactly 9 caption phrases from this script.
Rules: 3-5 words each, ALL CAPS, punchy, in order, no punctuation except ! or ?
Output exactly 9 lines, one phrase per line only.
Script: {clean}"""
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
            words = clean.upper().split()
            caps, step = [], max(1, len(words) // 9)
            for i in range(9):
                chunk = words[i*step: i*step+4]
                caps.append(' '.join(chunk) if chunk else "INDIA KA FUTURE")
            return caps[:9]

    # ── PHASE 4b: VISUAL DIRECTOR ─────────────────────────────────
    # Reads the script and returns a shot list — one set of effects per scene.
    # Each scene gets: motion_a, motion_b (for split-clip), grade, transition.
    # GPT reads the script energy and picks from the confirmed-safe effects library.

    def visual_director(script, script_lines):
        print("\n[Visual Director]")

        # Build a clean version of the script for GPT (no emotion tags)
        clean = re.sub(r'<[^>]+>', '', script).strip()

        # Available options (GPT picks from these exact keys)
        motion_keys     = list(MOTIONS.keys())
        grade_keys      = list(GRADES.keys())
        transition_keys = list(TRANSITIONS.keys())

        prompt = f"""You are a YouTube Shorts video editor for an Indian tech/innovation channel.
Read this script and assign visual effects to each of the 3 scenes.

SCRIPT:
{clean}

SCENES:
- Scene 1 (Hook): first 2 sentences — must stop scrolling, highest impact
- Scene 2 (Content): sentences 3-4 — explain the story
- Scene 3 (Payoff): sentences 5-6 — challenge + debate question

For each scene assign:
- motion_a: first sub-clip motion (choose from: {", ".join(motion_keys)})
- motion_b: second sub-clip motion — MUST be different from motion_a
- grade: color grade mood (choose from: {", ".join(grade_keys)})
- transition: how this scene transitions INTO the NEXT scene (choose from: {", ".join(transition_keys)})
- energy: high / medium / low (reflects script energy at this point)

RULES:
- No two consecutive scenes should have same grade or same transition
- Scene 1 should have high energy — fast motion, bold grade
- Scene 3 (payoff) often suits bleach_bypass or warm_golden
- hard_cut is most energetic — use for shocking moments
- dissolve / fade for reflective endings
- diagonal motions feel more cinematic than simple left/right pans

Return ONLY valid JSON, no explanation:
{{
  "scene_1": {{"motion_a": "...", "motion_b": "...", "grade": "...", "transition": "...", "energy": "..."}},
  "scene_2": {{"motion_a": "...", "motion_b": "...", "grade": "...", "transition": "...", "energy": "..."}},
  "scene_3": {{"motion_a": "...", "motion_b": "...", "grade": "...", "transition": "...", "energy": "..."}}
}}"""

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.85, "max_tokens": 400},
                timeout=20
            )
            r.raise_for_status()
            raw  = r.json()["choices"][0]["message"]["content"].strip()
            # Extract JSON block
            raw  = raw[raw.find('{'):raw.rfind('}')+1]
            data = json.loads(raw)

            # Validate and sanitize each scene
            shot_list = []
            for sk in ["scene_1", "scene_2", "scene_3"]:
                s       = data.get(sk, {})
                energy  = s.get("energy", "medium")
                defs    = ENERGY_DEFAULTS.get(energy, ENERGY_DEFAULTS["medium"])

                ma = s.get("motion_a", defs["motion_a"])
                mb = s.get("motion_b", defs["motion_b"])
                gr = s.get("grade",    defs["grade"])
                tr = s.get("transition", defs["transition"])

                # Fall back to defaults if GPT returned an invalid key
                if ma not in MOTIONS:    ma = defs["motion_a"]
                if mb not in MOTIONS:    mb = defs["motion_b"]
                if gr not in GRADES:     gr = defs["grade"]
                if tr not in TRANSITIONS:tr = defs["transition"]
                if ma == mb:             mb = defs["motion_b"]  # force different

                shot_list.append({
                    "motion_a":   ma,
                    "motion_b":   mb,
                    "grade":      gr,
                    "transition": tr,
                    "energy":     energy,
                })

            for i, s in enumerate(shot_list):
                print(f"  Scene {i+1}: [{s['grade']}] {s['motion_a']}→{s['motion_b']} | {s['transition']} | {s['energy']}")
            return shot_list

        except Exception as e:
            print(f"  Visual Director failed ({e}), using energy defaults")
            # Fallback: rotate through different presets so videos still look different
            presets = [
                {"motion_a":"diagonal_bl_tr",  "motion_b":"zoom_in_sim",    "grade":"high_contrast_noir", "transition":"wipe_right",  "energy":"high"},
                {"motion_a":"pan_right_fast",   "motion_b":"pan_up",         "grade":"warm_golden",        "transition":"slide_left",  "energy":"medium"},
                {"motion_a":"zoom_in_sim",       "motion_b":"pull_back_sim",  "grade":"bleach_bypass",      "transition":"dissolve",    "energy":"low"},
            ]
            return presets

    # ── PHASE 5: SCENE PROMPTS ────────────────────────────────────

    def generate_scene_prompts(fact_package=None):
        style_insight = random.choice(VISUAL_STYLES)
        style_ending  = random.choice([s for s in VISUAL_STYLES if s != style_insight])
        shot_insight  = random.choice(SHOT_TYPES[1])
        shot_ending   = random.choice(SHOT_TYPES[2])

        fact_hint = ""
        if fact_package and fact_package.get("found"):
            fact_hint = f"\nReal context: {fact_package.get('key_fact','')}"

        # Hook image — dedicated showstopper brief, explicitly Indian
        hook_brief = f"""Create ONE ultra-dramatic showstopper image prompt for a YouTube Short hook frame.

Channel: India20Sixty — India's near future (tech, space, innovation, startups)
Topic: "{topic}"{fact_hint}

MANDATORY REQUIREMENTS:
- The image MUST look unmistakably Indian — Indian faces, Indian architecture, Indian landscape, or Indian technology
- ONE dominant subject filling 70% of frame
- Extreme contrast: old India vs new India, or dramatic futuristic Indian scene
- Hyperrealistic ARRI Alexa cinematic quality, 8K, film grain
- Make a viewer stop mid-scroll and say "what IS this happening in India?"

Indian visual anchors to consider: saffron sky, monsoon light, Indian scientists/engineers, ISRO control rooms,
IIT campuses, Indian megacity skylines, rural India transformed by tech, lotus motifs in futuristic architecture,
marigold colour palettes, Indian street life meets future tech

Return ONLY the image prompt as a single descriptive string. No labels."""

        scenes_brief = f"""Create 2 cinematic image prompts for an Indian tech/innovation YouTube Short.

Topic: "{topic}"{fact_hint}
Channel: India20Sixty — India's near future

Scene 2 (Insight/Story): {style_insight}, {shot_insight}
- Show the technology or innovation IN an Indian context
- Must include Indian visual elements: Indian people, Indian setting, Indian aesthetic
- Technology in action, showing scale and impact

Scene 3 (Hopeful Ending): {style_ending}, {shot_ending}
- Wide, emotional, optimistic view of India's future
- Should feel uplifting — India achieving something remarkable
- Real Indian people benefiting from this change

Both prompts must feel distinctly Indian, not generic international tech imagery.
Return ONLY: ["scene2_prompt", "scene3_prompt"]"""

        hook_prompt = None
        scene_2_3   = None

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": hook_brief}],
                      "temperature": 0.95, "max_tokens": 200},
                timeout=20
            )
            r.raise_for_status()
            hook_prompt = r.json()["choices"][0]["message"]["content"].strip().strip('"')
            print(f"  Hook: {hook_prompt[:80]}...")
        except Exception as e:
            print(f"  Hook prompt: {e}")
            hook_prompt = (
                f"Extreme cinematic contrast — crumbling old India vs gleaming futuristic India, "
                f"{topic} transformation, Indian engineers at work, ARRI cinematic lighting, "
                f"saffron sky, 8K film grain, hyperrealistic, unmistakably Indian"
            )

        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": scenes_brief}],
                      "temperature": 0.9, "max_tokens": 250},
                timeout=20
            )
            r.raise_for_status()
            content   = r.json()["choices"][0]["message"]["content"].strip()
            scene_2_3 = json.loads(content[content.find('['):content.rfind(']')+1])
        except Exception as e:
            print(f"  Scene 2+3: {e}")
            scene_2_3 = [
                f"{topic} technology India — {SCENE_TEMPLATES_FALLBACK[1]}",
                f"Future {topic} India — {SCENE_TEMPLATES_FALLBACK[2]}"
            ]

        return [
            hook_prompt,
            scene_2_3[0] if scene_2_3 else SCENE_TEMPLATES_FALLBACK[1],
            scene_2_3[1] if scene_2_3 and len(scene_2_3) > 1 else SCENE_TEMPLATES_FALLBACK[2],
        ]

    # ── PHASE 6: IMAGES ───────────────────────────────────────────

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
                        raise Exception("No images")
                    img_r = requests.get(images[0]["url"], timeout=30)
                    img_r.raise_for_status()
                    with open(output_path, "wb") as f:
                        f.write(img_r.content)
                    print(f"  Saved: {os.path.getsize(output_path)//1024}KB")
                    return True
            except Exception as e:
                if "FAILED" in str(e) or "COMPLETE" in str(e):
                    raise
        raise Exception("Timeout")

    # ══════════════════════════════════════════════════════════════
    # IMAGE ENGINE — 6-tier fallback chain
    # Tier 1 (Free AI):   Pollinations.ai  — no key, pure AI
    # Tier 2 (Free AI):   Hugging Face     — free API key, Flux model
    # Tier 3 (Free stock):Pixabay          — free photo search
    # Tier 4 (Cheap AI):  Together AI      — ~$0.01/image
    # Tier 5 (Cheap AI):  Replicate        — ~$0.003/image
    # Tier 6 (Paid AI):   Leonardo         — credits, best quality
    # Every successful image is saved to R2 for future library reuse.
    # ══════════════════════════════════════════════════════════════

    HF_API_KEY        = os.environ.get("HF_API_KEY", "")
    TOGETHER_API_KEY  = os.environ.get("TOGETHER_API_KEY", "")
    REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY", "")
    PIXABAY_API_KEY   = os.environ.get("PIXABAY_API_KEY", "")

    def _extract_keywords(prompt, n=4):
        """Pull n meaningful keywords from a prompt for stock search."""
        skip = {'cinematic','ultra','realistic','dramatic','hyperrealistic',
                'photorealistic','epic','wide','shot','award','winning','arri',
                'alexa','grain','film','8k','4k','high','contrast','indian',
                'india','futuristic','vibrant','moody','golden','hour','aerial',
                'stunning','beautiful','detailed','sharp','focus','bokeh',
                'lighting','dark','bright','color','colorful','create','image',
                'prompt','scene','showing','featuring','depicting','must','look'}
        words = re.sub(r'[^a-zA-Z\s]', '', prompt).lower().split()
        return [w for w in words if len(w) > 3 and w not in skip][:n]

    # ── TIER 1: Pollinations.ai (free, no key) ────────────────────
    def try_pollinations(prompt, output_path):
        try:
            import urllib.parse
            # Pollinations generates 1024x1792 portrait by default
            safe_prompt = urllib.parse.quote(prompt[:400])
            url = (f"https://image.pollinations.ai/prompt/{safe_prompt}"
                   f"?width={IMG_WIDTH}&height={IMG_HEIGHT}"
                   f"&model=flux&nologo=true&seed={random.randint(1,99999)}")
            print(f"  Pollinations: requesting...")
            r = requests.get(url, timeout=60, stream=True)
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            if size < 50_000:
                print(f"  Pollinations: image too small ({size}B), skipping")
                return False
            print(f"  Pollinations: OK ({size//1024}KB)")
            return True
        except Exception as e:
            print(f"  Pollinations failed: {e}")
            return False

    # ── TIER 2: Hugging Face Inference API (free tier) ────────────
    def try_huggingface(prompt, output_path):
        if not HF_API_KEY:
            return False
        try:
            # Use FLUX.1-schnell — fast, high quality, free tier
            models = [
                "black-forest-labs/FLUX.1-schnell",
                "stabilityai/stable-diffusion-xl-base-1.0",
            ]
            for model in models:
                try:
                    r = requests.post(
                        f"https://api-inference.huggingface.co/models/{model}",
                        headers={"Authorization": f"Bearer {HF_API_KEY}"},
                        json={"inputs": prompt[:500],
                              "parameters": {"width": IMG_WIDTH,
                                             "height": IMG_HEIGHT}},
                        timeout=60
                    )
                    if r.status_code == 503:
                        print(f"  HuggingFace {model}: model loading, skipping")
                        continue
                    if r.status_code != 200:
                        print(f"  HuggingFace {model}: {r.status_code}")
                        continue
                    with open(output_path, "wb") as f:
                        f.write(r.content)
                    size = os.path.getsize(output_path)
                    if size < 50_000:
                        continue
                    print(f"  HuggingFace {model}: OK ({size//1024}KB)")
                    return True
                except Exception as e:
                    print(f"  HuggingFace {model}: {e}")
                    continue
            return False
        except Exception as e:
            print(f"  HuggingFace failed: {e}")
            return False

    # ── TIER 3: Pixabay (free stock photos) ───────────────────────
    def try_pixabay(prompt, output_path):
        if not PIXABAY_API_KEY:
            return False
        try:
            keywords = _extract_keywords(prompt)
            query = ' '.join(keywords) if keywords else 'technology india'
            print(f"  Pixabay: '{query}'")
            r = requests.get(
                "https://pixabay.com/api/",
                params={"key": PIXABAY_API_KEY, "q": query,
                        "image_type": "photo", "orientation": "vertical",
                        "min_width": 800, "min_height": 1200,
                        "safesearch": "true", "per_page": 5},
                timeout=15
            )
            r.raise_for_status()
            hits = r.json().get("hits", [])
            if not hits:
                print(f"  Pixabay: no results")
                return False
            img_url = hits[0].get("largeImageURL") or hits[0].get("webformatURL")
            if not img_url:
                return False
            img_r = requests.get(img_url, timeout=30, stream=True)
            img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            if size < 50_000:
                return False
            print(f"  Pixabay: OK ({size//1024}KB)")
            return True
        except Exception as e:
            print(f"  Pixabay failed: {e}")
            return False

    # ── TIER 4: Together AI (cheap ~$0.01/image) ──────────────────
    def try_together(prompt, output_path):
        if not TOGETHER_API_KEY:
            return False
        try:
            r = requests.post(
                "https://api.together.xyz/v1/images/generations",
                headers={"Authorization": f"Bearer {TOGETHER_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model":  "black-forest-labs/FLUX.1-schnell-Free",
                      "prompt": prompt[:500],
                      "width":  IMG_WIDTH, "height": IMG_HEIGHT,
                      "steps":  4, "n": 1},
                timeout=60
            )
            print(f"  Together: {r.status_code}")
            if r.status_code != 200:
                return False
            data = r.json()
            img_url = data.get("data", [{}])[0].get("url")
            if not img_url:
                b64 = data.get("data", [{}])[0].get("b64_json")
                if b64:
                    import base64
                    with open(output_path, "wb") as f:
                        f.write(base64.b64decode(b64))
                    size = os.path.getsize(output_path)
                    print(f"  Together: OK b64 ({size//1024}KB)")
                    return size > 50_000
                return False
            img_r = requests.get(img_url, timeout=30, stream=True)
            img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            print(f"  Together: OK ({size//1024}KB)")
            return size > 50_000
        except Exception as e:
            print(f"  Together failed: {e}")
            return False

    # ── TIER 5: Replicate (cheap ~$0.003/image) ───────────────────
    def try_replicate(prompt, output_path):
        if not REPLICATE_API_KEY:
            return False
        try:
            # Use FLUX Schnell on Replicate
            r = requests.post(
                "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
                headers={"Authorization": f"Token {REPLICATE_API_KEY}",
                         "Content-Type": "application/json"},
                json={"input": {"prompt": prompt[:500],
                                "aspect_ratio": "9:16",
                                "output_format": "png",
                                "num_outputs": 1}},
                timeout=30
            )
            if r.status_code not in (200, 201):
                print(f"  Replicate: {r.status_code}")
                return False
            pred = r.json()
            pred_id = pred.get("id")
            if not pred_id:
                return False
            # Poll for result
            for _ in range(30):
                time.sleep(2)
                poll = requests.get(
                    f"https://api.replicate.com/v1/predictions/{pred_id}",
                    headers={"Authorization": f"Token {REPLICATE_API_KEY}"},
                    timeout=10
                )
                data = poll.json()
                status = data.get("status")
                if status == "succeeded":
                    urls = data.get("output", [])
                    if not urls:
                        return False
                    img_r = requests.get(urls[0], timeout=30, stream=True)
                    img_r.raise_for_status()
                    with open(output_path, "wb") as f:
                        for chunk in img_r.iter_content(8192):
                            f.write(chunk)
                    size = os.path.getsize(output_path)
                    print(f"  Replicate: OK ({size//1024}KB)")
                    return size > 50_000
                elif status in ("failed", "canceled"):
                    print(f"  Replicate prediction {status}")
                    return False
            print("  Replicate: timeout")
            return False
        except Exception as e:
            print(f"  Replicate failed: {e}")
            return False

    # ── TIER 6: Leonardo AI (paid, best quality) ──────────────────
    def try_leonardo(prompt, output_path):
        for model_id in LEONARDO_MODELS:
            try:
                r = requests.post(
                    "https://cloud.leonardo.ai/api/rest/v1/generations",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}",
                             "Content-Type": "application/json"},
                    json={"prompt": prompt, "modelId": model_id,
                          "width": IMG_WIDTH, "height": IMG_HEIGHT,
                          "num_images": 1, "presetStyle": "CINEMATIC"},
                    timeout=30
                )
                if r.status_code == 402:
                    print("  Leonardo: credits exhausted")
                    return False
                if r.status_code != 200:
                    print(f"  Leonardo: {r.status_code}")
                    time.sleep(5)
                    continue
                data = r.json()
                if "sdGenerationJob" not in data:
                    continue
                gen_id = data["sdGenerationJob"]["generationId"]
                print(f"  Leonardo gen: {gen_id}")
                poll_for_image(gen_id, output_path)
                size = os.path.getsize(output_path)
                print(f"  Leonardo: OK ({size//1024}KB)")
                return True
            except Exception as e:
                if "credits_exhausted" in str(e):
                    return False
                print(f"  Leonardo model failed: {str(e)[:60]}")
                time.sleep(5)
        return False

    # ── R2 LIBRARY fallback ───────────────────────────────────────
    def try_r2_library(prompt, output_path, exclude_keys=None):
        try:
            words = _extract_keywords(prompt)
            rows = []
            for term in words[:2]:
                r = requests.get(
                    f"{SUPABASE_URL}/rest/v1/image_cache"
                    f"?topic=ilike.*{term}*&select=r2_key,public_url,topic"
                    f"&limit=5&order=created_at.desc",
                    headers={"apikey": SUPABASE_ANON_KEY,
                             "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                    timeout=5
                )
                if r.status_code == 200:
                    rows.extend(r.json())
            if not rows:
                r = requests.get(
                    f"{SUPABASE_URL}/rest/v1/image_cache"
                    f"?select=r2_key,public_url,topic&limit=10&order=created_at.desc",
                    headers={"apikey": SUPABASE_ANON_KEY,
                             "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                    timeout=5
                )
                if r.status_code == 200:
                    rows = r.json()
            seen, unique = set(), []
            for row in rows:
                k = row.get("r2_key", "")
                if k and k not in seen and k not in (exclude_keys or []):
                    seen.add(k); unique.append(row)
            if not unique:
                return False
            chosen  = unique[0]
            img_url = chosen.get("public_url") or \
                      f"{R2_BASE_URL.rstrip('/')}/{chosen['r2_key']}"
            print(f"  R2 Library: '{chosen.get('topic','?')[:40]}'")
            img_r = requests.get(img_url, timeout=30, stream=True)
            img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            return size > 10_000
        except Exception as e:
            print(f"  R2 Library failed: {e}")
            return False

    def save_image_to_r2(local_path, topic_slug, idx):
        """Save generated image to R2 for future library reuse."""
        try:
            key        = f"images/{topic_slug}/{job_id}_{idx}.png"
            public_url = upload_to_r2(local_path, key)
            requests.post(
                f"{SUPABASE_URL}/rest/v1/image_cache",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json",
                         "Prefer": "return=minimal"},
                json={"job_id": job_id, "topic": topic,
                      "r2_key": key, "public_url": public_url,
                      "scene_idx": idx,
                      "created_at": datetime.utcnow().isoformat()},
                timeout=5
            )
            print(f"  Saved to R2: {key}")
            return key, public_url
        except Exception as e:
            print(f"  R2 save failed (non-fatal): {e}")
            return None, None

    def generate_image(prompt, output_path, exclude_r2_keys=None):
        """
        6-tier fallback chain. Tries engines in order, stops at first success.
        Tier 1: Pollinations.ai (free AI, no key)
        Tier 2: Hugging Face   (free AI, free key)
        Tier 3: Pixabay        (free stock, free key)
        Tier 4: Together AI    (cheap AI, ~$0.01)
        Tier 5: Replicate      (cheap AI, ~$0.003)
        Tier 6: Leonardo       (paid AI, best quality)
        Fallback: R2 Library   (your saved images)
        """
        TIERS = [
            ("Pollinations",  try_pollinations),
            ("HuggingFace",   try_huggingface),
            ("Pixabay",       try_pixabay),
            ("Together",      try_together),
            ("Replicate",     try_replicate),
            ("Leonardo",      try_leonardo),
        ]
        for name, fn in TIERS:
            print(f"  [Tier: {name}]")
            try:
                if fn(prompt, output_path):
                    print(f"  ✓ {name} succeeded")
                    return True
            except Exception as e:
                print(f"  {name} exception: {e}")
        # Final fallback: R2 Library
        print(f"  [Tier: R2 Library fallback]")
        if try_r2_library(prompt, output_path, exclude_r2_keys):
            print(f"  ✓ R2 Library succeeded")
            return True
        print(f"  ✗ All engines failed")
        return False

    def generate_all_images(fact_package=None, preselected_urls=None):
        """Generate 3 images using the 6-tier fallback chain."""
        # ── Pre-selected from library ──────────────────────────────
        if preselected_urls and len(preselected_urls) >= 3:
            print("\n[Images — library selection]")
            image_paths = []
            for i, url in enumerate(preselected_urls[:3]):
                path = f"{TMP_DIR}/{job_id}_{i}.png"
                try:
                    r = requests.get(url, timeout=60, stream=True)
                    r.raise_for_status()
                    with open(path, "wb") as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                    image_paths.append(path)
                    print(f"  Downloaded library image {i+1}: {os.path.getsize(path)//1024}KB")
                except Exception as e:
                    print(f"  Library download failed: {e}")
                    subprocess.run(["ffmpeg", "-y", "-f", "lavfi",
                        "-i", f"color=c=0x0d1117:s={IMG_WIDTH}x{IMG_HEIGHT}:d=1",
                        "-frames:v", "1", path], capture_output=True, timeout=15)
                    image_paths.append(path)
            return image_paths

        # ── Engine chain ───────────────────────────────────────────
        print("\n[Scene Prompts]")
        scene_prompts = generate_scene_prompts(fact_package)
        topic_slug    = re.sub(r'[^a-z0-9]+', '-', topic.lower())[:40]
        image_paths   = []
        used_r2_keys  = []

        for i, prompt in enumerate(scene_prompts):
            update_status("images")
            print(f"\n[Image {i+1}/3] {prompt[:60]}...")
            path = f"{TMP_DIR}/{job_id}_{i}.png"
            if i > 0:
                time.sleep(2)

            success = generate_image(prompt, path, exclude_r2_keys=used_r2_keys)

            if success:
                # Save to R2 library for future reuse
                key, _ = save_image_to_r2(path, topic_slug, i)
                if key:
                    used_r2_keys.append(key)
                image_paths.append(path)
            else:
                # Last resort: copy previous or black frame
                if image_paths:
                    shutil.copy(image_paths[-1], path)
                    print(f"  Using previous image as last resort")
                else:
                    subprocess.run(["ffmpeg", "-y", "-f", "lavfi",
                        "-i", f"color=c=0x0d1117:s={IMG_WIDTH}x{IMG_HEIGHT}:d=1",
                        "-frames:v", "1", path], capture_output=True, timeout=15)
                    print(f"  Using black frame as last resort")
                image_paths.append(path)

        return image_paths

    # ── PHASE 7: VOICE ────────────────────────────────────────────

    def generate_voice(reviewed_script):
        update_status("voice")
        print("\n[Voice]")

        # Convert "..." to ElevenLabs pause
        speech_text = reviewed_script.replace("...", "<break time='0.5s'/>")

        r = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
            headers={"xi-api-key": ELEVENLABS_API_KEY,
                     "Content-Type": "application/json"},
            json={
                "text":     speech_text,
                "model_id": "eleven_multilingual_v2",
                "voice_settings": {
                    "stability":         0.42,   # Slightly higher = less random accent drift
                    "similarity_boost":  0.85,
                    "style":             0.35,
                    "use_speaker_boost": True
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
        print(f"  Duration: {duration:.1f}s")
        os.rename(raw_path, audio_path)
        return audio_path, duration

    # ── PHASE 8: RENDER ───────────────────────────────────────────

    def render_scene_clip(img_path, duration, scene_idx, captions, shot=None):
        """
        Render one image as TWO sub-clips with a hard cut between them.
        Motion, grade, and energy come from the Visual Director shot list.
        Each video gets a unique look — no two scenes have the same motion pair.
        """
        clip_path  = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
        pre_path   = f"{TMP_DIR}/{job_id}_pre{scene_idx}.jpg"
        cap_y      = int(OUT_HEIGHT * 0.73)
        cap_size   = 58
        wm         = escape_dt("@India20Sixty")

        # Resolve shot from Visual Director or use fallback
        if shot is None:
            defs = ENERGY_DEFAULTS["medium"]
            shot = {"motion_a": defs["motion_a"], "motion_b": defs["motion_b"],
                    "grade": defs["grade"], "energy": "medium"}

        motion_a = MOTIONS.get(shot["motion_a"], MOTIONS["pan_right_fast"])
        motion_b = MOTIONS.get(shot["motion_b"], MOTIONS["diagonal_tl_br"])
        grade    = GRADES.get(shot["grade"],    GRADES["warm_golden"])
        energy   = shot.get("energy", "medium")

        # Scale headroom to whichever motion needs more
        hpct  = max(motion_a["hpct"], motion_b["hpct"])
        pan_w = int(OUT_WIDTH  * hpct)
        pan_h = int(OUT_HEIGHT * hpct)
        dx    = pan_w - OUT_WIDTH
        dy    = pan_h - OUT_HEIGHT

        print(f"  Clip {scene_idx}: [{grade['label']}] {shot['motion_a']} | {shot['motion_b']} [{energy}]")

        # PASS 1: Pre-process to scaled JPEG
        run_ffmpeg([
            "ffmpeg", "-y", "-i", img_path,
            "-vf", f"scale={pan_w}:{pan_h}:force_original_aspect_ratio=increase:flags=lanczos,crop={pan_w}:{pan_h}",
            "-frames:v", "1", "-q:v", "3", "-f", "image2", "-vcodec", "mjpeg", pre_path
        ], f"pre-{scene_idx}", timeout=20)

        # Split: A=42%, B=58%
        dur_a = duration * 0.42
        dur_b = duration * 0.58
        n_a   = int(dur_a * FPS)
        n_b   = int(dur_b * FPS)

        # Speed multiplier: high energy = faster travel
        speed = {"high": 1.0, "medium": 0.72, "low": 0.45}.get(energy, 0.72)
        sdx_a = max(1, min(int(dx * speed), dx))
        sdy_a = max(1, min(int(dy * speed), dy))
        sdx_b = max(1, min(int(dx * speed), dx))
        sdy_b = max(1, min(int(dy * speed), dy))

        x_a = motion_a["x"](sdx_a, sdy_a, n_a)
        y_a = motion_a["y"](sdx_a, sdy_a, n_a)
        x_b = motion_b["x"](sdx_b, sdy_b, n_b)
        y_b = motion_b["y"](sdx_b, sdy_b, n_b)

        def make_vf(x_expr, y_expr, caps_for_sub, sub_dur):
            third = sub_dur / 3.0
            parts = [
                f"crop={OUT_WIDTH}:{OUT_HEIGHT}:{x_expr}:{y_expr}",
                grade["eq"], grade["hue"], grade["sharp"], grade["noise"],
                "setsar=1",
                f"drawtext=text='{wm}':fontsize=44:fontcolor=white@0.9"
                f":borderw=4:bordercolor=black@0.95:x=28:y=h-88",
            ]
            for ci, cap in enumerate(caps_for_sub):
                if not cap.strip(): continue
                escaped = escape_dt(cap)
                t_s = ci * third
                t_e = (ci + 1) * third
                parts.append(
                    f"drawtext=text='{escaped}':fontsize={cap_size}:fontcolor=white"
                    f":borderw=5:bordercolor=black@0.85"
                    f":x=(w-text_w)/2:y={cap_y}"
                    f":enable='between(t,{t_s:.3f},{t_e:.3f})'"
                )
            return ",".join(parts)

        scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
        while len(scene_caps) < 3: scene_caps.append("")
        caps_a = [scene_caps[0], scene_caps[1], ""]
        caps_b = ["", scene_caps[2], ""]

        sub_a = f"{TMP_DIR}/{job_id}_clip{scene_idx}a.mp4"
        sub_b = f"{TMP_DIR}/{job_id}_clip{scene_idx}b.mp4"
        lst   = f"{TMP_DIR}/{job_id}_list{scene_idx}.txt"

        run_ffmpeg([
            "ffmpeg", "-y", "-loop", "1", "-r", str(FPS), "-i", pre_path,
            "-vf", make_vf(x_a, y_a, caps_a, dur_a),
            "-t", str(dur_a), "-r", str(FPS),
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-pix_fmt", "yuv420p", sub_a
        ], f"clip-{scene_idx}a", timeout=180)

        run_ffmpeg([
            "ffmpeg", "-y", "-loop", "1", "-r", str(FPS), "-i", pre_path,
            "-vf", make_vf(x_b, y_b, caps_b, dur_b),
            "-t", str(dur_b), "-r", str(FPS),
            "-c:v", "libx264", "-preset", "fast", "-crf", "20",
            "-pix_fmt", "yuv420p", sub_b
        ], f"clip-{scene_idx}b", timeout=180)

        with open(lst, "w") as f:
            f.write(f"file '{sub_a}'\nfile '{sub_b}'\n")
        run_ffmpeg([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", lst, "-c", "copy", clip_path
        ], f"concat-{scene_idx}", timeout=60)

        for p in [sub_a, sub_b, lst, pre_path]:
            try: os.remove(p)
            except Exception: pass
        try: os.remove(img_path)
        except Exception: pass

        print(f"  Clip {scene_idx}: {os.path.getsize(clip_path)//1024}KB")
        return clip_path

    def apply_xfade(clip_paths, scene_dur, shot_list=None):
        if len(clip_paths) == 1:
            return clip_paths[0]

        output_path = f"{TMP_DIR}/{job_id}_xfaded.mp4"
        n           = len(clip_paths)
        inputs      = []
        for cp in clip_paths:
            inputs += ["-i", cp]

        # Use per-scene transitions from shot_list if available
        # shot[i]["transition"] applies between clip i and clip i+1
        def get_transition(idx):
            if shot_list and idx < len(shot_list):
                key = shot_list[idx].get("transition", "dissolve")
                val = TRANSITIONS.get(key, "dissolve")
                # hard_cut = None means no xfade — fall back to dissolve for xfade chain
                return val if val else "dissolve"
            return random.choice(XFADE_TRANSITIONS)

        fc_parts = []
        offset   = scene_dur - XFADE_DUR
        t0       = get_transition(0)
        fc_parts.append(
            f"[0:v][1:v]xfade=transition={t0}"
            f":duration={XFADE_DUR}:offset={offset:.3f}[xf0]"
        )
        for i in range(2, n):
            offset += scene_dur - XFADE_DUR
            t_i    = get_transition(i - 1)
            prev   = f"[xf{i-2}]" if i > 2 else "[xf0]"
            fc_parts.append(
                f"{prev}[{i}:v]xfade=transition={t_i}"
                f":duration={XFADE_DUR}:offset={offset:.3f}[xf{i-1}]"
            )

        transitions_used = [get_transition(i) for i in range(n-1)]
        print(f"  xfade: {' → '.join(transitions_used)}")
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
            print(f"  xfade failed: {e}, using concat")
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

    def render_video(images, audio, audio_dur, captions, shot_list=None):
        update_status("render")
        print("\n[Render]")
        scene_dur  = audio_dur / len(images)
        video_path = f"{TMP_DIR}/{job_id}.mp4"
        print(f"  {audio_dur:.1f}s | {len(images)} scenes x {scene_dur:.1f}s")

        clip_paths = []
        for i, img in enumerate(images):
            shot = shot_list[i] if shot_list and i < len(shot_list) else None
            clip = render_scene_clip(img, scene_dur, i, captions, shot=shot)
            clip_paths.append(clip)

        transitioned = apply_xfade(clip_paths, scene_dur, shot_list=shot_list)
        for cp in clip_paths:
            try: os.remove(cp)
            except Exception: pass

        fade_out_st = audio_dur - 0.5
        try:
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", transitioned, "-i", audio,
                "-vf", f"fade=t=out:st={fade_out_st:.2f}:d=0.5",
                "-map", "0:v", "-map", "1:a",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "128k",
                "-t", str(audio_dur),          # use audio duration — not -shortest
                "-movflags", "+faststart", video_path
            ], "final-mux", timeout=120)
        except Exception as e:
            print(f"  fade failed ({e}), plain mux")
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", transitioned, "-i", audio,
                "-map", "0:v", "-map", "1:a",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "128k",
                "-t", str(audio_dur),          # use audio duration — not -shortest
                "-movflags", "+faststart", video_path
            ], "final-mux-plain", timeout=120)

        try: os.remove(transitioned)
        except Exception: pass

        size = os.path.getsize(video_path)
        if size < 100_000:
            raise Exception(f"Video too small: {size}")
        print(f"  Final: {size//1024}KB")
        return video_path

    # ── PHASE 9: TITLE ────────────────────────────────────────────

    def generate_title(script, fact_package=None):
        key_fact = fact_package.get("key_fact","") if fact_package else ""
        hooks    = [
            "Question hook: start with Why/How/What",
            "Shock stat: lead with the most surprising number",
            "Contrast: India Before vs After",
            "Timeline: 5 Years From Now / By 2030",
            "Revelation: Nobody Talks About This",
        ]
        prompt = f"""Write a YouTube Shorts title.
Topic: {topic}
Key fact: {key_fact}
Hook pattern: {random.choice(hooks)}
Rules: under 60 chars, NO emoji, plain English only, clickable, no hashtags.
Return ONLY the title text, nothing else."""
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.9, "max_tokens": 60},
                timeout=15
            )
            r.raise_for_status()
            raw_title = r.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
            # Strip any emoji GPT adds despite instructions
            import re
            raw_title = re.sub(r'[\U0001F000-\U0001FFFF]', '', raw_title)
            raw_title = re.sub(r'[\u2600-\u27BF]', '', raw_title).strip()
            if len(raw_title) > 95:
                raw_title = raw_title[:92] + "..."
            if not raw_title:
                raise ValueError("Empty title after sanitize")
            print(f"  Title: {raw_title}")
            return raw_title
        except Exception as e:
            print(f"  Title failed ({e}), using fallback")
            # Safe ASCII fallback titles — no emoji, no special chars
            options = [
                f"India's {topic[:45]} - The Real Story",
                f"Why Nobody Is Talking About This India Story",
                f"What Is Actually Happening With {topic[:40]}",
                f"India Just Changed The Game - {topic[:35]}",
                f"The Truth About {topic[:50]}",
            ]
            return random.choice(options)[:95]

    # ── PHASE 10: YOUTUBE ─────────────────────────────────────────

    def sanitize_for_youtube(text):
        """
        Remove characters YouTube API rejects in title and description.
        YouTube API (not Studio) rejects: emoji, Devanagari, smart quotes,
        em dashes, zero-width chars, control chars.
        All replacements go to safe ASCII equivalents.
        Letters, numbers, punctuation, spaces — ALL preserved.
        """
        import re
        if not text:
            return ""

        # Step 1: replace smart punctuation with ASCII equivalents
        replacements = [
            ('\u2019', "'"),    # right single quote / apostrophe
            ('\u2018', "'"),    # left single quote
            ('\u201c', '"'),    # left double quote
            ('\u201d', '"'),    # right double quote
            ('\u2013', '-'),    # en dash
            ('\u2014', '-'),    # em dash
            ('\u2026', '...'),  # ellipsis
            ('\u00a0', ' '),    # non-breaking space
            ('\u20b9', 'Rs.'),  # Indian rupee sign
            ('\u2022', '-'),    # bullet
            ('\u00b7', '-'),    # middle dot
        ]
        for bad, good in replacements:
            text = text.replace(bad, good)

        # Strip ElevenLabs emotion tags — YouTube rejects < > in descriptions
        import re as _re
        text = _re.sub(r'</?(?:excited|happy|sad|whisper|angry)[^>]*>', '', text)

        # Step 2: remove control chars (keep newline \n and tab \t)
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

        # Step 3: remove zero-width and invisible formatting chars
        text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)

        # Step 4: remove emoji — supplementary plane (U+1F000 and above)
        # Use \U (8-digit) for supplementary characters — \u only handles BMP
        text = re.sub(u'[\U0001F000-\U0001FFFF]', '', text)  # emoji / misc symbols
        text = re.sub(u'[\U00020000-\U0002FA1F]', '', text)  # CJK extension

        # Step 5: remove BMP emoji and symbol blocks (safe ranges only)
        text = re.sub(r'[\u2600-\u26FF]', '', text)   # misc symbols (☀☁⚡ etc)
        text = re.sub(r'[\u2700-\u27BF]', '', text)   # dingbats

        # Step 6: remove Devanagari and other non-Latin scripts
        text = re.sub(r'[\u0900-\u097F]', '', text)   # Devanagari (Hindi)
        text = re.sub(r'[\u0980-\u09FF]', '', text)   # Bengali
        text = re.sub(r'[\u0A00-\u0A7F]', '', text)   # Gurmukhi
        text = re.sub(r'[\u0A80-\u0AFF]', '', text)   # Gujarati
        text = re.sub(r'[\u0B00-\u0B7F]', '', text)   # Odia
        text = re.sub(r'[\u0B80-\u0BFF]', '', text)   # Tamil
        text = re.sub(r'[\u0C00-\u0C7F]', '', text)   # Telugu
        text = re.sub(r'[\u0C80-\u0CFF]', '', text)   # Kannada
        text = re.sub(r'[\u0D00-\u0D7F]', '', text)   # Malayalam

        # Step 7: collapse whitespace
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def upload_to_youtube(video_path, title, script, fact_package=None):
        update_status("upload")
        print("\n[Upload]")

        # ── OAuth token refresh ───────────────────────────────────
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id":     YOUTUBE_CLIENT_ID,
                  "client_secret": YOUTUBE_CLIENT_SECRET,
                  "refresh_token": YOUTUBE_REFRESH_TOKEN,
                  "grant_type":    "refresh_token"},
            timeout=10
        )
        print(f"  OAuth response ({r.status_code}): {r.text[:200]}")
        if not r.ok:
            raise Exception(f"YouTube OAuth failed {r.status_code}: {r.text[:200]}")
        token = r.json().get("access_token")
        if not token:
            raise Exception(f"No access_token in OAuth response: {r.text[:200]}")

        # ── Build sanitized metadata ───────────────────────────────
        source_raw = ""
        if fact_package and fact_package.get("found"):
            src = fact_package.get("source", "")
            if src:
                source_raw = f"\nSource: {src}\n"

        script_safe = sanitize_for_youtube(script or "")
        source_safe = sanitize_for_youtube(source_raw)
        description = (
            f"{script_safe}\n\n{source_safe}"
            "India20Sixty - India's near future, explained.\n\n"
            "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
        )

        safe_title = sanitize_for_youtube(title or topic[:80])[:100]
        if not safe_title.strip():
            safe_title = f"India Future Tech - {sanitize_for_youtube(topic[:60])}"

        print(f"  Title ({len(safe_title)}): {safe_title}")
        print(f"  Desc ({len(description)} chars): {description[:120]}...")

        metadata = {
            "snippet": {
                "title":       safe_title,
                "description": description[:5000],
                "tags":        ["Future India", "India innovation", "AI",
                                "Technology", "Shorts", "India2030"],
                "categoryId":  "28"
            },
            "status": {
                "privacyStatus":          "public",
                "selfDeclaredMadeForKids": False
            }
        }

        # ── Upload via multipart/related (required by YouTube API) ─
        # YouTube requires multipart/related — NOT multipart/form-data
        # Manually construct the body with a boundary
        boundary  = "india20sixty_upload_boundary"
        meta_json = json.dumps(metadata).encode("utf-8")

        with open(video_path, "rb") as vf:
            video_bytes = vf.read()

        body = (
            f"--{boundary}\r\n"
            f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
        ).encode("utf-8")
        body += meta_json
        body += (
            f"\r\n--{boundary}\r\n"
            f"Content-Type: video/mp4\r\n\r\n"
        ).encode("utf-8")
        body += video_bytes
        body += f"\r\n--{boundary}--".encode("utf-8")

        print(f"  Uploading {len(video_bytes)//1024}KB...")
        r = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos"
            "?uploadType=multipart&part=snippet,status",
            headers={
                "Authorization":  f"Bearer {token}",
                "Content-Type":   f"multipart/related; boundary={boundary}",
                "Content-Length": str(len(body)),
            },
            data=body,
            timeout=300
        )
        print(f"  YouTube response ({r.status_code}): {r.text[:400]}")
        r.raise_for_status()
        video_id = r.json()["id"]
        print(f"  UPLOADED: https://youtube.com/watch?v={video_id}")
        return video_id


    def render_video_silent(images, captions, shot_list=None):
        """Render video without audio track — for human voice staging."""
        update_status("render")
        print("\n[Render — silent]")

        scene_dur  = 8.6
        total_dur  = scene_dur * len(images)
        video_path = f"{TMP_DIR}/{job_id}_silent.mp4"

        print(f"  {len(images)} scenes x {scene_dur:.1f}s = {total_dur:.1f}s total")

        clip_paths = []
        for i, img in enumerate(images):
            shot = shot_list[i] if shot_list and i < len(shot_list) else None
            clip = render_scene_clip(img, scene_dur, i, captions, shot=shot)
            clip_paths.append(clip)

        transitioned = apply_xfade(clip_paths, scene_dur, shot_list=shot_list)
        for cp in clip_paths:
            try: os.remove(cp)
            except Exception: pass

        # Simple copy — no audio
        run_ffmpeg([
            "ffmpeg", "-y",
            "-i", transitioned,
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-an",  # no audio
            "-movflags", "+faststart",
            video_path
        ], "silent-render", timeout=120)

        try: os.remove(transitioned)
        except Exception: pass

        size = os.path.getsize(video_path)
        print(f"  Silent video: {size//1024}KB")
        return video_path

    def upload_to_r2(file_path, r2_key):
        """
        Upload file to Cloudflare R2 via S3-compatible API.
        Returns public URL or signed URL.
        """
        print(f"\n[R2 Upload] {r2_key}")

        if not R2_ACCOUNT_ID:
            print("  R2 not configured — returning local path for TEST_MODE")
            return f"file://{file_path}"

        try:
            import hmac
            import hashlib
            import base64
            from urllib.parse import quote

            endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
            url      = f"{endpoint}/{R2_BUCKET}/{r2_key}"

            with open(file_path, "rb") as f:
                data = f.read()

            # Use AWS4 signature (R2 is S3-compatible)
            now       = datetime.utcnow()
            date_str  = now.strftime("%Y%m%d")
            time_str  = now.strftime("%Y%m%dT%H%M%SZ")
            content_type = "video/mp4"
            payload_hash = hashlib.sha256(data).hexdigest()

            headers = {
                "Content-Type": content_type,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": time_str,
                "Host": f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            }

            # Canonical request
            signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
            canonical = "\n".join([
                "PUT",
                f"/{R2_BUCKET}/{quote(r2_key, safe='/')}",
                "",
                f"content-type:{content_type}",
                f"host:{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                f"x-amz-content-sha256:{payload_hash}",
                f"x-amz-date:{time_str}",
                "",
                signed_headers,
                payload_hash,
            ])

            cred_scope = f"{date_str}/auto/s3/aws4_request"
            string_to_sign = "\n".join([
                "AWS4-HMAC-SHA256",
                time_str,
                cred_scope,
                hashlib.sha256(canonical.encode()).hexdigest(),
            ])

            def sign(key, msg):
                return hmac.new(key, msg.encode(), hashlib.sha256).digest()

            signing_key = sign(
                sign(sign(sign(
                    f"AWS4{R2_SECRET_ACCESS_KEY}".encode(),
                    date_str), "auto"), "s3"), "aws4_request")

            signature = hmac.new(
                signing_key, string_to_sign.encode(), hashlib.sha256
            ).hexdigest()

            headers["Authorization"] = (
                f"AWS4-HMAC-SHA256 Credential={R2_ACCESS_KEY_ID}/{cred_scope},"
                f"SignedHeaders={signed_headers},Signature={signature}"
            )

            r = requests.put(url, data=data, headers=headers, timeout=120)
            r.raise_for_status()

            # Return public URL if configured, else the R2 endpoint URL
            if R2_BASE_URL:
                public_url = f"{R2_BASE_URL.rstrip('/')}/{r2_key}"
            else:
                public_url = url

            size = len(data)
            print(f"  R2: {size//1024}KB → {public_url}")
            return public_url

        except Exception as e:
            print(f"  R2 upload failed: {e}")
            # Non-fatal in test mode
            if TEST_MODE:
                return f"r2-error://{r2_key}"
            raise

    # ── RUN ───────────────────────────────────────────────────────

    def preflight_check():
        """
        Check everything before spending a single credit.
        Returns (ok: bool, reason: str)
        """
        print("\n[Pre-flight Check]")
        issues = []

        # 1. YouTube OAuth — try to get a fresh access token
        if not TEST_MODE:
            if not all([YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN]):
                issues.append("YouTube credentials missing from secrets")
            else:
                try:
                    r = requests.post(
                        "https://oauth2.googleapis.com/token",
                        data={"client_id":     YOUTUBE_CLIENT_ID,
                              "client_secret":  YOUTUBE_CLIENT_SECRET,
                              "refresh_token":  YOUTUBE_REFRESH_TOKEN,
                              "grant_type":     "refresh_token"},
                        timeout=10
                    )
                    if r.status_code == 200:
                        token_data = r.json()
                        if "access_token" not in token_data:
                            issues.append(f"YouTube token missing access_token: {token_data}")
                        else:
                            print("  YouTube OAuth: OK ✓")
                            # Channel check — non-fatal warning only
                            try:
                                ch = requests.get(
                                    "https://www.googleapis.com/youtube/v3/channels"
                                    "?part=status,snippet&mine=true",
                                    headers={"Authorization": f"Bearer {token_data['access_token']}"},
                                    timeout=10
                                )
                                if ch.status_code == 200:
                                    items = ch.json().get("items", [])
                                    if items:
                                        ch_name = items[0].get("snippet", {}).get("title", "Unknown")
                                        print(f"  Channel: {ch_name} ✓")
                                    else:
                                        print("  Channel check: no items returned (non-fatal)")
                                else:
                                    print(f"  Channel check: {ch.status_code} (non-fatal, continuing)")
                            except Exception as ce:
                                print(f"  Channel check skipped: {ce} (non-fatal)")

                            # Quota check — non-fatal
                            try:
                                qr = requests.get(
                                    "https://www.googleapis.com/youtube/v3/videos"
                                    "?part=id&mine=true&maxResults=1",
                                    headers={"Authorization": f"Bearer {token_data['access_token']}"},
                                    timeout=10
                                )
                                if qr.status_code == 403:
                                    err = qr.json().get("error", {})
                                    if "quotaExceeded" in str(err):
                                        issues.append("YouTube API quota exceeded for today")
                                    else:
                                        print(f"  Quota check: 403 (non-quota error, continuing)")
                                else:
                                    print(f"  Quota check: {qr.status_code} ✓")
                            except Exception as qe:
                                print(f"  Quota check skipped: {qe} (non-fatal)")
                    else:
                        err_body = r.json() if r.headers.get("content-type","").startswith("application/json") else r.text[:200]
                        issues.append(f"YouTube OAuth failed {r.status_code}: {err_body}")
                except Exception as e:
                    issues.append(f"YouTube pre-flight exception: {e}")

        # 4. ElevenLabs — check subscription/credits
        try:
            el = requests.get(
                "https://api.elevenlabs.io/v1/user/subscription",
                headers={"xi-api-key": ELEVENLABS_API_KEY},
                timeout=8
            )
            if el.status_code == 200:
                sub  = el.json()
                used = sub.get("character_count", 0)
                lim  = sub.get("character_limit", 0)
                left = lim - used
                print(f"  ElevenLabs: {used:,}/{lim:,} chars used, {left:,} remaining")
                if left < 500:
                    issues.append(f"ElevenLabs nearly out of credits: {left} chars left")
            else:
                print(f"  ElevenLabs check: {el.status_code} (non-fatal)")
        except Exception as e:
            print(f"  ElevenLabs check failed (non-fatal): {e}")

        # 5. Leonardo — check credits
        try:
            leo = requests.get(
                "https://cloud.leonardo.ai/api/rest/v1/me",
                headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                timeout=8
            )
            if leo.status_code == 200:
                data    = leo.json().get("user_details", [{}])[0]
                tokens  = data.get("tokenRenewalDate", "unknown")
                credits = data.get("apiCreditBalance", "?")
                print(f"  Leonardo: credits={credits}, renewal={tokens}")
                if isinstance(credits, (int, float)) and credits < 10:
                    issues.append(f"Leonardo credits very low: {credits}")
            else:
                print(f"  Leonardo check: {leo.status_code} (non-fatal)")
        except Exception as e:
            print(f"  Leonardo check failed (non-fatal): {e}")

        if issues:
            print(f"  PRE-FLIGHT FAILED: {issues}")
            return False, " | ".join(issues)

        print("  Pre-flight: ALL CLEAR ✓")
        return True, "ok"

    try:
        update_status("processing", {"topic": topic})
        log_to_db("Pipeline v4.0 started")

        # 0. PRE-FLIGHT — check publish state before spending any credits
        ok, reason = preflight_check()
        if not ok:
            raise Exception(f"Pre-flight failed: {reason}")
        log_to_db(f"Pre-flight: {reason}")

        # 0b. Read voice mode from system_state
        voice_mode = "ai"  # default
        try:
            rows = requests.get(
                f"{SUPABASE_URL}/rest/v1/system_state?id=eq.main&select=voice_mode",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                timeout=5
            ).json()
            voice_mode = rows[0].get("voice_mode", "ai") if rows else "ai"
        except Exception as e:
            print(f"  voice_mode read failed ({e}), defaulting to 'ai'")
        print(f"  Voice mode: {voice_mode.upper()}")
        log_to_db(f"Voice mode: {voice_mode}")

        # 1. Research
        fact_package = research_topic()
        log_to_db(f"Research: {'found' if fact_package else 'no anchor'}")

        # 2. Script
        script, script_lines = generate_script(fact_package)
        log_to_db(f"Script ({len(script.split())}w): {script[:60]}")

        # 3. Language Expert — pronunciation + emotion tags
        reviewed_script = language_expert_review(script)
        log_to_db("Language review done")

        # 4. Captions
        captions = extract_captions(script_lines)

        # 4b. Visual Director — assigns motion, grade, transition per scene
        shot_list = visual_director(script, script_lines)

        # 5. Images — use pre-selected from library or generate via Leonardo
        images = generate_all_images(fact_package,
                                     preselected_urls=image_urls or None)
        log_to_db(f"Images: {len(images)}")

        # ══ BRANCH ON VOICE MODE ══════════════════════════════════

        if voice_mode == "human":
            # ── HUMAN VOICE MODE ──────────────────────────────────
            # Render silent video (no audio track)
            # Save to R2 and park in staging queue
            print("\n[Human Voice Mode — rendering silent video]")

            video = render_video_silent(images, captions, shot_list=shot_list)
            log_to_db("Silent video rendered")

            # Upload to R2
            r2_key      = f"staged/{job_id}/video.mp4"
            video_r2_url = upload_to_r2(video, r2_key)
            log_to_db(f"Staged to R2: {r2_key}")

            # Save script package for the studio view
            update_status("staged", {
                "script_package": {
                    "text":         reviewed_script,
                    "original":     script,
                    "lines":        script_lines,
                    "captions":     captions,
                    "fact_anchor":  fact_package,
                    "generated_at": datetime.utcnow().isoformat(),
                },
                "video_r2_url":  video_r2_url,
                "video_r2_key":  r2_key,
            })

            print(f"\nSTAGED: {job_id}")
            print(f"  Video: {video_r2_url}")
            print("  Waiting for human voice recording in dashboard.")

            try: os.remove(video)
            except Exception: pass

        else:
            # ── AI VOICE MODE ─────────────────────────────────────
            # Full ElevenLabs pipeline, auto-upload to YouTube
            print("\n[AI Voice Mode — full auto pipeline]")

            audio, audio_dur = generate_voice(reviewed_script)
            log_to_db(f"Voice: {audio_dur:.1f}s")

            video = render_video(images, audio, audio_dur, captions, shot_list=shot_list)
            log_to_db("Video rendered")

            # AI mode: check publish gate
            if TEST_MODE:
                print(f"\nTEST MODE — skipping YouTube upload")
                video_id, final_status = "TEST_MODE", "test_complete"
            else:
                # Check publish state from system_state
                publish_enabled = False
                try:
                    pub_rows = requests.get(
                        f"{SUPABASE_URL}/rest/v1/system_state?id=eq.main&select=publish",
                        headers={"apikey": SUPABASE_ANON_KEY,
                                 "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                        timeout=5
                    ).json()
                    publish_enabled = pub_rows[0].get("publish", False) if pub_rows else False
                except Exception as e:
                    print(f"  Publish state check failed: {e}")

                if not publish_enabled:
                    # CBDP mode — video rendered, PUBLISH is OFF
                    # Save to R2 for review in dashboard
                    print("\n[PUBLISH OFF — saving to review queue (CBDP)]")
                    r2_key       = f"review/{job_id}/video.mp4"
                    video_r2_url = upload_to_r2(video, r2_key)
                    title        = generate_title(reviewed_script, fact_package)
                    update_status("review", {
                        "video_r2_url":  video_r2_url,
                        "video_r2_key":  r2_key,
                        "script_package": {
                            "text":         reviewed_script,
                            "original":     script,
                            "lines":        script_lines,
                            "captions":     captions,
                            "fact_anchor":  fact_package,
                            "title":        title,
                            "generated_at": datetime.utcnow().isoformat(),
                        }
                    })
                    print(f"\nCBDP: {job_id}")
                    print(f"  Video saved to R2: {r2_key}")
                    print(f"  Title ready: {title}")
                    print("  Waiting for review in dashboard.")
                    try: os.remove(video)
                    except Exception: pass
                    try: os.remove(audio)
                    except Exception: pass
                else:
                    title        = generate_title(reviewed_script, fact_package)
                    video_id     = upload_to_youtube(video, title, reviewed_script, fact_package)
                    final_status = "complete"
                    log_to_db(f"Uploaded: {video_id} | {title}")

                    try:
                        requests.post(
                            f"{SUPABASE_URL}/rest/v1/videos",
                            headers={"apikey": SUPABASE_ANON_KEY,
                                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                                     "Content-Type": "application/json",
                                     "Prefer": "return=minimal"},
                            json={"job_id": job_id, "topic": topic,
                                  "youtube_url": f"https://youtube.com/watch?v={video_id}"
                                                 if video_id not in ("TEST_MODE",) else None},
                            timeout=10
                        )
                    except Exception as e:
                        print(f"videos insert: {e}")

                    update_status(final_status, {
                        "youtube_id":     video_id,
                        "script_package": {
                            "text":         reviewed_script,
                            "original":     script,
                            "lines":        script_lines,
                            "captions":     captions,
                            "fact_anchor":  fact_package,
                            "generated_at": datetime.utcnow().isoformat(),
                        }
                    })

                    for f in [audio, video]:
                        try: os.remove(f)
                        except Exception: pass

                    print(f"\nPIPELINE COMPLETE (AI): {video_id}\n")

    except Exception as e:
        msg = str(e)
        print(f"\nFAILED: {msg}\n{traceback.format_exc()}")
        log_to_db(f"FAILED: {msg[:400]}")

        # ── CBDP Detection ─────────────────────────────────────────
        # "Completed But Didn't Publish" — video rendered, upload failed
        # Check if this failure happened at upload stage (video exists in R2)
        upload_keywords = ["400", "401", "403", "youtube", "upload", "quota",
                           "invalidDescription", "Bad Request", "oauth"]
        is_upload_fail  = any(kw.lower() in msg.lower() for kw in upload_keywords)

        # Check if video_r2_url was saved (AI mode saves locally not R2,
        # but script_package exists meaning we got past render)
        try:
            job_rows = requests.get(
                f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
                "&select=script_package,video_r2_url",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                timeout=5
            ).json()
            has_script = bool(job_rows and job_rows[0].get("script_package"))
        except Exception:
            has_script = False

        if is_upload_fail and has_script:
            # Mark as CBDP — video was ready, only upload failed
            print("  → CBDP: upload failed but video was rendered. Marking for retry.")
            update_status("cbdp", {
                "error": f"CBDP: {msg[:350]}",
            })
        else:
            update_status("failed", {"error": msg[:400]})
        raise


# ==========================================
# RETRY UPLOAD — for CBDP jobs
# ==========================================

@app.function(image=image, secrets=secrets, cpu=1.0, memory=512, timeout=120)
@modal.fastapi_endpoint(method="POST")
def retry_upload(data: dict):
    """
    Re-upload a CBDP job to YouTube without re-rendering.
    Called from dashboard Staging → CBDP tab → Retry button.
    data = { job_id: str }
    For AI mode jobs: re-downloads rendered video from script_package data
    and re-attempts YouTube upload with fixed sanitizer.
    """
    import json as _json, traceback as _tb

    job_id   = data.get("job_id")
    if not job_id:
        return {"status": "error", "message": "Missing job_id"}

    SUPABASE_URL      = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    OPENAI_API_KEY    = os.environ["OPENAI_API_KEY"]
    TEST_MODE         = os.environ.get("TEST_MODE", "true").lower() == "true"

    def sb_patch(ep, payload):
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/{ep}",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal"},
            json=payload, timeout=10
        )

    try:
        # Fetch job
        rows = requests.get(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
            "&select=id,topic,status,script_package,council_score,cluster",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
            timeout=10
        ).json()

        if not rows:
            return {"status": "error", "message": "Job not found"}
        job = rows[0]

        if job["status"] not in ("cbdp", "failed"):
            return {"status": "error",
                    "message": f"Job status is '{job['status']}' — only cbdp/failed can be retried"}

        script_pkg = job.get("script_package") or {}
        topic      = job.get("topic", "India Future Tech")
        script     = script_pkg.get("text", "")
        fact_pkg   = script_pkg.get("fact_anchor", {})

        if not script:
            return {"status": "error",
                    "message": "No script found — job needs full re-render, not just retry"}

        # Mark as retrying
        sb_patch(f"jobs?id=eq.{job_id}", {
            "status": "upload",
            "error": None,
            "updated_at": datetime.utcnow().isoformat()
        })
        print(f"\nCBDP RETRY: {job_id}")
        print(f"  Topic: {topic}")

        if TEST_MODE:
            sb_patch(f"jobs?id=eq.{job_id}", {
                "status": "test_complete",
                "youtube_id": "CBDP_TEST",
                "updated_at": datetime.utcnow().isoformat()
            })
            return {"status": "test_complete", "job_id": job_id}

        # ── Sanitize for YouTube ───────────────────────────────────
        import re as _re

        def sanitize(text):
            if not text: return ""
            replacements = [
                ('\u2019', "'"), ('\u2018', "'"),
                ('\u201c', '"'), ('\u201d', '"'),
                ('\u2013', '-'), ('\u2014', '-'),
                ('\u2026', '...'), ('\u00a0', ' '),
                ('\u20b9', 'Rs.'), ('\u2022', '-'),
            ]
            for bad, good in replacements:
                text = text.replace(bad, good)
            text = _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
            text = _re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)
            text = _re.sub(r'[\U0001F000-\U0001FFFF]', '', text)
            text = _re.sub(r'[\u2600-\u27BF]', '', text)
            text = _re.sub(r'[\u0900-\u097F]', '', text)
            text = _re.sub(r' {2,}', ' ', text)
            text = _re.sub(r'\n{3,}', '\n\n', text)
            return text.strip()

        # ── Generate title ────────────────────────────────────────
        import random as _rnd
        try:
            tr = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user",
                                    "content": f"Write a YouTube title for: {topic}\n"
                                               f"Rules: under 60 chars, NO emoji, plain English only.\n"
                                               f"Return ONLY the title."}],
                      "temperature": 0.9, "max_tokens": 60},
                timeout=15
            )
            raw_title = tr.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
            raw_title = _re.sub(r'[\U0001F000-\U0001FFFF]', '', raw_title)
            raw_title = _re.sub(r'[\u2600-\u27BF]', '', raw_title).strip()
            if not raw_title:
                raise ValueError("empty")
            title = raw_title[:95]
        except Exception:
            options = [
                f"India's {topic[:45]} - The Real Story",
                f"Why Nobody Is Talking About This India Story",
                f"What Is Actually Happening With {topic[:40]}",
                f"India Just Changed The Game - {topic[:35]}",
            ]
            title = _rnd.choice(options)[:95]

        print(f"  Title: {title}")

        # ── Build description ─────────────────────────────────────
        source_raw = ""
        if fact_pkg and fact_pkg.get("found"):
            src = fact_pkg.get("source", "")
            if src: source_raw = f"\nSource: {src}\n"

        description = sanitize(
            f"{script}\n\n{source_raw}"
            "India20Sixty - India's near future, explained.\n\n"
            "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
        )[:5000]

        # ── OAuth token ───────────────────────────────────────────
        tr = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id":     YOUTUBE_CLIENT_ID,
                  "client_secret":  YOUTUBE_CLIENT_SECRET,
                  "refresh_token":  YOUTUBE_REFRESH_TOKEN,
                  "grant_type":     "refresh_token"},
            timeout=10
        )
        tr.raise_for_status()
        token = tr.json()["access_token"]

        # ── We need the video file ────────────────────────────────
        # For CBDP jobs the video was rendered but cleaned up from /tmp
        # We need to get it from R2 if it was staged, or re-render minimal
        # For now: download from R2 if video_r2_url exists
        video_r2_url = job.get("video_r2_url") or ""
        video_path   = f"/tmp/cbdp_{job_id}.mp4"

        if video_r2_url and video_r2_url.startswith("http"):
            print(f"  Downloading from R2: {video_r2_url}")
            r2r = requests.get(video_r2_url, timeout=120, stream=True)
            r2r.raise_for_status()
            with open(video_path, "wb") as f:
                for chunk in r2r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"  Downloaded: {os.path.getsize(video_path)//1024}KB")
        else:
            return {"status": "error",
                    "message": "No R2 video URL found. This job needs full re-render — use Restore Failed instead."}

        # ── Upload ────────────────────────────────────────────────
        metadata = {
            "snippet": {
                "title":       title,
                "description": description,
                "tags":        ["Future India", "India innovation", "AI",
                                "Technology", "Shorts", "India2030"],
                "categoryId":  "28"
            },
            "status": {
                "privacyStatus":          "public",
                "selfDeclaredMadeForKids": False
            }
        }

        print(f"  Uploading {os.path.getsize(video_path)//1024}KB...")
        with open(video_path, "rb") as vf:
            up = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos"
                "?uploadType=multipart&part=snippet,status",
                headers={"Authorization": f"Bearer {token}"},
                files={"snippet": (None, _json.dumps(metadata), "application/json"),
                       "video":   ("video.mp4", vf, "video/mp4")},
                timeout=300
            )
        print(f"  YouTube ({up.status_code}): {up.text[:200]}")
        up.raise_for_status()
        video_id = up.json()["id"]
        print(f"  UPLOADED: https://youtube.com/watch?v={video_id}")

        sb_patch(f"jobs?id=eq.{job_id}", {
            "status":     "complete",
            "youtube_id": video_id,
            "error":      None,
            "updated_at": datetime.utcnow().isoformat()
        })

        try: os.remove(video_path)
        except Exception: pass

        return {"status": "complete", "job_id": job_id, "youtube_id": video_id,
                "url": f"https://youtube.com/watch?v={video_id}"}

    except Exception as e:
        msg = str(e)
        print(f"\nCBDP RETRY FAILED: {msg}")
        sb_patch(f"jobs?id=eq.{job_id}", {
            "status": "cbdp",
            "error":  f"Retry failed: {msg[:350]}",
            "updated_at": datetime.utcnow().isoformat()
        })
        return {"status": "error", "message": msg}



# ==========================================
# ADD VOICE TO SILENT VIDEO + PUBLISH
# ==========================================

@app.function(image=image, secrets=secrets, cpu=2.0, memory=2048, timeout=300)
@modal.fastapi_endpoint(method="POST")
def add_voice_and_publish(data: dict):
    """
    Takes an existing silent staged video from R2,
    generates ElevenLabs AI voice from the script,
    mixes voice + video, uploads to YouTube.
    
    data = { job_id: str }
    """
    import re as _re

    job_id = data.get("job_id")
    if not job_id:
        return {"status": "error", "message": "Missing job_id"}

    SUPABASE_URL          = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY     = os.environ["SUPABASE_ANON_KEY"]
    ELEVENLABS_API_KEY    = os.environ.get("ELEVENLABS_API_KEY", "")
    VOICE_ID              = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    R2_BASE_URL           = os.environ.get("R2_BASE_URL", "").rstrip("/")
    TEST_MODE             = os.environ.get("TEST_MODE", "true").lower() == "true"

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    def sb_patch(ep, payload):
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/{ep}",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal"},
            json=payload, timeout=10
        )

    def log(msg):
        print(f"[add_voice] {msg}")
        requests.post(
            f"{SUPABASE_URL}/rest/v1/render_logs",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal"},
            json={"job_id": job_id, "message": msg,
                  "created_at": datetime.utcnow().isoformat()},
            timeout=5
        )

    try:
        # ── Fetch job ──────────────────────────────────────────────
        rows = requests.get(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}"
            "&select=id,topic,status,script_package,video_r2_url,video_r2_key,council_score,cluster",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
            timeout=10
        ).json()

        if not rows:
            return {"status": "error", "message": "Job not found"}

        job        = rows[0]
        topic      = job.get("topic", "India Future Tech")
        script_pkg = job.get("script_package") or {}
        script     = script_pkg.get("text", "")
        fact_pkg   = script_pkg.get("fact_anchor", {})
        title      = script_pkg.get("title", topic[:80])
        video_url  = job.get("video_r2_url", "")

        if not script:
            return {"status": "error", "message": "No script in job — cannot generate voice"}
        if not video_url:
            return {"status": "error", "message": "No video_r2_url — no silent video to add voice to"}

        # Mark as processing
        sb_patch(f"jobs?id=eq.{job_id}", {
            "status": "voice",
            "error": None,
            "updated_at": datetime.utcnow().isoformat()
        })
        log("Starting voice generation for staged video")

        # ── Generate ElevenLabs voice ──────────────────────────────
        # Apply pronunciation fixes
        PRONUNCIATION_FIXES = [
            ("ISRO",        "I.S.R.O."),
            ("ISRO's",      "I.S.R.O.'s"),
            ("NASA",        "N.A.S.A."),
            ("DRDO",        "D.R.D.O."),
            ("AI",          "A.I."),
            ("Chandrayaan", "Chandra-yaan"),
            ("\u20b9",      "rupees "),
            ("₹",          "rupees "),
            ("crore",       "crore"),
        ]
        clean_script = script
        # Strip emotion tags
        clean_script = _re.sub(
            r'</?(?:excited|happy|sad|whisper|angry)[^>]*>', '', clean_script)
        for wrong, right in PRONUNCIATION_FIXES:
            clean_script = clean_script.replace(wrong, right)

        speech_text = clean_script.replace("...", "<break time='0.5s'/>")
        log(f"Generating voice: {len(speech_text)} chars")

        r = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
            headers={"xi-api-key": ELEVENLABS_API_KEY,
                     "Content-Type": "application/json"},
            json={
                "text": speech_text,
                "model_id": "eleven_multilingual_v2",
                "voice_settings": {
                    "stability": 0.42,
                    "similarity_boost": 0.85,
                    "style": 0.35,
                    "use_speaker_boost": True
                }
            },
            timeout=60
        )
        r.raise_for_status()

        raw_audio  = f"{TMP_DIR}/{job_id}_voice_raw.mp3"
        voice_path = f"{TMP_DIR}/{job_id}_voice.mp3"
        with open(raw_audio, "wb") as f:
            f.write(r.content)

        # Normalize audio
        subprocess.run([
            "ffmpeg", "-y", "-i", raw_audio,
            "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
            "-ar", "44100", "-ac", "1", voice_path
        ], capture_output=True, timeout=30)

        # Get voice duration
        probe = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries",
            "format=duration", "-of", "default=noprint_wrappers=1:nokey=1",
            voice_path
        ], capture_output=True, text=True, timeout=10)
        audio_dur = float(probe.stdout.strip() or "15")
        log(f"Voice: {audio_dur:.1f}s")

        # ── Download silent video from R2 ─────────────────────────
        # Build full URL if needed
        if video_url.startswith("http"):
            full_video_url = video_url
        else:
            full_video_url = f"{R2_BASE_URL}/{video_url}"

        silent_video = f"{TMP_DIR}/{job_id}_silent.mp4"
        log(f"Downloading silent video: {full_video_url[:60]}")
        r = requests.get(full_video_url, timeout=60, stream=True)
        r.raise_for_status()
        with open(silent_video, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        log(f"Silent video: {os.path.getsize(silent_video)//1024}KB")

        # ── Mix voice into video ───────────────────────────────────
        final_video = f"{TMP_DIR}/{job_id}_final.mp4"

        # Mux: loop video to match audio duration, add voice track
        result = subprocess.run([
            "ffmpeg", "-y",
            "-stream_loop", "-1", "-i", silent_video,
            "-i", voice_path,
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "128k",
            "-t", str(audio_dur),
            "-movflags", "+faststart",
            final_video
        ], capture_output=True, timeout=120)

        if result.returncode != 0:
            log(f"Mix error: {result.stderr.decode()[:200]}")
            raise Exception("ffmpeg mix failed")

        log(f"Final video: {os.path.getsize(final_video)//1024}KB")

        # ── Upload to YouTube ──────────────────────────────────────
        if TEST_MODE:
            log("TEST_MODE=true — skipping YouTube upload")
            sb_patch(f"jobs?id=eq.{job_id}", {
                "status": "complete",
                "updated_at": datetime.utcnow().isoformat()
            })
            return {"status": "test_complete", "job_id": job_id}

        # Get OAuth token
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id": YOUTUBE_CLIENT_ID,
                  "client_secret": YOUTUBE_CLIENT_SECRET,
                  "refresh_token": YOUTUBE_REFRESH_TOKEN,
                  "grant_type": "refresh_token"},
            timeout=10
        )
        print(f"OAuth: {r.status_code}: {r.text[:100]}")
        if not r.ok:
            raise Exception(f"OAuth failed: {r.text[:100]}")
        token = r.json().get("access_token")

        # Sanitize title + description
        def sanitize(text):
            if not text:
                return ""
            text = text.replace('\u2019', "'").replace('\u2018', "'")
            text = text.replace('\u201c', '"').replace('\u201d', '"')
            text = text.replace('\u2013', '-').replace('\u2014', '-')
            text = text.replace('\u2026', '...').replace('\u00a0', ' ')
            text = text.replace('\u20b9', 'Rs.').replace('₹', 'Rs.')
            text = _re.sub(r'</?(?:excited|happy|sad|whisper|angry)[^>]*>', '', text)
            text = _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
            text = _re.sub(r'[\u200b-\u200f\u202a-\u202e\ufeff]', '', text)
            text = _re.sub(u'[\U0001F000-\U0001FFFF]', '', text)
            text = _re.sub(r'[\u2600-\u26FF\u2700-\u27BF]', '', text)
            text = _re.sub(r'[\u0900-\u0D7F]', '', text)
            return text.strip()

        safe_title = sanitize(title)[:100] or f"India Future: {sanitize(topic[:60])}"
        safe_desc  = sanitize(script)[:4500]
        description = (
            f"{safe_desc}\n\n"
            "India20Sixty - India's near future, explained.\n\n"
            "#IndiaFuture #FutureTech #India #Shorts #ISRO #Technology"
        )

        metadata = {
            "snippet": {
                "title": safe_title,
                "description": description[:5000],
                "tags": ["Future India", "India", "ISRO", "Technology", "Shorts"],
                "categoryId": "28"
            },
            "status": {
                "privacyStatus": "public",
                "selfDeclaredMadeForKids": False
            }
        }

        boundary  = "india20sixty_boundary"
        meta_json = json.dumps(metadata).encode("utf-8")
        with open(final_video, "rb") as vf:
            video_bytes = vf.read()

        body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                ).encode() + meta_json
        body += (f"\r\n--{boundary}\r\nContent-Type: video/mp4\r\n\r\n").encode()
        body += video_bytes
        body += f"\r\n--{boundary}--".encode()

        log(f"Uploading {len(video_bytes)//1024}KB to YouTube...")
        r = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos"
            "?uploadType=multipart&part=snippet,status",
            headers={
                "Authorization":  f"Bearer {token}",
                "Content-Type":   f"multipart/related; boundary={boundary}",
                "Content-Length": str(len(body)),
            },
            data=body,
            timeout=300
        )
        print(f"YouTube response ({r.status_code}): {r.text[:400]}")
        r.raise_for_status()
        youtube_id = r.json()["id"]
        log(f"UPLOADED: https://youtube.com/watch?v={youtube_id}")

        # Update job
        sb_patch(f"jobs?id=eq.{job_id}", {
            "status":     "complete",
            "youtube_id": youtube_id,
            "error":      None,
            "updated_at": datetime.utcnow().isoformat()
        })

        # Cleanup
        for f in [raw_audio, voice_path, silent_video, final_video]:
            try: os.remove(f)
            except: pass

        return {"status": "published", "job_id": job_id, "youtube_id": youtube_id}

    except Exception as e:
        msg = str(e)[:400]
        log(f"ERROR: {msg}")
        sb_patch(f"jobs?id=eq.{job_id}", {
            "status": "failed",
            "error":  msg,
            "updated_at": datetime.utcnow().isoformat()
        })
        return {"status": "error", "message": msg}




@app.local_entrypoint()
def main():
    print("Running v4.0 pipeline test...")
    run_pipeline.remote(
        job_id="test-v4-001",
        topic="ISRO building Indias first space station by 2035",
        webhook_url=""
    )