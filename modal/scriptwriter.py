import modal
import os
import re
import json
import random
import requests

# ==========================================
# MODAL APP — SCRIPTWRITER
# GPT calls only. No ffmpeg. No image work.
# Returns everything the renderer needs:
#   script, reviewed_script, captions, mood, scene_prompts
# ==========================================

app = modal.App("india20sixty-scriptwriter")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

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
    "hyperrealistic Indian scientist or engineer, modern lab with traditional motifs, ARRI cinematic",
]

SHOT_TYPES = [
    ["extreme wide establishing shot of Indian city or landscape",
     "dramatic low angle hero shot of Indian technology",
     "sweeping panoramic view of India from above"],
    ["medium shot of Indian engineers or workers in action",
     "intimate human-scale scene in Indian context",
     "detailed view of Indian technology or infrastructure"],
    ["soaring aerial overview of transformed Indian landscape",
     "golden hour wide shot of Indian achievement",
     "emotional cinematic close-up of Indian people"],
]

SCENE_FALLBACKS = [
    "futuristic Indian megacity at golden hour, lotus-shaped towers, electric air taxis, cinematic",
    "Indian scientists in smart attire, holographic displays, IIT-style campus, temple meets lab",
    "aerial green India, solar farms, rural fibre internet, diverse communities, hopeful sunrise",
]

MOOD_VALID = {
    "cinematic_epic", "breaking_news", "hopeful_future", "dark_serious",
    "cold_tech", "vibrant_pop", "nostalgic_film", "warm_human",
}

CLUSTER_MOOD_DEFAULTS = {
    "Space":     "cinematic_epic",
    "DeepTech":  "cold_tech",
    "AI":        "cold_tech",
    "Gadgets":   "vibrant_pop",
    "GreenTech": "hopeful_future",
    "Startups":  "hopeful_future",
}


@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=90)
def run_scriptwriter(
    topic:         str,
    cluster:       str,
    fact_package:  dict,
    subscribe_cta: bool = False,
) -> dict:
    """
    Returns {
        script, reviewed_script, script_lines,
        captions, mood, mood_label, scene_prompts
    }
    """
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

    # ── 1. SCRIPT ─────────────────────────────────────────────────
    fact_section = ""
    if fact_package and fact_package.get("found"):
        fact_section = f"\nREAL FACT ANCHOR:\nFact: {fact_package['key_fact']}\nSource: {fact_package['source']}"

    cta = "\n- End with exactly: Follow India20Sixty for daily India tech updates." if subscribe_cta else ""

    prompt = f"""Write a YouTube Shorts voiceover script for India20Sixty — India's near future channel.

Topic: {topic}
{fact_section}

STRICT RULES:
- Maximum 55 words total. Count every word.
- Language: Indian English. Direct, warm, modern. NOT American or British tone.
- NO Hindi words. NO Hinglish. Pure English only.
- Every sentence max 12 words.
- Open with a fact that stops the scroll.
- NEVER start with "Fact:" — state the fact directly.{cta}

6 sentences:
1. Hook — real fact or number, make it land hard
2. What is happening right now in India
3. The scale — money, reach, jobs, impact
4. What this means for ordinary Indians
5. The honest challenge or twist
6. One debate question to drive comments

Return ONLY the script as plain text. No labels. No JSON."""

    script = ""
    script_lines = []
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.75, "max_tokens": 200},
            timeout=30,
        )
        r.raise_for_status()
        raw          = r.json()["choices"][0]["message"]["content"].strip()
        script_lines = [re.sub(r'^[\d]+[.)]\s*|^[-•]\s*', '', l.strip())
                        for l in raw.split('\n') if l.strip()]
        script = ' '.join(script_lines)
        print(f"  Script ({len(script.split())}w): {script[:80]}...")
    except Exception as e:
        print(f"  Script failed: {e}")
        script = (f"India is building something that will change everything. "
                  f"{topic} is no longer a dream — work has already started. "
                  f"The government has committed serious money and a real deadline. "
                  f"Thousands of skilled jobs will follow. "
                  f"But execution is the real test. Will India deliver on time?")
        script_lines = [script]

    # ── 2. PRONUNCIATION FIX ─────────────────────────────────────
    # Pure deterministic find-and-replace. NO GPT. NO rewriting.
    fixed = script
    for wrong, right in [
        ("ISRO",       "I.S.R.O."), ("ISRO's",      "I.S.R.O.'s"),
        ("DRDO",       "D.R.D.O."), ("DRDO's",      "D.R.D.O.'s"),
        ("IIT",        "I.I.T."),   ("IITs",         "I.I.T.s"),
        ("IIM",        "I.I.M."),   ("AIIMS",        "A.I.I.M.S."),
        ("UPI",        "U.P.I."),   ("NASSCOM",      "NAS-com"),
        ("SEBI",       "SEE-bi"),
        ("Chandrayaan","Chandra-yaan"), ("Gaganyaan",  "Gagan-yaan"),
        ("Mangalyaan", "Mangal-yaan"), ("Aditya-L1",  "Aditya L-one"),
        ("\u20b9",     "rupees "),  ("%",            " percent"),
        ("&",          " and "),    ("\u2192",       " to "),
    ]:
        fixed = fixed.replace(wrong, right)

    # Indian number formats
    fixed = re.sub(r'(\d+),00,00,000', lambda m: m.group(1)+' crore', fixed)
    fixed = re.sub(r'(\d+),00,000',    lambda m: m.group(1)+' lakh',  fixed)
    fixed = re.sub(r'(\d+),000',       lambda m: m.group(1)+' thousand', fixed)

    # Emotion tags at sentence boundaries only
    sentences = fixed.split('. ')
    if len(sentences) >= 1 and any(c.isdigit() for c in sentences[0]):
        sentences[0] = '<excited>' + sentences[0] + '</excited>'
    if len(sentences) >= 2 and sentences[-1].strip().endswith('?'):
        sentences[-1] = '<happy>' + sentences[-1].strip() + '</happy>'
    reviewed_script = '. '.join(sentences)
    print(f"  Reviewed: {reviewed_script[:100]}...")

    # ── 3. CAPTIONS ───────────────────────────────────────────────
    clean_for_captions = re.sub(r'<[^>]+>', '', script)
    cap_prompt = f"""Extract exactly 9 caption phrases from this script.
Rules: 3-5 words each, ALL CAPS, punchy, in order, no punctuation except ! or ?
Output exactly 9 lines, one phrase per line only.
Script: {clean_for_captions}"""

    captions = []
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": cap_prompt}],
                  "temperature": 0.3, "max_tokens": 200},
            timeout=20,
        )
        r.raise_for_status()
        raw      = r.json()["choices"][0]["message"]["content"].strip()
        captions = [re.sub(r'^[\d]+[.)]\s*', '', l.strip()).upper()
                    for l in raw.split('\n') if l.strip()][:9]
        while len(captions) < 9:
            captions.append(captions[-1] if captions else "INDIA KA FUTURE")
    except Exception as e:
        print(f"  Captions failed: {e}")
        words = clean_for_captions.upper().split()
        step  = max(1, len(words) // 9)
        captions = [' '.join(words[i*step: i*step+4]) or "INDIA KA FUTURE" for i in range(9)]
    print(f"  Captions: {captions}")

    # ── 4. MOOD CLASSIFIER ────────────────────────────────────────
    clean_script = re.sub(r'<[^>]+>', '', script).strip()
    mood_prompt  = f"""Read this script and pick ONE mood that best matches.

MOODS:
cinematic_epic   — powerful, dramatic, milestone, space/defence/scale
breaking_news    — urgent, shocking fact, current event, time-sensitive
hopeful_future   — optimistic, innovation solving problems, new beginning
dark_serious     — challenge, warning, crisis, difficult truth
cold_tech        — analytical, data-driven, AI/chips/infrastructure
vibrant_pop      — exciting, consumer, youth energy, product launch
nostalgic_film   — cultural pride, heritage meets future, emotional
warm_human       — people-first, healthcare/education, community

SCRIPT: {clean_script}
CLUSTER: {cluster}

Return ONLY the mood key. Nothing else. No explanation."""

    mood = CLUSTER_MOOD_DEFAULTS.get(cluster, "hopeful_future")
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": mood_prompt}],
                  "temperature": 0.3, "max_tokens": 20},
            timeout=15,
        )
        r.raise_for_status()
        raw_mood = r.json()["choices"][0]["message"]["content"].strip().lower()
        raw_mood = raw_mood.replace('"','').replace("'",'').split()[0]
        if raw_mood in MOOD_VALID:
            mood = raw_mood
        else:
            for key in MOOD_VALID:
                if key in raw_mood or raw_mood in key:
                    mood = key
                    break
    except Exception as e:
        print(f"  Mood classifier failed ({e}), using cluster default: {mood}")
    print(f"  Mood: {mood}")

    # ── 5. SCENE PROMPTS ──────────────────────────────────────────
    style_a   = random.choice(VISUAL_STYLES)
    style_b   = random.choice([s for s in VISUAL_STYLES if s != style_a])
    shot_mid  = random.choice(SHOT_TYPES[1])
    shot_end  = random.choice(SHOT_TYPES[2])
    fact_hint = f"\nReal context: {fact_package.get('key_fact','')}" if fact_package and fact_package.get("found") else ""

    hook_prompt = f"""Create ONE ultra-dramatic showstopper image prompt for a YouTube Short hook frame.

Channel: India20Sixty — India's near future
Topic: "{topic}"{fact_hint}

REQUIREMENTS:
- Unmistakably Indian — Indian faces, architecture, landscape, or technology
- ONE dominant subject filling 70% of frame
- Extreme contrast — old India vs new India, or dramatic futuristic scene
- Hyperrealistic ARRI Alexa cinematic quality, 8K, film grain

Return ONLY the image prompt as a single string. No labels."""

    scenes_prompt = f"""Create 2 cinematic image prompts for an Indian tech/innovation YouTube Short.

Topic: "{topic}"{fact_hint}
Scene 2 (Insight): {style_a}, {shot_mid} — technology IN Indian context
Scene 3 (Ending): {style_b}, {shot_end} — wide, hopeful, emotional

Both must feel distinctly Indian. Return ONLY: ["scene2_prompt", "scene3_prompt"]"""

    hook = None
    scene_2_3 = None

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": hook_prompt}],
                  "temperature": 0.95, "max_tokens": 200},
            timeout=20,
        )
        r.raise_for_status()
        hook = r.json()["choices"][0]["message"]["content"].strip().strip('"')
        print(f"  Hook: {hook[:70]}...")
    except Exception as e:
        print(f"  Hook prompt failed: {e}")
        hook = (f"Extreme cinematic contrast — crumbling old India vs gleaming futuristic India, "
                f"{topic} transformation, Indian engineers at work, ARRI lighting, saffron sky, 8K")

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": scenes_prompt}],
                  "temperature": 0.9, "max_tokens": 250},
            timeout=20,
        )
        r.raise_for_status()
        content   = r.json()["choices"][0]["message"]["content"].strip()
        scene_2_3 = json.loads(content[content.find('['):content.rfind(']')+1])
    except Exception as e:
        print(f"  Scene 2+3 failed: {e}")
        scene_2_3 = [SCENE_FALLBACKS[1], SCENE_FALLBACKS[2]]

    scene_prompts = [
        hook,
        scene_2_3[0] if scene_2_3 else SCENE_FALLBACKS[1],
        scene_2_3[1] if scene_2_3 and len(scene_2_3) > 1 else SCENE_FALLBACKS[2],
    ]

    return {
        "script":          script,
        "reviewed_script": reviewed_script,
        "script_lines":    script_lines,
        "captions":        captions,
        "mood":            mood,
        "mood_label":      mood.replace("_", " ").title(),
        "scene_prompts":   scene_prompts,
    }
