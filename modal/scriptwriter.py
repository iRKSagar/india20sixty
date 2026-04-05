import modal
import os
import re
import json
import requests

# ==========================================
# MODAL APP — SCRIPTWRITER
# Handles: script generation, pronunciation fix,
#          caption extraction, mood classification
# Pure GPT calls + deterministic text ops.
# No ffmpeg. No ElevenLabs. No images.
# ==========================================

app = modal.App("india20sixty-scriptwriter")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

# ==========================================
# MOOD PRESETS REGISTRY
# Imported by renderer.py too.
# Defined here as the single source of truth.
# ==========================================

MOOD_PRESETS = {
    "cinematic_epic": {
        "label": "Cinematic Epic",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.05:rg=0.0:rb=-0.05:gr=0.0:gg=0.95:gb=0.05:br=-0.10:bg=0.03:bb=1.07",
            "eq":      "eq=contrast=1.38:brightness=-0.03:saturation=0.82",
            "sharp":   "unsharp=7:7:1.2:3:3:0.0",
            "noise":   "noise=c0s=18:c0f=t+u",
            "vignette":"vignette=angle=0.6",
        },
        "scenes": [
            {"motion_a":"diagonal_bl_tr",  "motion_b":"zoom_in_sim",   "transition":"wiperight",  "energy":"high",   "caption":"box"},
            {"motion_a":"pan_right_fast",   "motion_b":"pan_up",        "transition":"slideright", "energy":"high",   "caption":"box"},
            {"motion_a":"diagonal_br_tl",   "motion_b":"pull_back_sim", "transition":"dissolve",   "energy":"medium", "caption":"plain"},
        ],
    },
    "breaking_news": {
        "label": "Breaking News",
        "grade": {
            "ccm":     "colorchannelmixer=rr=0.90:rg=0.05:rb=0.05:gr=0.0:gg=0.95:gb=0.05:br=0.05:bg=0.08:bb=0.87",
            "eq":      "eq=contrast=1.28:brightness=0.0:saturation=0.68",
            "sharp":   "unsharp=5:5:1.1:3:3:0.0",
            "noise":   "noise=c0s=10:c0f=t+u",
            "vignette":"vignette=angle=0.45",
        },
        "scenes": [
            {"motion_a":"pan_right_fast",  "motion_b":"diagonal_tl_br", "transition":"slideleft", "energy":"high",   "caption":"box"},
            {"motion_a":"pan_left_fast",   "motion_b":"pan_up",          "transition":"wipeleft",  "energy":"high",   "caption":"box"},
            {"motion_a":"diagonal_tr_bl",  "motion_b":"static_hold",     "transition":"fadeblack", "energy":"medium", "caption":"plain"},
        ],
    },
    "hopeful_future": {
        "label": "Hopeful Future",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.08:rg=0.05:rb=-0.03:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=-0.02:bb=0.97",
            "eq":      "eq=contrast=1.12:brightness=0.04:saturation=1.45",
            "sharp":   "unsharp=3:3:0.7:3:3:0.0",
            "noise":   "noise=c0s=8:c0f=t+u",
            "vignette":"vignette=angle=0.30",
        },
        "scenes": [
            {"motion_a":"pan_right_slow",  "motion_b":"zoom_in_sim",   "transition":"dissolve", "energy":"medium", "caption":"plain"},
            {"motion_a":"diagonal_bl_tr",  "motion_b":"pan_up",        "transition":"fade",     "energy":"medium", "caption":"plain"},
            {"motion_a":"drift_slow",       "motion_b":"pull_back_sim", "transition":"dissolve", "energy":"low",    "caption":"plain"},
        ],
    },
    "dark_serious": {
        "label": "Dark Serious",
        "grade": {
            "ccm":     "colorchannelmixer=rr=0.95:rg=0.0:rb=0.05:gr=0.0:gg=0.88:gb=0.12:br=0.08:bg=0.05:bb=0.87",
            "eq":      "eq=contrast=1.45:brightness=-0.06:saturation=0.52",
            "sharp":   "unsharp=7:7:1.0:3:3:0.0",
            "noise":   "noise=c0s=24:c0f=t+u",
            "vignette":"vignette=angle=0.70",
        },
        "scenes": [
            {"motion_a":"drift_slow",       "motion_b":"pan_left_slow",  "transition":"fadeblack", "energy":"low",  "caption":"box"},
            {"motion_a":"diagonal_tr_bl",   "motion_b":"static_hold",    "transition":"dissolve",  "energy":"low",  "caption":"box"},
            {"motion_a":"pan_up",           "motion_b":"pull_back_sim",  "transition":"fade",      "energy":"low",  "caption":"plain"},
        ],
    },
    "cold_tech": {
        "label": "Cold Tech",
        "grade": {
            "ccm":     "colorchannelmixer=rr=0.88:rg=0.05:rb=0.07:gr=-0.03:gg=0.95:gb=0.08:br=0.0:bg=0.05:bb=1.15",
            "eq":      "eq=contrast=1.22:brightness=0.0:saturation=0.88",
            "sharp":   "unsharp=5:5:1.0:3:3:0.0",
            "noise":   "noise=c0s=12:c0f=t+u",
            "vignette":"vignette=angle=0.42",
        },
        "scenes": [
            {"motion_a":"diagonal_tl_br",  "motion_b":"zoom_in_sim",   "transition":"slideleft", "energy":"medium", "caption":"box"},
            {"motion_a":"pan_right_fast",   "motion_b":"diagonal_br_tl","transition":"wipeleft",  "energy":"medium", "caption":"box"},
            {"motion_a":"pull_back_sim",    "motion_b":"drift_slow",    "transition":"dissolve",  "energy":"low",    "caption":"plain"},
        ],
    },
    "vibrant_pop": {
        "label": "Vibrant Pop",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.05:rg=0.0:rb=0.0:gr=0.05:gg=1.08:gb=0.0:br=0.0:bg=0.0:bb=1.05",
            "eq":      "eq=contrast=1.08:brightness=0.06:saturation=1.72",
            "sharp":   "unsharp=3:3:0.6:3:3:0.0",
            "noise":   "noise=c0s=6:c0f=t+u",
            "vignette":"vignette=angle=0.22",
        },
        "scenes": [
            {"motion_a":"diagonal_tl_br",  "motion_b":"diagonal_br_tl", "transition":"wiperight",  "energy":"high",   "caption":"box"},
            {"motion_a":"pan_right_fast",   "motion_b":"zoom_in_sim",    "transition":"slideright", "energy":"high",   "caption":"box"},
            {"motion_a":"diagonal_bl_tr",   "motion_b":"pan_up",         "transition":"dissolve",   "energy":"medium", "caption":"plain"},
        ],
    },
    "nostalgic_film": {
        "label": "Nostalgic Film",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.12:rg=0.05:rb=-0.08:gr=0.05:gg=1.0:gb=-0.05:br=-0.03:bg=0.0:bb=0.93",
            "eq":      "eq=contrast=1.18:brightness=0.03:saturation=1.12",
            "sharp":   "unsharp=3:3:0.5:3:3:0.0",
            "noise":   "noise=c0s=26:c0f=t+u",
            "vignette":"vignette=angle=0.65",
        },
        "scenes": [
            {"motion_a":"pan_right_slow",   "motion_b":"drift_slow",    "transition":"dissolve", "energy":"low",    "caption":"plain"},
            {"motion_a":"diagonal_bl_tr",   "motion_b":"pan_up",        "transition":"fade",     "energy":"medium", "caption":"plain"},
            {"motion_a":"zoom_in_sim",       "motion_b":"static_hold",   "transition":"dissolve", "energy":"low",    "caption":"plain"},
        ],
    },
    "warm_human": {
        "label": "Warm Human",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.10:rg=0.05:rb=-0.05:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=0.0:bb=0.95",
            "eq":      "eq=contrast=1.10:brightness=0.05:saturation=1.32",
            "sharp":   "unsharp=3:3:0.5:3:3:0.0",
            "noise":   "noise=c0s=8:c0f=t+u",
            "vignette":"vignette=angle=0.28",
        },
        "scenes": [
            {"motion_a":"pan_right_slow",  "motion_b":"zoom_in_sim",   "transition":"dissolve", "energy":"low", "caption":"plain"},
            {"motion_a":"drift_slow",       "motion_b":"pan_up",        "transition":"fade",     "energy":"low", "caption":"plain"},
            {"motion_a":"static_hold",      "motion_b":"pull_back_sim", "transition":"dissolve", "energy":"low", "caption":"plain"},
        ],
    },
}

CLUSTER_MOOD_DEFAULTS = {
    "Space":     "cinematic_epic",
    "DeepTech":  "cold_tech",
    "AI":        "cold_tech",
    "Gadgets":   "vibrant_pop",
    "GreenTech": "hopeful_future",
    "Startups":  "hopeful_future",
}


# ==========================================
# MAIN SCRIPTWRITER FUNCTION
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=90)
def run_scriptwriter(job_id: str, topic: str, fact_package: dict, cluster: str, subscribe_cta: bool = False) -> dict:
    """
    Generate script, fix pronunciation, extract captions, classify mood.
    Returns:
    {
        script, reviewed_script, script_lines,
        captions, mood, mood_label, scene_prompts
    }
    """
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    print(f"\n[Scriptwriter] job={job_id} cluster={cluster} cta={subscribe_cta}")

    script, script_lines   = _generate_script(OPENAI_API_KEY, topic, fact_package, subscribe_cta)
    reviewed_script        = _pronunciation_fix(script)
    captions               = _extract_captions(OPENAI_API_KEY, script_lines)
    mood                   = _mood_classifier(OPENAI_API_KEY, script, cluster)
    scene_prompts          = _generate_scene_prompts(OPENAI_API_KEY, topic, fact_package)

    return {
        "script":          script,
        "reviewed_script": reviewed_script,
        "script_lines":    script_lines,
        "captions":        captions,
        "mood":            mood,
        "mood_label":      MOOD_PRESETS[mood]["label"],
        "scene_prompts":   scene_prompts,
    }


# ==========================================
# INTERNAL FUNCTIONS
# ==========================================

def _generate_script(api_key: str, topic: str, fact_package: dict, subscribe_cta: bool) -> tuple:
    fact_section = ""
    if fact_package and fact_package.get("found"):
        fact_section = f"\nREAL FACT ANCHOR:\nFact: {fact_package['key_fact']}\nSource: {fact_package['source']}"

    from datetime import datetime as _dt
    today = _dt.utcnow().strftime("%B %Y")  # e.g. "April 2026"
    cta = ""  # NO CTA ever — Shorts must never include subscribe prompts

    prompt = f"""Write a YouTube Shorts voiceover script for India20Sixty — India's near future channel.
Today is {today}. Treat events before {today} as past history — use past tense for them.

Topic: {topic}
{fact_section}

STRICT RULES:
- Between 48 and 55 words total. Count every word carefully. Must be 48-55. Too short = rejected.
- Indian English. Clear, confident, modern. Not American or British.
- NO Hindi words. NO Hinglish. Pure English only.
- Max 12 words per sentence.
- Open with a fact that stops the scroll.
- Use correct tense — past for what already happened, future for what is coming.
- NEVER start with "Fact:" — state facts directly.
- NO subscribe CTA. NO "Follow us". End sentence 6 with a debate question ONLY.

6 sentences (each 8-10 words):
1. Hook — the real fact or number
2. What is happening right now in India
3. The scale — money, reach, jobs, impact
4. What this means for ordinary Indians
5. The honest challenge or twist
6. One debate question to drive comments

Return ONLY the script as plain text. No labels. No JSON. No numbering."""

    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.75, "max_tokens": 300},
                timeout=30,
            )
            r.raise_for_status()
            raw   = r.json()["choices"][0]["message"]["content"].strip()
            lines = [re.sub(r"^[\d]+[.)]\s*|^[-\u2022]\s*", "", l.strip())
                     for l in raw.split("\n") if l.strip()]
            script = " ".join(lines)
            word_count = len(script.split())
            print(f"  Script attempt {attempt+1} ({word_count} words): {script[:80]}...")
            if word_count >= 45:
                return script, lines
            print(f"  Script too short ({word_count} words) — retrying")
        except Exception as e:
            print(f"  Script attempt {attempt+1} failed: {e}")

    # All attempts failed or too short — use guaranteed 52-word fallback
    fallback = (
        f"India just crossed a major milestone with {topic}. "
        f"This is not a future plan — the work is happening right now. "
        f"Billions have been committed and a hard deadline is locked in. "
        f"Hundreds of thousands of skilled jobs are being created across India. "
        f"The biggest challenge is not money or talent — it is execution speed. "
        f"Will India deliver on time and set a new global standard?"
    )
    print(f"  Using fallback script ({len(fallback.split())} words)")
    return fallback, [fallback]


def _pronunciation_fix(script: str) -> str:
    """Pure deterministic find-and-replace. NO GPT. NO rewriting."""
    fixed = script

    acronyms = [
        ("ISRO",    "I.S.R.O."), ("DRDO",    "D.R.D.O."),
        ("DRDO's",  "D.R.D.O.'s"), ("ISRO's", "I.S.R.O.'s"),
        ("IIT",     "I.I.T."), ("IITs",    "I.I.T.s"),
        ("IIM",     "I.I.M."), ("AIIMS",   "A.I.I.M.S."),
        ("UPI",     "U.P.I."), ("NDTV",    "N.D.T.V."),
        ("NASSCOM", "NAS-com"), ("SEBI",    "SEE-bi"),
    ]
    for wrong, right in acronyms:
        fixed = fixed.replace(wrong, right)

    missions = [
        ("Chandrayaan", "Chandra-yaan"), ("Gaganyaan",  "Gagan-yaan"),
        ("Mangalyaan",  "Mangal-yaan"),  ("Aditya-L1",  "Aditya L-one"),
    ]
    for wrong, right in missions:
        fixed = fixed.replace(wrong, right)

    fixed = fixed.replace("\u20b9", "rupees ").replace("%", " percent")
    fixed = fixed.replace("&", " and ").replace("\u2192", " to ").replace("~", " approximately ")

    # Indian number formats
    fixed = re.sub(r"(\d+),00,00,000", lambda m: m.group(1) + " crore", fixed)
    fixed = re.sub(r"(\d+),00,000",    lambda m: m.group(1) + " lakh",  fixed)
    fixed = re.sub(r"(\d+),000",       lambda m: m.group(1) + " thousand", fixed)

    # Add emotion tags at sentence boundaries only
    sentences = fixed.split(". ")
    if sentences and any(c.isdigit() for c in sentences[0]):
        sentences[0] = "<excited>" + sentences[0] + "</excited>"
    if len(sentences) >= 2 and sentences[-1].strip().endswith("?"):
        sentences[-1] = "<happy>" + sentences[-1].strip() + "</happy>"
    fixed = ". ".join(sentences)

    print(f"  Pronunciation fix done: {fixed[:80]}...")
    return fixed


def _extract_captions(api_key: str, script_lines: list) -> list:
    full  = " ".join(script_lines)
    clean = re.sub(r"<[^>]+>", "", full)
    prompt = f"""Extract exactly 9 caption phrases from this script.
Rules: 3-5 words each, ALL CAPS, punchy, in order, no punctuation except ! or ?
Output exactly 9 lines, one phrase per line only.
Script: {clean}"""
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 200},
            timeout=20,
        )
        r.raise_for_status()
        raw      = r.json()["choices"][0]["message"]["content"].strip()
        captions = [re.sub(r"^[\d]+[.)]\s*", "", l.strip()).upper()
                    for l in raw.split("\n") if l.strip()][:9]
        while len(captions) < 9:
            captions.append(captions[-1] if captions else "INDIA KA FUTURE")
        print(f"  Captions: {captions[:3]}...")
        return captions
    except Exception as e:
        print(f"  Caption failed: {e}")
        words = clean.upper().split()
        caps, step = [], max(1, len(words) // 9)
        for i in range(9):
            chunk = words[i * step: i * step + 4]
            caps.append(" ".join(chunk) if chunk else "INDIA KA FUTURE")
        return caps[:9]


def _mood_classifier(api_key: str, script: str, cluster: str) -> str:
    """
    GPT picks ONE of 8 mood keys. Nothing else.
    Returns a valid key from MOOD_PRESETS.
    Falls back to cluster default if GPT fails.
    """
    clean = re.sub(r"<[^>]+>", "", script).strip()
    prompt = f"""Read this script and pick ONE mood that best matches.

MOODS:
cinematic_epic   — powerful, dramatic, milestone achievement, space/defence/scale
breaking_news    — urgent, shocking fact, current event, time-sensitive
hopeful_future   — optimistic, new beginning, innovation solving problems
dark_serious     — challenge, warning, problem, crisis, difficult truth
cold_tech        — analytical, data-driven, AI/chips/infrastructure, precise
vibrant_pop      — exciting, consumer, youth energy, product launch, fun
nostalgic_film   — cultural pride, heritage meets future, emotional journey
warm_human       — people-first, healthcare/education, community, empathy

SCRIPT:
{clean}

CLUSTER: {cluster}

Return ONLY the mood key. One word. No explanation. No quotes."""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.3, "max_tokens": 20},
            timeout=15,
        )
        r.raise_for_status()
        raw  = r.json()["choices"][0]["message"]["content"].strip().lower()
        mood = raw.strip().replace('"', "").replace("'", "").split()[0]
        if mood in MOOD_PRESETS:
            print(f"  Mood: {mood} [{MOOD_PRESETS[mood]['label']}]")
            return mood
        # Partial match fallback
        for key in MOOD_PRESETS:
            if key in mood or mood in key:
                print(f"  Mood (partial match): {key}")
                return key
    except Exception as e:
        print(f"  Mood classifier failed: {e}")

    fallback = CLUSTER_MOOD_DEFAULTS.get(cluster, "hopeful_future")
    print(f"  Mood (cluster fallback): {fallback}")
    return fallback


# ── VISUAL STYLES ───────────────────────────────────────────────
_VISUAL_STYLES = [
    "natural daylight photography, modern Indian city, sharp and clean, photorealistic",
    "soft indoor lighting, Indian tech office or lab, contemporary professional setting",
    "overcast natural light, Indian urban street level, authentic and grounded",
    "blue hour city lights, Indian metro or tech park, cool tones, sharp focus",
    "bright morning light, Indian campus or research facility, optimistic and clean",
    "dramatic overcast sky, god rays through clouds, Indian landscape, natural colors",
    "close-up editorial photography, shallow depth of field, Indian faces, soft bokeh",
    "aerial drone perspective, sweeping wide angle, Indian cityscape, natural colors",
    "futuristic neon-lit Indian megacity, rain-slicked streets, cool blues and purples",
    "vibrant street-level India, mix of ancient and ultra-modern, people of all ages",
    "medium shot Indian engineers or scientists in action, modern facility, natural light",
    "hyperrealistic Indian professional in contemporary setting, clean background, sharp",
]

_SHOT_TYPES = [
    ["extreme wide establishing shot", "dramatic low angle hero shot", "sweeping panoramic view", "epic aerial wide shot"],
    ["medium shot of Indian engineers in action", "intimate human-scale scene", "detailed view of Indian technology", "focused mid-shot with Indian faces"],
    ["soaring aerial overview", "wide hopeful scene of India's future", "morning light wide shot of Indian achievement", "emotional close-up of Indian faces"],
]

_FALLBACKS = [
    "photorealistic modern Indian city skyline, natural daylight, glass towers and infrastructure",
    "Indian scientists in contemporary clothing, modern research campus, clean professional setting",
    "aerial view of green India, solar farms and villages, natural colors, morning light",
]


def _generate_scene_prompts(api_key: str, topic: str, fact_package: dict) -> list:
    import random
    fact_hint = ""
    if fact_package and fact_package.get("found"):
        fact_hint = f"\nReal context: {fact_package.get('key_fact', '')}"

    style1 = random.choice(_VISUAL_STYLES)
    style2 = random.choice([s for s in _VISUAL_STYLES if s != style1])
    shot1  = random.choice(_SHOT_TYPES[1])
    shot2  = random.choice(_SHOT_TYPES[2])

    hook_prompt = None
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content":
                    f"""Create ONE striking image prompt for a YouTube Short hook frame.
Channel: India20Sixty — India's near future (tech, space, innovation, startups)
Topic: "{topic}"{fact_hint}

CRITICAL — show THE SPECIFIC SUBJECT of this topic:
- If topic is about cars/EVs → show cars, roads, EV charging, NOT offices
- If topic is about space/rockets → show launch pad, rocket, satellite, NOT generic city
- If topic is about solar/green → show solar panels, installations, NOT engineers at desks
- If topic is about AI/software → THEN show engineers at computers
- Always: Indian faces, Indian setting, natural daylight, photorealistic, no text

Return ONLY the image prompt as one sentence under 100 characters."""}],
                  "temperature": 0.9, "max_tokens": 200},
            timeout=20,
        )
        r.raise_for_status()
        hook_prompt = r.json()["choices"][0]["message"]["content"].strip().strip('"')
        print(f"  Hook prompt: {hook_prompt[:60]}...")
    except Exception as e:
        print(f"  Hook prompt failed: {e}")
        hook_prompt = f"Photorealistic modern India, {topic}, Indian faces and technology, natural daylight, sharp focus"

    scene_2_3 = None
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini",
                  "messages": [{"role": "user", "content":
                    f"""Create 2 image prompts for a YouTube Short about: "{topic}"{fact_hint}

CRITICAL — both prompts must show the ACTUAL SUBJECT of this topic (not generic offices):
- Cars/EVs → cars, charging, roads
- Space → rockets, satellites, launch pads  
- Solar/Green → panels, farms, installations
- AI → engineers at computers (only for AI topics)
- Startups → founders, pitch rooms, startup offices

Scene 2 (detail/insight): close-up of the specific technology or key moment, Indian setting
Scene 3 (wide/hopeful): wide optimistic shot related to this topic, India scale, natural daylight

Return ONLY: ["scene2_prompt_under_80chars", "scene3_prompt_under_80chars"]"""}],
                  "temperature": 0.9, "max_tokens": 250},
            timeout=20,
        )
        r.raise_for_status()
        content   = r.json()["choices"][0]["message"]["content"].strip()
        scene_2_3 = json.loads(content[content.find("["):content.rfind("]") + 1])
    except Exception as e:
        print(f"  Scene prompts 2+3 failed: {e}")
        scene_2_3 = [_FALLBACKS[1], _FALLBACKS[2]]

    return [
        hook_prompt,
        scene_2_3[0] if scene_2_3 else _FALLBACKS[1],
        scene_2_3[1] if scene_2_3 and len(scene_2_3) > 1 else _FALLBACKS[2],
    ]


@app.local_entrypoint()
def main():
    result = run_scriptwriter.remote(
        job_id="test-001",
        topic="ISRO space station India 2035",
        fact_package={"found": True, "headline": "ISRO announces 2035 space station", "source": "PIB", "key_fact": "India will launch its own space station by 2035"},
        cluster="Space",
        subscribe_cta=False,
    )
    print("Script:", result["script"])
    print("Mood:", result["mood"])
    print("Captions:", result["captions"][:3])