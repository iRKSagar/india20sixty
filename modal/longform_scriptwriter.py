import modal
import os
import re
import json
import requests

app = modal.App("india20sixty-longform-scriptwriter")
image = (modal.Image.debian_slim(python_version="3.11").pip_install("requests","fastapi[standard]"))
secrets = [modal.Secret.from_name("india20sixty-secrets")]

# ============================================================
# SEGMENT STRUCTURE
# ============================================================
SEGMENT_TYPES = [
    {"type":"hook",         "label":"Hook",            "duration_target":30,  "word_target":55,
     "description":"Opens mid-story. The most startling fact or moment. Viewer must want to know what happens next.",
     "caption_style":"large",    "transition_out":"hard_cut"},
    {"type":"context",      "label":"The Setup",       "duration_target":90,  "word_target":160,
     "description":"How did we get here. The history, the stakes, the characters. Builds genuine investment.",
     "caption_style":"subtitle", "transition_out":"dissolve"},
    {"type":"deepdive",     "label":"The Story",       "duration_target":150, "word_target":270,
     "description":"The full narrative. Specific people, specific numbers, specific moments. The story nobody else told.",
     "caption_style":"subtitle", "transition_out":"dissolve"},
    {"type":"implications", "label":"What Changes",    "duration_target":90,  "word_target":160,
     "description":"Concrete impact on real Indians. Not abstract — jobs, prices, daily life, opportunities.",
     "caption_style":"subtitle", "transition_out":"fade"},
    {"type":"challenge",    "label":"The Hard Truth",  "duration_target":60,  "word_target":110,
     "description":"What could go wrong. What is being ignored. Said like a friend being honest, not a disclaimer.",
     "caption_style":"subtitle", "transition_out":"fadeblack"},
    {"type":"payoff",       "label":"The Bigger Picture","duration_target":60, "word_target":110,
     "description":"Zoom out. India in the world. Makes the viewer feel part of something larger. End with CTA.",
     "caption_style":"large",    "transition_out":"fade"},
]

SEGMENT_IMAGE_COUNTS = {"hook":1,"context":2,"deepdive":3,"implications":2,"challenge":1,"payoff":1}

VISUAL_CONTEXT = {
    "hook":        "ultra-dramatic single moment, one person or object 70% of frame, extreme contrast, natural dramatic lighting",
    "context":     "wide establishing shot, Indian setting, documentary photorealistic, natural daylight, sense of scale",
    "deepdive":    "technical detail, Indian infrastructure or technology in use, people working, showing real process",
    "implications":"ordinary Indians in daily life, human scale, hopeful but grounded, natural light",
    "challenge":   "tension, honest darker tone, overcast or harsh indoor light, problem clearly visible",
    "payoff":      "wide optimistic India landscape or skyline, morning light, sense of possibility",
}

_BAD = ["ARRI","8K","8k","golden hour","saffron","ochre","warm palette","marigold","anamorphic","HDR","cinematic"]

def _sanitize(p):
    for bad in _BAD:
        p = p.replace(bad,"").replace(bad.lower(),"")
    return re.sub(r"  +"," ",p).strip()

MOOD_KEYS = ["cinematic_epic","breaking_news","hopeful_future","dark_serious","cold_tech","vibrant_pop","nostalgic_film","warm_human"]
CLUSTER_MOODS = {"Space":"cinematic_epic","DeepTech":"cold_tech","AI":"cold_tech","Gadgets":"vibrant_pop","GreenTech":"hopeful_future","Startups":"hopeful_future"}


@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=300)
def generate_longform_script(job_id: str, topic: str, cluster: str, target_duration: int, fact_package: dict = None) -> dict:
    """
    Two-pass scriptwriter:
    Pass 1 — generate the full story arc (central question, narrative beats, key facts, character)
    Pass 2 — write each segment FROM that arc, so every segment connects
    """
    api_key = os.environ["OPENAI_API_KEY"]
    print(f"\n[Longform Script] {job_id} | {topic[:60]} | {target_duration}s")

    # ── PASS 1: STORY ARC ──────────────────────────────────────
    arc = _gen_story_arc(api_key, topic, cluster, fact_package)
    print(f"  Arc: question='{arc.get('central_question','')[:60]}'")
    print(f"  Hook beat: {arc.get('hook_beat','')[:60]}")

    scale    = target_duration / sum(s["duration_target"] for s in SEGMENT_TYPES)
    segments = []

    # ── PASS 2: WRITE EACH SEGMENT FROM THE ARC ────────────────
    for i, st in enumerate(SEGMENT_TYPES):
        scaled_dur   = int(st["duration_target"] * scale)
        scaled_words = int(st["word_target"] * scale)
        script       = _gen_segment_from_arc(api_key, topic, cluster, st, scaled_words, arc, i)
        img_prompts  = _gen_image_prompts(api_key, topic, cluster, st, script, SEGMENT_IMAGE_COUNTS.get(st["type"],2))
        segments.append({
            "segment_idx":    i,
            "type":           st["type"],
            "label":          st["label"],
            "script":         script,
            "duration_target":scaled_dur,
            "word_count":     len(script.split()),
            "image_prompts":  img_prompts,
            "caption_style":  st["caption_style"],
            "transition_out": st["transition_out"],
        })
        print(f"  [{st['label']}] {len(script.split())}w | {scaled_dur}s")

    mood = _classify_mood(api_key, segments[0]["script"] + " " + segments[2]["script"], cluster)
    print(f"  Mood: {mood}")
    return {
        "segments":       segments,
        "total_duration": sum(s["duration_target"] for s in segments),
        "mood":           mood,
        "story_arc":      arc,
    }


def _gen_story_arc(api_key, topic, cluster, fact_package):
    """
    Pass 1: Generate the narrative skeleton before writing a single word of narration.
    """
    from datetime import datetime as _dt
    today = _dt.utcnow().strftime("%B %Y")
    fact_hint = f"\nReal fact to anchor the story: {fact_package['key_fact']}" if (fact_package and fact_package.get("found")) else ""

    prompt = f"""You are a documentary story editor for India20Sixty — a YouTube channel about India's near future.
Today is {today}. Treat anything before {today} as past history — not upcoming news.

TOPIC: {topic}
CLUSTER: {cluster}{fact_hint}

Before writing any narration, design the story architecture. Think like a documentary director.

Return ONLY valid JSON:
{{
  "central_question": "The one question this video answers. Must make a viewer curious. Not 'what is X' but 'why does X keep failing/winning/changing everything'.",
  "hook_beat": "The single most startling moment or fact. Opens mid-story. Something the viewer did not know.",
  "story_spine": "Two sentences describing the emotional arc of the full video — where it starts and where it ends.",
  "key_character": "One real person, institution, or team at the centre of this story. Specific name.",
  "key_number": "The single most powerful statistic. Specific and surprising.",
  "tension": "What is at stake. What could go wrong. The honest uncomfortable truth.",
  "resolution": "How the video leaves the viewer feeling. Not just 'hopeful' — what specific thought stays with them.",
  "segment_beats": [
    "Hook: exact opening line — mid-story, startling",
    "Setup: what history the viewer needs to understand the stakes",
    "Story: the specific narrative — what happened, who did it, the turning points",
    "Impact: two specific concrete changes for ordinary Indians",
    "Challenge: the one honest uncomfortable fact",
    "Payoff: the reframe that makes India feel part of something world-historical"
  ]
}}"""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o",
                  "messages":[{"role":"user","content":prompt}],
                  "temperature":0.85,
                  "max_tokens":800,
                  "response_format":{"type":"json_object"}},
            timeout=40)
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])
    except Exception as e:
        print(f"  Arc generation failed: {e} — using fallback")
        return {
            "central_question": f"Why does {topic} matter for every Indian right now?",
            "hook_beat": f"Something about {topic} is about to change India permanently.",
            "story_spine": f"We start with what most Indians don't know about {topic}. We end understanding why it changes everything.",
            "key_character": "Indian engineers and scientists",
            "key_number": "millions of Indians affected",
            "tension": "Execution and timeline are the real test",
            "resolution": "India is not catching up — India is setting the pace",
            "segment_beats": [
                f"Hook: The one thing about {topic} that nobody is talking about.",
                f"Setup: How India got here with {topic}.",
                f"Story: The specific people and decisions behind {topic}.",
                f"Impact: What {topic} means for jobs, prices, and daily life.",
                f"Challenge: What could still go wrong with {topic}.",
                f"Payoff: Where {topic} puts India in the world."
            ]
        }


def _gen_segment_from_arc(api_key, topic, cluster, st, word_target, arc, idx):
    """
    Pass 2: Write each segment using the arc as context.
    Every segment knows the full story and its role in it.
    """
    from datetime import datetime as _dt
    beat = arc.get("segment_beats", [""] * 6)
    beat_hint = beat[idx] if idx < len(beat) else ""

    payoff_cta = "\n- Final sentence: 'Follow India20Sixty for daily India tech updates.'" if st["type"] == "payoff" else ""

    style_by_type = {
        "hook":        "Opens mid-story. No context needed. Viewer is dropped into the most startling moment. No setup, no 'today we talk about'. Direct and disorienting in the best way.",
        "context":     "Builds genuine investment. Treats viewer as intelligent. Gives only the history that makes the story make sense.",
        "deepdive":    "Specific. Names real people. Uses real numbers. Tells what happened in the order it happened. Builds. The pace accelerates as the scale becomes clear.",
        "implications":"Concrete and personal. Not 'the economy will grow' but 'the auto driver in Pune will have 40% more fares'. Specific Indians in specific situations.",
        "challenge":   "Said like a trusted friend being honest, not a legal disclaimer. Acknowledges the real risk without undermining the story.",
        "payoff":      "Zooms out. Makes the viewer feel part of something larger than one news story. Lands with weight. Not cheerful — earned.",
    }

    prompt = f"""You are the narrator of a long-form YouTube documentary for India20Sixty.
Today is {_dt.utcnow().strftime("%B %Y")}. Use correct tense — past for what already happened, future only for what genuinely has not occurred yet.

STORY: {arc.get('story_spine','')}
CENTRAL QUESTION: {arc.get('central_question','')}
KEY CHARACTER: {arc.get('key_character','')}
KEY NUMBER: {arc.get('key_number','')}

You are writing SEGMENT {idx+1}/6: {st['label']}
SEGMENT ROLE: {st['description']}
NARRATIVE STYLE FOR THIS SEGMENT: {style_by_type.get(st['type'],'')}
THIS SEGMENT'S BEAT: {beat_hint}
TARGET LENGTH: {word_target} words
{payoff_cta}

VOICE: Indian English. Educated, warm, direct. Not academic. Not news anchor. Like a brilliant friend explaining something they genuinely find extraordinary.

RULES:
- Vary sentence length. Short for impact. Longer to build. Never more than 18 words in one sentence.
- No "Fact:" prefix. No "Today we look at". No throat-clearing.
- Never use Hindi words. Pure Indian English.
- Every sentence earns its place. Cut anything generic.
- This segment must flow FROM the previous and INTO the next — it is chapter {idx+1} of one continuous story.

Return ONLY the narration. No labels. No explanation."""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o",
                  "messages":[{"role":"user","content":prompt}],
                  "temperature":0.8,
                  "max_tokens":max(400, word_target * 2)},
            timeout=45)
        r.raise_for_status()
        raw   = r.json()["choices"][0]["message"]["content"].strip()
        lines = [re.sub(r"^[\d]+[.)]\s*|^[-•]\s*","",l.strip()) for l in raw.split("\n") if l.strip()]
        script = " ".join(lines)
        print(f"    {st['type']}: {len(script.split())} words")
        return script
    except Exception as e:
        print(f"  Seg {idx} failed: {e}")
        fallbacks = {
            "hook":        f"{arc.get('hook_beat', topic + ' is changing India permanently.')}",
            "context":     f"To understand what is happening with {topic}, you need to know where this started.",
            "deepdive":    f"The real story of {topic} is more specific than any headline has told you.",
            "implications":f"For ordinary Indians, {topic} is not abstract. It changes real things in real lives.",
            "challenge":   f"There is one thing nobody wants to say about {topic}. Here it is.",
            "payoff":      f"India is not catching up. With {topic}, India is beginning to lead. Follow India20Sixty for daily India tech updates.",
        }
        return fallbacks.get(st["type"], f"{topic} — the story continues.")


def _gen_image_prompts(api_key, topic, cluster, st, script, n):
    visual = VISUAL_CONTEXT.get(st["type"], "photorealistic India, natural daylight")
    prompt = f"""Create {n} distinct image prompt(s) for a YouTube documentary segment.

TOPIC: {topic} | SEGMENT: {st['label']}
VISUAL STYLE: {visual}
SCRIPT EXCERPT: {script[:180]}

Rules:
- Unmistakably Indian — specific locations, faces, technology
- Photorealistic, natural or dramatic lighting — NOT golden hour, NOT orange
- No text, no logos, no watermarks
- Each prompt visually distinct
- Specific is better than generic ("ISRO control room in Bengaluru" not "space centre")

Return ONLY a JSON array: ["prompt1"{(',"prompt2"' if n>1 else '')}{(',"prompt3"' if n>2 else '')}]"""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini","messages":[{"role":"user","content":prompt}],
                  "temperature":0.9,"max_tokens":400},
            timeout=20)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        raw = json.loads(content[content.find("["):content.rfind("]")+1])[:n]
        return [_sanitize(p) for p in raw]
    except Exception as e:
        print(f"  Image prompts failed {st['type']}: {e}")
        defaults = {
            "hook":        f"Indian professional in pivotal moment, {topic}, sharp focus, dramatic natural light",
            "context":     f"Wide establishing shot Indian setting, {topic}, documentary style, daylight",
            "deepdive":    f"Indian engineers or scientists at work, {topic}, modern facility, real process",
            "implications":f"Ordinary Indian family or worker, grounded scene, natural daylight, hopeful",
            "challenge":   f"Honest documentary scene, tension visible, Indian setting, overcast light",
            "payoff":      f"Wide India landscape or skyline, morning light, vast and hopeful",
        }
        return [defaults.get(st["type"], f"Photorealistic modern India, {topic}")] * n


def _classify_mood(api_key, excerpt, cluster):
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini",
                  "messages":[{"role":"user","content":f"Pick ONE mood for this India tech video.\nOptions: {', '.join(MOOD_KEYS)}\nText: {excerpt[:400]}\nCluster: {cluster}\nReturn ONLY the key."}],
                  "temperature":0.3,"max_tokens":15},
            timeout=10)
        r.raise_for_status()
        mood = r.json()["choices"][0]["message"]["content"].strip().lower().split()[0]
        return mood if mood in MOOD_KEYS else CLUSTER_MOODS.get(cluster,"hopeful_future")
    except Exception:
        return CLUSTER_MOODS.get(cluster,"hopeful_future")


@app.local_entrypoint()
def main():
    result = generate_longform_script.remote(
        job_id="test-arc-001", topic="ISRO space station 2035",
        cluster="Space", target_duration=420)
    print(f"\nDuration: {result['total_duration']}s | Mood: {result['mood']}")
    print(f"Central question: {result['story_arc'].get('central_question','')}")
    for s in result["segments"]:
        print(f"\n[{s['label']}] {s['word_count']}w")
        print(s["script"][:200] + "...")