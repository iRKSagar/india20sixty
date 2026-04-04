import modal
import os
import re
import json
import requests

app = modal.App("india20sixty-longform-scriptwriter")
image = (modal.Image.debian_slim(python_version="3.11").pip_install("requests","fastapi[standard]"))
secrets = [modal.Secret.from_name("india20sixty-secrets")]

SEGMENT_TYPES = [
    {"type":"hook",         "label":"Hook",            "duration_target":30,  "word_target":50,  "description":"The most shocking fact. Stop-scroll energy. One powerful image.",           "caption_style":"large",    "transition_out":"hard_cut"},
    {"type":"context",      "label":"Context",         "duration_target":90,  "word_target":150, "description":"What is happening. History, background, why it matters now.",              "caption_style":"subtitle", "transition_out":"dissolve"},
    {"type":"deepdive",     "label":"Deep Dive",       "duration_target":150, "word_target":250, "description":"The full story. Numbers, comparisons, milestones, key people.",            "caption_style":"subtitle", "transition_out":"dissolve"},
    {"type":"implications", "label":"What It Means",   "duration_target":90,  "word_target":150, "description":"Impact on ordinary Indians. Jobs, economy, daily life changes.",          "caption_style":"subtitle", "transition_out":"fade"},
    {"type":"challenge",    "label":"The Challenge",   "duration_target":60,  "word_target":100, "description":"Honest counterpoint. What could go wrong. What is being ignored.",       "caption_style":"subtitle", "transition_out":"fadeblack"},
    {"type":"payoff",       "label":"The Big Picture", "duration_target":60,  "word_target":100, "description":"Optimistic close. India's place in the world. Subscribe CTA.",           "caption_style":"large",    "transition_out":"fade"},
]

SEGMENT_IMAGE_COUNTS = {"hook":1,"context":2,"deepdive":3,"implications":2,"challenge":1,"payoff":1}

# Visual context per segment type — no ARRI, no golden hour
VISUAL_CONTEXT = {
    "hook":        "ultra-dramatic single subject, 70% frame, extreme contrast, natural dramatic lighting, Indian setting",
    "context":     "documentary photorealistic, Indian setting, wide establishing shot, natural daylight",
    "deepdive":    "technical detail, Indian infrastructure or technology, people at work, showing scale",
    "implications":"human impact, everyday Indians, hopeful community scene, natural light",
    "challenge":   "tension, honest darker tone, problem clearly visualised, overcast or indoor lighting",
    "payoff":      "wide optimistic shot, India's scale, morning light, proud moment, Indian skyline or landscape",
}

# Banned words to strip from image prompts
_BAD = ["ARRI","8K","8k","golden hour","saffron","ochre","warm palette","marigold","anamorphic","HDR"]

def _sanitize(prompt):
    for bad in _BAD:
        prompt = prompt.replace(bad,"").replace(bad.lower(),"")
    return re.sub(r"  +"," ",prompt).strip()

MOOD_KEYS = ["cinematic_epic","breaking_news","hopeful_future","dark_serious","cold_tech","vibrant_pop","nostalgic_film","warm_human"]
CLUSTER_MOODS = {"Space":"cinematic_epic","DeepTech":"cold_tech","AI":"cold_tech","Gadgets":"vibrant_pop","GreenTech":"hopeful_future","Startups":"hopeful_future"}


@app.function(image=image, secrets=secrets, cpu=0.5, memory=512, timeout=300)
def generate_longform_script(job_id: str, topic: str, cluster: str, target_duration: int, fact_package: dict = None) -> dict:
    """Generate 6-segment structured script. Returns: { segments, total_duration, mood }"""
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    print(f"\n[Longform Script] {job_id} | {topic[:60]} | {target_duration}s")

    scale = target_duration / sum(s["duration_target"] for s in SEGMENT_TYPES)
    segments = []

    for i, st in enumerate(SEGMENT_TYPES):
        scaled_dur   = int(st["duration_target"] * scale)
        scaled_words = int(st["word_target"] * scale)
        script       = _gen_segment_script(OPENAI_API_KEY, topic, cluster, st, scaled_words, fact_package, i)
        img_prompts  = _gen_image_prompts(OPENAI_API_KEY, topic, cluster, st, script, SEGMENT_IMAGE_COUNTS.get(st["type"],2))
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
        print(f"  [{st['label']}] {len(script.split())}w | {scaled_dur}s | {len(img_prompts)} prompts")

    mood = _classify_mood(OPENAI_API_KEY, " ".join(s["script"] for s in segments)[:500], cluster)
    print(f"  Mood: {mood}")
    return {
        "segments":       segments,
        "total_duration": sum(s["duration_target"] for s in segments),
        "mood":           mood,
    }


def _gen_segment_script(api_key, topic, cluster, st, word_target, fact_package, idx):
    fact_hint  = f"\nKey fact to anchor: {fact_package['key_fact']}" if (fact_package and fact_package.get("found") and idx == 0) else ""
    payoff_cta = "\n- End with: Follow India20Sixty for daily India tech updates." if st["type"] == "payoff" else ""
    hook_rule  = "\n- Open with the single most dramatic stat or fact about this topic." if idx == 0 else ""

    prompt = f"""Write segment {idx+1}/6 of a long-form YouTube video for India20Sixty — India's near future channel.

TOPIC: {topic}
CLUSTER: {cluster}
SEGMENT TYPE: {st['label']} — {st['description']}
TARGET LENGTH: ~{word_target} words
{fact_hint}

STRICT RULES:
- Indian English only. No Hindi words. No Hinglish.
- Warm, direct, authoritative voice.
- Short punchy sentences, max 15 words each.
- Never start with "Fact:". State facts directly.
- Flows naturally into next segment.{hook_rule}{payoff_cta}

Return ONLY the narration as plain text. No labels, no numbering."""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini",
                  "messages":[{"role":"user","content":prompt}],
                  "temperature":0.75,
                  "max_tokens":max(300, word_target * 2)},
            timeout=40)
        r.raise_for_status()
        raw   = r.json()["choices"][0]["message"]["content"].strip()
        lines = [re.sub(r"^[\d]+[.)]\s*|^[-\u2022]\s*","",l.strip()) for l in raw.split("\n") if l.strip()]
        return " ".join(lines)
    except Exception as e:
        print(f"  Seg {idx} script failed: {e}")
        fallbacks = {
            "hook":        f"India is rewriting history with {topic}. This is happening right now.",
            "context":     f"{topic} has been years in the making. Here is the full story.",
            "deepdive":    f"The numbers behind {topic} are extraordinary. Let us break it down.",
            "implications":f"For ordinary Indians this changes everything. Jobs, opportunities, daily life.",
            "challenge":   f"Execution is the real test. India has made big promises before.",
            "payoff":      f"India is not catching up. India is setting the pace. Follow India20Sixty for daily updates.",
        }
        return fallbacks.get(st["type"], f"India's {topic} story is only beginning.")


def _gen_image_prompts(api_key, topic, cluster, st, script, n):
    visual = VISUAL_CONTEXT.get(st["type"], "photorealistic India, natural daylight")
    prompt = f"""Create {n} distinct image prompt(s) for this YouTube long-form video segment.

TOPIC: {topic} | SEGMENT: {st['label']}
VISUAL STYLE: {visual}
SCRIPT: {script[:200]}

Rules:
- Unmistakably Indian settings, faces, technology
- Photorealistic, contemporary, no text or logos
- Natural or dramatic lighting — NOT golden hour, NOT orange/saffron
- Each prompt visually distinct from the others
- No ARRI, no 8K, no "cinematic" buzz words

Return ONLY a JSON array of {n} prompt string(s): ["prompt1"{(',"prompt2"' if n>1 else '')}{(',"prompt3"' if n>2 else '')}]"""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini",
                  "messages":[{"role":"user","content":prompt}],
                  "temperature":0.9,
                  "max_tokens":400},
            timeout=20)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        raw_prompts = json.loads(content[content.find("["):content.rfind("]")+1])[:n]
        return [_sanitize(p) for p in raw_prompts]
    except Exception as e:
        print(f"  Image prompts failed for {st['type']}: {e}")
        defaults = {
            "hook":        f"Indian professional in dramatic moment, {topic}, sharp focus, natural light",
            "context":     f"Wide establishing shot of Indian city or technology hub, {topic}, daylight",
            "deepdive":    f"Indian engineers or scientists at work, {topic}, modern facility",
            "implications":f"Ordinary Indian family or community, hopeful scene, natural daylight",
            "challenge":   f"Indian urban challenge or problem, honest documentary style, overcast light",
            "payoff":      f"Wide optimistic India landscape or skyline, morning light, hopeful",
        }
        return [defaults.get(st["type"], f"Photorealistic modern India, {topic}")] * n


def _classify_mood(api_key, excerpt, cluster):
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini",
                  "messages":[{"role":"user","content":f"Pick ONE mood for this India tech video script.\nOptions: {', '.join(MOOD_KEYS)}\nScript: {excerpt}\nCluster: {cluster}\nReturn ONLY the key, nothing else."}],
                  "temperature":0.3,"max_tokens":15},
            timeout=10)
        r.raise_for_status()
        mood = r.json()["choices"][0]["message"]["content"].strip().lower().split()[0]
        if mood in MOOD_KEYS: return mood
    except Exception as e:
        print(f"  Mood classify failed: {e}")
    return CLUSTER_MOODS.get(cluster, "hopeful_future")


@app.local_entrypoint()
def main():
    result = generate_longform_script.remote(
        job_id="test-lf-001", topic="ISRO space station 2035",
        cluster="Space", target_duration=420)
    print(f"Duration: {result['total_duration']}s | Mood: {result['mood']}")
    for s in result["segments"]:
        print(f"  [{s['label']}] {s['word_count']}w | {s['duration_target']}s | {len(s['image_prompts'])} imgs")