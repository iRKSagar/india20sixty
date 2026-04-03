import modal
import os
import re
import json
import requests

app = modal.App("india20sixty-longform-scriptwriter")
image = (modal.Image.debian_slim(python_version="3.11").pip_install("requests","fastapi[standard]"))
secrets = [modal.Secret.from_name("india20sixty-secrets")]

# ==========================================
# SEGMENT DEFINITIONS
# Fixed 6-segment chapter structure.
# Every long-form video uses this layout.
# ==========================================

SEGMENT_TYPES = [
    {"type":"hook",         "label":"Hook",            "duration_target":30,  "word_target":50,  "description":"The most shocking fact. Stop-scroll energy. One image.",                              "caption_style":"large",    "transition_out":"hard_cut"},
    {"type":"context",      "label":"Context",         "duration_target":90,  "word_target":150, "description":"What is actually happening. History, background, why now.",                           "caption_style":"subtitle", "transition_out":"dissolve"},
    {"type":"deepdive",     "label":"Deep Dive",       "duration_target":150, "word_target":250, "description":"The detailed story. Numbers, comparisons, milestones, people.",                       "caption_style":"subtitle", "transition_out":"dissolve"},
    {"type":"implications", "label":"What It Means",   "duration_target":90,  "word_target":150, "description":"Impact on ordinary Indians. Jobs, economy, daily life.",                              "caption_style":"subtitle", "transition_out":"fade"},
    {"type":"challenge",    "label":"The Challenge",   "duration_target":60,  "word_target":100, "description":"Honest counterpoint. What could go wrong. What is being ignored.",                   "caption_style":"subtitle", "transition_out":"fadeblack"},
    {"type":"payoff",       "label":"The Big Picture", "duration_target":60,  "word_target":100, "description":"Optimistic close. India's place in the world. Subscribe CTA.",                       "caption_style":"large",    "transition_out":"fade"},
]

SEGMENT_IMAGE_COUNTS = {"hook":1,"context":2,"deepdive":3,"implications":2,"challenge":1,"payoff":1}

VISUAL_CONTEXT = {
    "hook":"ultra-dramatic showstopper, one dominant subject 70% of frame, extreme contrast, stop-scroll",
    "context":"documentary style, informative, Indian setting, establishing shots",
    "deepdive":"technical detail, infrastructure, people at work, showing scale and process",
    "implications":"human impact, everyday Indians, hopeful community scenes",
    "challenge":"tension, honest, darker tone, problem clearly visualised",
    "payoff":"wide optimistic, India's place in the world, golden light, proud moment",
}

MOOD_KEYS = ["cinematic_epic","breaking_news","hopeful_future","dark_serious","cold_tech","vibrant_pop","nostalgic_film","warm_human"]
CLUSTER_MOODS = {"Space":"cinematic_epic","DeepTech":"cold_tech","AI":"cold_tech","Gadgets":"vibrant_pop","GreenTech":"hopeful_future","Startups":"hopeful_future"}


@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=120)
def generate_longform_script(job_id: str, topic: str, cluster: str, target_duration: int, fact_package: dict = None) -> dict:
    """
    Generate 6-segment structured script for a long-form video.
    target_duration in seconds (180-720).
    Returns: { segments, total_duration, mood }
    """
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
        print(f"  [{st['label']}] {len(script.split())}w | {scaled_dur}s | {len(img_prompts)} img")

    mood = _classify_mood(OPENAI_API_KEY, " ".join(s["script"] for s in segments)[:500], cluster)
    return {"segments": segments, "total_duration": sum(s["duration_target"] for s in segments), "mood": mood}


def _gen_segment_script(api_key, topic, cluster, st, word_target, fact_package, idx):
    fact_hint = f"\nKey fact: {fact_package['key_fact']}" if (fact_package and fact_package.get("found") and idx == 0) else ""
    payoff_cta = "\n- End with: Follow India20Sixty for daily India tech updates." if st["type"] == "payoff" else ""
    hook_rule  = "\n- Open with the most dramatic stat or fact about this topic." if idx == 0 else ""

    prompt = f"""Write segment {idx+1}/6 of a long-form YouTube video for India20Sixty — India's near future channel.

TOPIC: {topic}
CLUSTER: {cluster}
SEGMENT: {st['label']} — {st['description']}
TARGET: ~{word_target} words
{fact_hint}
RULES:
- Indian English. Warm, direct, authoritative. Not American or British.
- Short punchy sentences, max 15 words each.
- No "Fact:" prefix. State facts directly. No Hindi words.
- Flow naturally into the next segment.{hook_rule}{payoff_cta}

Return ONLY the narration as plain text."""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini","messages":[{"role":"user","content":prompt}],"temperature":0.75,"max_tokens":max(300,word_target*2)},
            timeout=30)
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
        lines = [re.sub(r"^[\d]+[.)]\s*|^[-\u2022]\s*","",l.strip()) for l in raw.split("\n") if l.strip()]
        return " ".join(lines)
    except Exception as e:
        print(f"  Seg {idx} script failed: {e}")
        fallbacks = {"hook":f"India is about to change everything with {topic}. This is happening right now.","context":f"{topic} has been years in the making.","deepdive":f"The numbers behind {topic} are staggering.","implications":f"For ordinary Indians this changes everything.","challenge":f"Execution is the real test. India has promised before.","payoff":f"India is not catching up. India is setting the pace. Follow India20Sixty for daily India tech updates."}
        return fallbacks.get(st["type"], f"India's {topic} story continues.")


def _gen_image_prompts(api_key, topic, cluster, st, script, n):
    prompt = f"""Create {n} distinct cinematic image prompt(s) for this YouTube video segment.

TOPIC: {topic} | SEGMENT: {st['label']}
VISUAL: {VISUAL_CONTEXT.get(st['type'],'cinematic India')}
SCRIPT EXCERPT: {script[:200]}

Rules: unmistakably Indian, hyperrealistic ARRI cinematic, each prompt visually distinct.
Return ONLY a JSON array: ["prompt1"{",'prompt2'" if n>1 else ""}{",'prompt3'" if n>2 else ""}]"""

    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini","messages":[{"role":"user","content":prompt}],"temperature":0.9,"max_tokens":400},
            timeout=20)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        return json.loads(content[content.find("["):content.rfind("]")+1])[:n]
    except Exception as e:
        print(f"  Image prompts failed for {st['type']}: {e}")
        return [f"Cinematic India, {topic}, {st['type']}, ARRI 8K"] * n


def _classify_mood(api_key, excerpt, cluster):
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"},
            json={"model":"gpt-4o-mini","messages":[{"role":"user","content":f"Pick ONE mood for this script. Options: {', '.join(MOOD_KEYS)}\nScript: {excerpt}\nCluster: {cluster}\nReturn ONLY the key."}],"temperature":0.3,"max_tokens":10},
            timeout=10)
        r.raise_for_status()
        mood = r.json()["choices"][0]["message"]["content"].strip().lower().split()[0]
        if mood in MOOD_KEYS: return mood
    except Exception: pass
    return CLUSTER_MOODS.get(cluster, "hopeful_future")


@app.local_entrypoint()
def main():
    result = generate_longform_script.remote(job_id="test-lf-001", topic="ISRO space station 2035", cluster="Space", target_duration=420)
    print(f"Duration: {result['total_duration']}s | Mood: {result['mood']}")
    for s in result["segments"]: print(f"  [{s['label']}] {s['word_count']}w | {s['duration_target']}s")
