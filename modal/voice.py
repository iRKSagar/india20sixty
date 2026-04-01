import modal
import os
import re
import subprocess
import requests
from pathlib import Path

# ==========================================
# MODAL APP — VOICE
# ElevenLabs TTS only.
# No ffmpeg rendering. No image work. No GPT.
# Takes reviewed_script → returns local audio path + duration.
# ==========================================

app = modal.App("india20sixty-voice")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg")   # ffprobe needed for duration check
    .pip_install("requests")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR = "/tmp/voice"

# ElevenLabs settings — confirmed working for Indian English
# stability=0.42 minimum — below this accent drifts between sentences
# eleven_multilingual_v2 — better language detection than turbo variants
ELEVENLABS_SETTINGS = {
    "model_id":     "eleven_multilingual_v2",
    "voice_settings": {
        "stability":        0.42,
        "similarity_boost": 0.85,
        "style":            0.35,
        "use_speaker_boost": True,
    }
}

# Deterministic pronunciation fixes applied before sending to ElevenLabs.
# NO GPT in this path — any AI rewriting causes language misdetection (German accent).
PRONUNCIATION_FIXES = [
    ("ISRO",        "I.S.R.O."),
    ("ISRO's",      "I.S.R.O.'s"),
    ("NASA",        "N.A.S.A."),
    ("DRDO",        "D.R.D.O."),
    ("DRDO's",      "D.R.D.O.'s"),
    ("IIT",         "I.I.T."),
    ("Chandrayaan", "Chandra-yaan"),
    ("Gaganyaan",   "Gagan-yaan"),
    ("Mangalyaan",  "Mangal-yaan"),
    ("\u20b9",      "rupees "),
    ("₹",           "rupees "),
    ("%",           " percent"),
]


@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=90)
def generate_voice(job_id: str, reviewed_script: str) -> tuple:
    """
    Takes reviewed_script (with emotion tags and pronunciation fixes already applied).
    Returns (local_audio_path, duration_seconds).

    RULES (from engineering journal):
    - Strip emotion tags before sending to ElevenLabs — they cause German accent
    - Strip "Fact:" prefix — not meant to be spoken
    - Apply pronunciation fixes deterministically — NO GPT
    - Use eleven_multilingual_v2 with stability=0.42 minimum
    """
    ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
    VOICE_ID           = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    print(f"\n[Voice] job={job_id}")

    # ── CLEAN SCRIPT FOR TTS ─────────────────────────────────────
    clean = reviewed_script

    # Strip "Fact:" prefix — not meant to be spoken
    clean = re.sub(r'^Fact:\s*', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\bFact:\s*', '', clean, flags=re.IGNORECASE)

    # Strip emotion tags — ElevenLabs does not understand them
    # and they cause language misdetection producing German accent
    clean = re.sub(r'</?(?:excited|happy|sad|whisper|angry)[^>]*>', '', clean)

    # Apply deterministic pronunciation fixes
    # This is the ONLY processing allowed — no GPT rewriting
    for wrong, right in PRONUNCIATION_FIXES:
        clean = clean.replace(wrong, right)

    # Convert "..." to ElevenLabs pause tag
    speech_text = clean.replace("...", "<break time='0.5s'/>")

    print(f"  Speech text ({len(speech_text)} chars): {speech_text[:100]}...")

    # ── TTS API CALL ─────────────────────────────────────────────
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
        headers={"xi-api-key":    ELEVENLABS_API_KEY,
                 "Content-Type":  "application/json"},
        json={"text": speech_text, **ELEVENLABS_SETTINGS},
        timeout=60,
    )
    r.raise_for_status()

    # ── SAVE AND CHECK DURATION ──────────────────────────────────
    raw_path   = f"{TMP_DIR}/{job_id}_raw.mp3"
    audio_path = f"{TMP_DIR}/{job_id}.mp3"

    with open(raw_path, "wb") as f:
        f.write(r.content)

    # Get duration via ffprobe
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", raw_path],
            capture_output=True, text=True, timeout=10,
        )
        duration = float(probe.stdout.strip())
    except Exception:
        duration = 25.0

    print(f"  Duration: {duration:.1f}s")

    # Rename to final path
    os.rename(raw_path, audio_path)

    # Sanity check — Hindi script reads 3x slower
    # 55-word English script should be 20-35 seconds
    if duration > 40:
        print(f"  WARNING: audio {duration:.1f}s — may be too long for Shorts")

    return audio_path, duration