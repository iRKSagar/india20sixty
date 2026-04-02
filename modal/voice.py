import modal
import io
import os
import re
import subprocess
from pathlib import Path

# ==========================================
# MODAL APP — VOICE
# india20sixty channel
#
# Primary:   Chatterbox (Resemble AI) — beats ElevenLabs 63.75% in blind tests
#            MIT license, 0.5B params, emotion control, GPU required
# Fallback1: Kokoro 82M — Apache 2.0, CPU, 4.5 MOS quality score
# Fallback2: ElevenLabs — if API key set and both above fail
#
# Each engine respects the same channel voice identity constants.
# ==========================================

app = modal.App("india20sixty-voice")

# ── CHANNEL VOICE IDENTITY ────────────────────────────────────
# Chatterbox settings
CB_EXAGGERATION = 0.45   # emotion intensity: 0=flat, 1=very expressive. 0.45 = warm confident
CB_CFG_WEIGHT   = 0.5    # classifier-free guidance: 0.5 = balanced natural delivery
CB_SPEAKING_RATE = 0.95  # slightly slower for clarity on Shorts

# Kokoro fallback settings
KK_VOICE_PRESET  = "af_heart"  # warm Indian English female
KK_SPEAKING_RATE = 0.95
KK_LANGUAGE      = "en-us"

CHANNEL_NAME = "india20sixty"
# ─────────────────────────────────────────────────────────────

TMP_DIR = "/tmp/india20sixty-voice"

# Chatterbox needs GPU — A10G or T4 both work; T4 cheaper for 0.5B model
chatterbox_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg", "libsndfile1", "espeak-ng")
    .pip_install(
        "chatterbox-tts>=0.1.1",
        "kokoro>=0.9.2",
        "soundfile",
        "numpy",
        "requests",
        "torch",
        "torchaudio",
    )
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]


# ==========================================
# MAIN ENTRY — called by pipeline.py
# ==========================================

@app.function(
    image=chatterbox_image,
    gpu="T4",              # T4 sufficient for 0.5B Chatterbox; cheaper than A10G
    cpu=2.0,
    memory=4096,
    timeout=120,
    secrets=secrets,
)
def generate_voice(
    job_id: str,
    reviewed_script: str,
    engine_mode: str = "inbuilt",  # "inbuilt" | "external"
) -> dict:
    """
    Generate voice. Returns { audio_path, duration, engine }.
    Inbuilt:  Chatterbox → Kokoro fallback
    External: ElevenLabs directly
    """
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    audio_path = f"{TMP_DIR}/{job_id}.mp3"

    print(f"\n[Voice] job={job_id} channel={CHANNEL_NAME} mode={engine_mode}")

    # Clean script — same rules regardless of engine
    clean = _clean_script(reviewed_script)
    print(f"  Script ({len(clean.split())} words, {len(clean)} chars): {clean[:80]}...")

    if engine_mode == "inbuilt":
        # ── PRIMARY: Chatterbox ───────────────────────────────
        print(f"  [Primary: Chatterbox — exaggeration={CB_EXAGGERATION}]")
        try:
            audio_bytes = _chatterbox_generate(clean)
            with open(audio_path, "wb") as f: f.write(audio_bytes)
            duration = _get_duration(audio_path)
            print(f"  ✓ Chatterbox: {duration:.1f}s")
            return {"audio_path": audio_path, "duration": duration, "engine": "chatterbox"}
        except Exception as e:
            print(f"  Chatterbox failed: {e}")

        # ── FALLBACK: Kokoro ──────────────────────────────────
        print(f"  [Fallback: Kokoro — {KK_VOICE_PRESET}]")
        try:
            audio_bytes = _kokoro_generate(clean)
            with open(audio_path, "wb") as f: f.write(audio_bytes)
            duration = _get_duration(audio_path)
            print(f"  ✓ Kokoro: {duration:.1f}s")
            return {"audio_path": audio_path, "duration": duration, "engine": "kokoro"}
        except Exception as e:
            print(f"  Kokoro failed: {e}")

    # ── EXTERNAL / LAST RESORT: ElevenLabs ───────────────────
    ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
    VOICE_ID           = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")

    if ELEVENLABS_API_KEY:
        print("  [ElevenLabs]")
        try:
            import requests as req
            speech_text = clean.replace("...", "<break time='0.5s'/>")
            r = req.post(
                f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
                headers={"xi-api-key": ELEVENLABS_API_KEY,
                         "Content-Type": "application/json"},
                json={
                    "text":     speech_text,
                    "model_id": "eleven_multilingual_v2",
                    "voice_settings": {
                        "stability":         0.42,
                        "similarity_boost":  0.85,
                        "style":             0.35,
                        "use_speaker_boost": True,
                    },
                },
                timeout=60,
            )
            r.raise_for_status()
            with open(audio_path, "wb") as f: f.write(r.content)
            duration = _get_duration(audio_path)
            print(f"  ✓ ElevenLabs: {duration:.1f}s")
            return {"audio_path": audio_path, "duration": duration, "engine": "elevenlabs"}
        except Exception as e:
            print(f"  ElevenLabs failed: {e}")

    raise Exception(
        f"All voice engines failed for job {job_id}. "
        "Check Modal logs for individual engine errors."
    )


# ==========================================
# CHATTERBOX GENERATION
# Resemble AI — MIT License
# Beats ElevenLabs in 63.75% of blind comparisons
# ==========================================

def _chatterbox_generate(text: str) -> bytes:
    """
    Run Chatterbox inference.
    Returns MP3 bytes.
    Exaggeration controls emotion intensity — 0.45 = confident, warm, natural.
    CFG weight controls how closely model follows text — 0.5 = balanced.
    """
    import torch
    import torchaudio
    from chatterbox.tts import ChatterboxTTS

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"  Chatterbox device: {device}")

    model = ChatterboxTTS.from_pretrained(device=device)

    # Generate audio
    wav = model.generate(
        text,
        exaggeration=CB_EXAGGERATION,
        cfg_weight=CB_CFG_WEIGHT,
    )

    # wav is a tensor — convert to MP3 bytes
    buf = io.BytesIO()

    # Save as WAV first then convert to MP3 via ffmpeg
    wav_path = f"/tmp/cb_wav_{os.getpid()}.wav"
    torchaudio.save(wav_path, wav, model.sr)

    mp3_path = f"/tmp/cb_mp3_{os.getpid()}.mp3"
    result = subprocess.run(
        ["ffmpeg", "-y", "-i", wav_path,
         "-codec:a", "libmp3lame", "-qscale:a", "2",  # VBR quality 2 = ~190kbps
         "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",      # normalize loudness
         mp3_path],
        capture_output=True, timeout=30
    )

    if result.returncode != 0:
        raise Exception(f"ffmpeg conversion failed: {result.stderr.decode()[:200]}")

    with open(mp3_path, "rb") as f:
        return f.read()


# ==========================================
# KOKORO GENERATION (FALLBACK)
# Apache 2.0 — 4.5 MOS — best open source CPU model
# ==========================================

def _kokoro_generate(text: str) -> bytes:
    from kokoro import KPipeline
    import soundfile as sf
    import numpy as np

    pipeline = KPipeline(lang_code=KK_LANGUAGE)
    audio_chunks = []
    sample_rate  = None

    for samples, sr, _ in pipeline(text, voice=KK_VOICE_PRESET, speed=KK_SPEAKING_RATE):
        audio_chunks.append(samples)
        if sample_rate is None:
            sample_rate = sr

    if not audio_chunks:
        raise Exception("Kokoro returned no audio chunks")

    audio = np.concatenate(audio_chunks)
    buf   = io.BytesIO()
    sf.write(buf, audio, sample_rate, format="MP3")
    return buf.getvalue()


# ==========================================
# SCRIPT CLEANING
# Same rules regardless of voice engine.
# Deterministic only — NO GPT in this path.
# ==========================================

def _clean_script(script: str) -> str:
    clean = script

    # Strip prefixes not meant to be spoken
    clean = re.sub(r"^Fact:\s*", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bFact:\s*", "", clean, flags=re.IGNORECASE)

    # Strip emotion/SSML tags — Chatterbox uses exaggeration param instead
    clean = re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>", "", clean)
    clean = re.sub(r"<break[^>]*/?>", " ", clean)

    # Indian pronunciation — deterministic find-and-replace ONLY
    # Rule: NEVER let GPT touch this. These are the exact substitutions, nothing else.
    acronyms = [
        ("ISRO's",   "I.S.R.O.'s"),
        ("DRDO's",   "D.R.D.O.'s"),
        ("ISRO",     "I.S.R.O."),
        ("DRDO",     "D.R.D.O."),
        ("IIT",      "I.I.T."),
        ("IITs",     "I.I.T.s"),
        ("UPI",      "U.P.I."),
        ("AIIMS",    "A.I.I.M.S."),
        ("NASSCOM",  "NAS-com"),
        ("SEBI",     "SEE-bi"),
        ("NITI",     "NEE-ti"),
        ("BJP",      "B.J.P."),
        ("RBI",      "R.B.I."),
        ("GST",      "G.S.T."),
        ("GDP",      "G.D.P."),
        ("EV",       "E.V."),
        ("EVs",      "E.V.s"),
        ("AI",       "A.I."),
        ("ML",       "M.L."),
    ]
    for wrong, right in acronyms:
        clean = clean.replace(wrong, right)

    missions = [
        ("Chandrayaan-3",  "Chandra-yaan Three"),
        ("Chandrayaan-2",  "Chandra-yaan Two"),
        ("Chandrayaan",    "Chandra-yaan"),
        ("Gaganyaan",      "Gagan-yaan"),
        ("Mangalyaan",     "Mangal-yaan"),
        ("Aditya-L1",      "Aditya L-one"),
        ("Shubhanshu",     "Shub-haan-shu"),
    ]
    for wrong, right in missions:
        clean = clean.replace(wrong, right)

    # Currency, symbols, numbers
    clean = clean.replace("\u20b9", "rupees ")
    clean = clean.replace("%", " percent")
    clean = clean.replace("&", " and ")
    clean = clean.replace("\u2192", " to ")
    clean = clean.replace("vs.", "versus")
    clean = clean.replace("vs ", "versus ")

    # Indian number formats: 10,00,000 → 10 lakh
    clean = re.sub(r"(\d+),00,00,000",
                   lambda m: m.group(1) + " crore", clean)
    clean = re.sub(r"(\d+),00,000",
                   lambda m: m.group(1) + " lakh", clean)

    # Whitespace
    clean = re.sub(r"\s+", " ", clean).strip()

    return clean


def _get_duration(path: str) -> float:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=10,
        )
        return float(result.stdout.strip())
    except Exception:
        return 25.0


@app.local_entrypoint()
def main():
    result = generate_voice.remote(
        job_id="test-chatterbox-001",
        reviewed_script=(
            "I.S.R.O. is building India's first space station by 2035. "
            "This changes everything for Indian science and global ambition. "
            "Twenty thousand engineers will work on this landmark project. "
            "Every Indian child will look at the sky differently now. "
            "But funding and political will remain the real test. "
            "Will India finally become a true space superpower?"
        ),
        engine_mode="inbuilt",
    )
    print(f"Engine: {result['engine']}")
    print(f"Duration: {result['duration']:.1f}s")
    print(f"Audio: {result['audio_path']}")
