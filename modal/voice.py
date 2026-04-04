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
# Primary:   Chatterbox with voice cloning from reference audio
#            MIT license, 0.5B params, zero-shot voice clone
# Fallback1: Kokoro 82M — Apache 2.0, CPU, 4.5 MOS
# Fallback2: ElevenLabs — if API key set
# ==========================================

app = modal.App("india20sixty-voice")

# ── CHANNEL VOICE IDENTITY ────────────────────────────────────
# Voice reference file stored in R2 — downloaded at runtime
# Upload india20sixty_voice_ref.mp3 to R2 under voice-refs/
VOICE_REF_R2_KEY = "voice-refs/india20sixty_voice_ref.mp3"

# Chatterbox settings — tuned for Indian English cloning
CB_EXAGGERATION  = 0.2    # very faithful to reference — minimises artifacts
CB_CFG_WEIGHT    = 0.6    # slightly higher guidance = cleaner pronunciation

# Kokoro fallback settings
KK_VOICE_PRESET  = "af_heart"
KK_SPEAKING_RATE = 0.95
KK_LANGUAGE      = "en-us"

CHANNEL_NAME = "india20sixty"
TMP_DIR      = "/tmp/india20sixty-voice"
# ─────────────────────────────────────────────────────────────

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
    gpu="T4",
    cpu=2.0,
    memory=4096,
    timeout=120,
    secrets=secrets,
)
def generate_voice(
    job_id: str,
    reviewed_script: str,
    engine_mode: str = "inbuilt",
) -> dict:
    """
    Generate voice using Chatterbox with voice cloning from reference.
    Returns { audio_path, duration, engine, audio_bytes }
    """
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    audio_path = f"{TMP_DIR}/{job_id}.mp3"

    print(f"\n[Voice] job={job_id} channel={CHANNEL_NAME} mode={engine_mode}")

    clean = _clean_script(reviewed_script)
    print(f"  Script ({len(clean.split())} words): {clean[:80]}...")

    if engine_mode == "inbuilt":
        # ── PRIMARY: Chatterbox with voice cloning ────────────
        print(f"  [Chatterbox — voice clone from R2 reference]")
        try:
            # Download voice reference from R2
            voice_ref_path = _download_voice_ref()
            audio_bytes = _chatterbox_generate(clean, voice_ref_path)
            with open(audio_path, "wb") as f: f.write(audio_bytes)
            duration = _get_duration(audio_path)
            print(f"  ✓ Chatterbox cloned: {duration:.1f}s")
            return {"audio_path": audio_path, "duration": duration,
                    "engine": "chatterbox-clone", "audio_bytes": audio_bytes}
        except Exception as e:
            print(f"  Chatterbox clone failed: {e}")

        # ── FALLBACK: Kokoro ──────────────────────────────────
        print(f"  [Fallback: Kokoro — {KK_VOICE_PRESET}]")
        try:
            audio_bytes = _kokoro_generate(clean)
            with open(audio_path, "wb") as f: f.write(audio_bytes)
            duration = _get_duration(audio_path)
            print(f"  ✓ Kokoro: {duration:.1f}s")
            return {"audio_path": audio_path, "duration": duration,
                    "engine": "kokoro", "audio_bytes": audio_bytes}
        except Exception as e:
            print(f"  Kokoro failed: {e}")

    # ── EXTERNAL: ElevenLabs ─────────────────────────────────
    ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
    VOICE_ID           = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")
    if ELEVENLABS_API_KEY:
        print("  [ElevenLabs]")
        try:
            import requests as req
            r = req.post(
                f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}",
                headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                json={"text": clean, "model_id": "eleven_multilingual_v2",
                      "voice_settings": {"stability": 0.42, "similarity_boost": 0.85,
                                         "style": 0.35, "use_speaker_boost": True}},
                timeout=60,
            )
            r.raise_for_status()
            audio_bytes = r.content
            with open(audio_path, "wb") as f: f.write(audio_bytes)
            duration = _get_duration(audio_path)
            return {"audio_path": audio_path, "duration": duration,
                    "engine": "elevenlabs", "audio_bytes": audio_bytes}
        except Exception as e:
            print(f"  ElevenLabs failed: {e}")

    raise Exception("All voice engines failed")


# ==========================================
# DOWNLOAD VOICE REFERENCE FROM R2
# ==========================================

def _download_voice_ref() -> str:
    """Download the channel voice reference from R2. Returns local path."""
    import requests as req
    R2_BASE_URL   = os.environ.get("R2_BASE_URL", "").rstrip("/")
    R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID", "")
    R2_SECRET     = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET     = os.environ.get("R2_BUCKET", "india20sixty")

    ref_path = f"{TMP_DIR}/voice_ref.mp3"
    if os.path.exists(ref_path):
        return ref_path  # already downloaded in this container

    # Try public R2 URL first
    if R2_BASE_URL:
        url = f"{R2_BASE_URL}/{VOICE_REF_R2_KEY}"
        try:
            r = req.get(url, timeout=15)
            if r.status_code == 200:
                with open(ref_path, "wb") as f: f.write(r.content)
                print(f"  Voice ref downloaded: {len(r.content)//1024}KB")
                return ref_path
        except Exception as e:
            print(f"  Public R2 fetch failed: {e}")

    # Try signed R2 URL
    if R2_ACCOUNT_ID and R2_ACCESS_KEY:
        import hashlib, hmac, urllib.parse
        from datetime import datetime as dt
        now      = dt.utcnow()
        date_str = now.strftime("%Y%m%d")
        time_str = now.strftime("%Y%m%dT%H%M%SZ")
        endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        url      = f"{endpoint}/{R2_BUCKET}/{VOICE_REF_R2_KEY}"

        payload_hash   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical = "\n".join([
            "GET",
            f"/{R2_BUCKET}/{urllib.parse.quote(VOICE_REF_R2_KEY, safe='/')}",
            "",
            f"host:{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            f"x-amz-content-sha256:{payload_hash}",
            f"x-amz-date:{time_str}",
            "",
            signed_headers,
            payload_hash,
        ])
        cred_scope     = f"{date_str}/auto/s3/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", time_str, cred_scope,
            hashlib.sha256(canonical.encode()).hexdigest(),
        ])
        def sign(key, msg):
            return hmac.new(key, msg.encode(), hashlib.sha256).digest()
        signing_key = sign(sign(sign(sign(
            f"AWS4{R2_SECRET}".encode(), date_str), "auto"), "s3"), "aws4_request")
        signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

        r = req.get(url, headers={
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": time_str,
            "Host": f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={R2_ACCESS_KEY}/{cred_scope},"
                f"SignedHeaders={signed_headers},Signature={signature}"
            ),
        }, timeout=15)
        if r.status_code == 200:
            with open(ref_path, "wb") as f: f.write(r.content)
            print(f"  Voice ref downloaded via signed URL: {len(r.content)//1024}KB")
            return ref_path

    raise Exception("Could not download voice reference from R2")


# ==========================================
# CHATTERBOX WITH VOICE CLONING
# ==========================================

def _chatterbox_generate(text: str, voice_ref_path: str) -> bytes:
    """
    Run Chatterbox with zero-shot voice cloning from reference audio.
    audio_prompt makes Chatterbox match the accent, tone and style.
    """
    import torch
    import torchaudio
    from chatterbox.tts import ChatterboxTTS

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"  Chatterbox device: {device}")

    model = ChatterboxTTS.from_pretrained(device=device)

    # Generate with voice reference for cloning
    wav = model.generate(
        text,
        audio_prompt_path=voice_ref_path,  # zero-shot voice clone
        exaggeration=CB_EXAGGERATION,       # 0.35 = faithful to reference
        cfg_weight=CB_CFG_WEIGHT,
    )

    # Convert to MP3
    wav_path = f"{TMP_DIR}/cb_wav_{os.getpid()}.wav"
    mp3_path = f"{TMP_DIR}/cb_mp3_{os.getpid()}.mp3"
    torchaudio.save(wav_path, wav, model.sr)

    result = subprocess.run(
        ["ffmpeg", "-y", "-i", wav_path,
         "-codec:a", "libmp3lame", "-qscale:a", "2",
         "-af", "afftdn=nf=-25,loudnorm=I=-16:TP=-1.5:LRA=11",
         mp3_path],
        capture_output=True, timeout=30
    )
    if result.returncode != 0:
        raise Exception(f"ffmpeg: {result.stderr.decode()[:200]}")

    with open(mp3_path, "rb") as f:
        return f.read()


# ==========================================
# KOKORO FALLBACK
# ==========================================

def _kokoro_generate(text: str) -> bytes:
    from kokoro import KPipeline
    import soundfile as sf
    import numpy as np

    pipeline = KPipeline(lang_code=KK_LANGUAGE)
    chunks, sr = [], None
    for samples, s, _ in pipeline(text, voice=KK_VOICE_PRESET, speed=KK_SPEAKING_RATE):
        chunks.append(samples)
        if sr is None: sr = s
    if not chunks: raise Exception("Kokoro: no audio")
    audio = np.concatenate(chunks)
    buf   = io.BytesIO()
    sf.write(buf, audio, sr, format="MP3")
    return buf.getvalue()


# ==========================================
# SCRIPT CLEANING
# ==========================================

def _clean_script(script: str) -> str:
    clean = script

    # Strip Devanagari (Hindi) characters entirely — should not reach here but safety net
    clean = re.sub(r'[\u0900-\u097F]+', '', clean)

    # Strip common Hindi transliteration words that sound wrong in TTS
    hindi_words = [
        r'\bYaar\b', r'\byaar\b', r'\bBhai\b', r'\bbhai\b',
        r'\bDesh\b', r'\bdesh\b', r'\bSach mein\b', r'\bsach mein\b',
        r'\bArre\b', r'\barre\b', r'\bSuno\b', r'\bsuno\b',
        r'\bDekho\b', r'\bdekho\b', r'\bHamare\b', r'\bhamare\b',
        r'\bAapka\b', r'\baapka\b', r'\bHaan\b', r'\bhaan\b',
    ]
    for w in hindi_words:
        clean = re.sub(w, '', clean)

    clean = re.sub(r"^Fact:\s*", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bFact:\s*", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>", "", clean)
    clean = re.sub(r"<break[^>]*/?>", " ", clean)

    acronyms = [
        ("ISRO's","I.S.R.O.'s"), ("DRDO's","D.R.D.O.'s"),
        ("ISRO","I.S.R.O."),     ("DRDO","D.R.D.O."),
        ("IIT","I.I.T."),        ("IITs","I.I.T.s"),
        ("UPI","U.P.I."),        ("AIIMS","A.I.I.M.S."),
        ("NASSCOM","NAS-com"),   ("SEBI","SEE-bi"),
        ("NITI","NEE-ti"),       ("RBI","R.B.I."),
        ("GST","G.S.T."),        ("GDP","G.D.P."),
        ("EVs","E.V.s"),         ("EV","E.V."),
    ]
    for wrong, right in acronyms:
        clean = clean.replace(wrong, right)

    missions = [
        ("Chandrayaan-3","Chandra-yaan Three"),
        ("Chandrayaan-2","Chandra-yaan Two"),
        ("Chandrayaan","Chandra-yaan"),
        ("Gaganyaan","Gagan-yaan"),
        ("Mangalyaan","Mangal-yaan"),
        ("Aditya-L1","Aditya L-one"),
    ]
    for wrong, right in missions:
        clean = clean.replace(wrong, right)

    clean = clean.replace("\u20b9","rupees ").replace("%"," percent")
    clean = clean.replace("&"," and ").replace("\u2192"," to ")
    clean = re.sub(r"(\d+),00,00,000", lambda m: m.group(1)+" crore", clean)
    clean = re.sub(r"(\d+),00,000",    lambda m: m.group(1)+" lakh",  clean)
    clean = re.sub(r"\s+", " ", clean).strip()

    words = clean.split()
    print(f"  Script after clean: {len(words)} words")
    return clean


def _get_duration(path: str) -> float:
    try:
        r = subprocess.run(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=10)
        return float(r.stdout.strip())
    except Exception:
        return 25.0


@app.local_entrypoint()
def main():
    result = generate_voice.remote(
        job_id="test-clone-001",
        reviewed_script=(
            "India is building its own space station. By 2035, Indian astronauts "
            "will live and work in orbit — not as guests, but as owners. "
            "I.S.R.O. has already tested the technology. Twenty thousand engineers "
            "are making this happen right now. Will India become the fourth nation "
            "to own the sky?"
        ),
        engine_mode="inbuilt",
    )
    print(f"Engine: {result['engine']} | Duration: {result['duration']:.1f}s")


