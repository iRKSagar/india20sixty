import modal
import os
import json
import re
import requests
import hmac
import hashlib
from pathlib import Path
from datetime import datetime
from urllib.parse import quote

# ==========================================
# MODAL APP — PUBLISHER
# Shared by pipeline.py and mixer.py
# Handles: R2 upload, YouTube OAuth, YouTube upload
# No ffmpeg. No GPT. No ElevenLabs.
# ==========================================

app = modal.App("india20sixty-publisher")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]


# ==========================================
# R2 UPLOAD
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=120)
def upload_to_r2(file_path: str, r2_key: str, file_bytes: bytes = None) -> str:
    """
    Upload to Cloudflare R2. Accepts file_bytes directly (preferred)
    or reads from file_path as fallback.
    Returns the public URL.
    """
    R2_ACCOUNT_ID     = os.environ.get("R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY_ID  = os.environ.get("R2_ACCESS_KEY_ID", "")
    R2_SECRET_KEY     = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET         = os.environ.get("R2_BUCKET", "india20sixty")
    R2_BASE_URL       = os.environ.get("R2_BASE_URL", "").rstrip("/")
    TEST_MODE         = os.environ.get("TEST_MODE", "true").lower() == "true"

    print(f"[R2 Upload] {r2_key}")

    if not R2_ACCOUNT_ID:
        print("  R2 not configured — returning placeholder")
        return f"file://{file_path}"

    try:
        endpoint     = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        url          = f"{endpoint}/{R2_BUCKET}/{r2_key}"
        now          = datetime.utcnow()
        date_str     = now.strftime("%Y%m%d")
        time_str     = now.strftime("%Y%m%dT%H%M%SZ")
        content_type = "video/mp4"

        # Use bytes directly if provided
        if file_bytes:
            data = file_bytes
        else:
            with open(file_path, "rb") as f:
                data = f.read()

        payload_hash   = hashlib.sha256(data).hexdigest()
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
        cred_scope     = f"{date_str}/auto/s3/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", time_str, cred_scope,
            hashlib.sha256(canonical.encode()).hexdigest(),
        ])

        def sign(key, msg):
            return hmac.new(key, msg.encode(), hashlib.sha256).digest()

        signing_key = sign(
            sign(sign(sign(
                f"AWS4{R2_SECRET_KEY}".encode(), date_str), "auto"), "s3"),
            "aws4_request"
        )
        signature = hmac.new(
            signing_key, string_to_sign.encode(), hashlib.sha256
        ).hexdigest()

        headers = {
            "Content-Type":          content_type,
            "x-amz-content-sha256":  payload_hash,
            "x-amz-date":            time_str,
            "Host":                  f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={R2_ACCESS_KEY_ID}/{cred_scope},"
                f"SignedHeaders={signed_headers},Signature={signature}"
            ),
        }

        r = requests.put(url, data=data, headers=headers, timeout=120)
        r.raise_for_status()

        public_url = f"{R2_BASE_URL}/{r2_key}" if R2_BASE_URL else url
        print(f"  R2: {len(data) // 1024}KB → {public_url}")
        return public_url

    except Exception as e:
        print(f"  R2 upload failed: {e}")
        if TEST_MODE:
            return f"r2-error://{r2_key}"
        raise


# ==========================================
# YOUTUBE HELPERS
# ==========================================

def sanitize_for_youtube(text: str) -> str:
    """
    Strip all characters YouTube API rejects.
    Applies to both title and description.
    Rule: emoji, Devanagari, smart quotes, em dashes, zero-width chars → removed or replaced.
    """
    if not text:
        return ""
    replacements = [
        ("\u2019", "'"), ("\u2018", "'"), ("\u201c", '"'), ("\u201d", '"'),
        ("\u2013", "-"), ("\u2014", "-"), ("\u2026", "..."), ("\u00a0", " "),
        ("\u20b9", "Rs."), ("\u2022", "-"), ("\u00b7", "-"),
    ]
    for bad, good in replacements:
        text = text.replace(bad, good)
    # Strip ElevenLabs emotion tags
    text = re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>", "", text)
    # Control chars
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
    # Zero-width / invisible
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]", "", text)
    # Emoji supplementary plane
    text = re.sub("[\U0001F000-\U0001FFFF]", "", text)
    text = re.sub("[\U00020000-\U0002FA1F]", "", text)
    # BMP emoji blocks
    text = re.sub(r"[\u2600-\u26FF]", "", text)
    text = re.sub(r"[\u2700-\u27BF]", "", text)
    # Non-Latin scripts
    text = re.sub(r"[\u0900-\u097F]", "", text)   # Devanagari
    text = re.sub(r"[\u0980-\u0D7F]", "", text)   # other Indian scripts
    # Whitespace
    text = re.sub(r" {2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _get_oauth_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Refresh YouTube OAuth token. Raises on failure."""
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type":    "refresh_token",
        },
        timeout=10,
    )
    print(f"  OAuth response ({r.status_code}): {r.text[:150]}")
    if not r.ok:
        raise Exception(f"YouTube OAuth failed {r.status_code}: {r.text[:200]}")
    token = r.json().get("access_token")
    if not token:
        raise Exception(f"No access_token in OAuth response: {r.text[:200]}")
    return token


# ==========================================
# YOUTUBE UPLOAD
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.25, memory=512, timeout=300)
def upload_to_youtube(
    video_path: str,
    title: str,
    description: str,
    tags: list = None,
    publish_at: str = None,
    video_bytes: bytes = None,   # preferred — avoids cross-container path issues
) -> str:
    """
    Upload a video file to YouTube via multipart/related upload.
    Accepts video_bytes directly (preferred) or video_path as fallback.
    Returns the YouTube video ID.
    """
    YOUTUBE_CLIENT_ID     = os.environ["YOUTUBE_CLIENT_ID"]
    YOUTUBE_CLIENT_SECRET = os.environ["YOUTUBE_CLIENT_SECRET"]
    YOUTUBE_REFRESH_TOKEN = os.environ["YOUTUBE_REFRESH_TOKEN"]

    safe_title = sanitize_for_youtube(title)[:100]
    if not safe_title.strip():
        safe_title = "India Future Tech"
    safe_desc = sanitize_for_youtube(description)[:5000]

    print(f"\n[YouTube Upload] {safe_title}")

    token = _get_oauth_token(YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN)

    privacy  = "private" if publish_at else "public"
    status   = {"privacyStatus": privacy, "selfDeclaredMadeForKids": False}
    if publish_at:
        status["publishAt"] = publish_at

    metadata = {
        "snippet": {
            "title":       safe_title,
            "description": safe_desc,
            "tags":        tags or ["Future India", "India innovation", "AI", "Technology", "Shorts"],
            "categoryId":  "28",
        },
        "status": status,
    }

    boundary  = "india20sixty_upload_boundary"
    meta_json = json.dumps(metadata).encode("utf-8")

    # Use bytes directly if provided, otherwise read from path
    if not video_bytes:
        with open(video_path, "rb") as vf:
            video_bytes = vf.read()

    body = (
        f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
    ).encode() + meta_json
    body += (f"\r\n--{boundary}\r\nContent-Type: video/mp4\r\n\r\n").encode()
    body += video_bytes
    body += f"\r\n--{boundary}--".encode()

    print(f"  Uploading {len(video_bytes) // 1024}KB...")
    r = requests.post(
        "https://www.googleapis.com/upload/youtube/v3/videos"
        "?uploadType=multipart&part=snippet,status",
        headers={
            "Authorization":  f"Bearer {token}",
            "Content-Type":   f"multipart/related; boundary={boundary}",
            "Content-Length": str(len(body)),
        },
        data=body,
        timeout=300,
    )
    print(f"  YouTube response ({r.status_code}): {r.text[:300]}")
    r.raise_for_status()
    video_id = r.json()["id"]
    print(f"  UPLOADED: https://youtube.com/watch?v={video_id}")
    return video_id


# ==========================================
# GENERATE TITLE (shared utility)
# ==========================================

@app.function(image=image, secrets=secrets, cpu=0.25, memory=256, timeout=30)
def generate_title(topic: str, key_fact: str = "", hook_style: str = "") -> str:
    """
    Generate a YouTube Shorts title via GPT.
    Returns a sanitized plain-ASCII title under 100 chars.
    """
    import random
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

    hooks = [
        "Question hook: start with Why/How/What",
        "Shock stat: lead with the most surprising number",
        "Contrast: India Before vs After",
        "Timeline: 5 Years From Now / By 2030",
        "Revelation: Nobody Talks About This",
    ]
    prompt = f"""Write a YouTube Shorts title.
Topic: {topic}
Key fact: {key_fact}
Hook pattern: {hook_style or random.choice(hooks)}
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
            timeout=15,
        )
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip().strip('"').strip("'")
        raw = re.sub(r"[\U0001F000-\U0001FFFF]", "", raw)
        raw = re.sub(r"[\u2600-\u27BF]", "", raw).strip()
        if raw:
            return raw[:95]
    except Exception as e:
        print(f"  Title generation failed: {e}")

    # Safe ASCII fallbacks
    import random
    options = [
        f"India's {topic[:45]} - The Real Story",
        "Why Nobody Is Talking About This India Story",
        f"What Is Actually Happening With {topic[:40]}",
        f"India Just Changed The Game - {topic[:35]}",
        f"The Truth About {topic[:50]}",
    ]
    return random.choice(options)[:95]


if __name__ == "__main__":
    print("publisher.py — test")
