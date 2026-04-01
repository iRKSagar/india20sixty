import modal
import os
import json
import re
import requests
import hashlib
import hmac
from pathlib import Path
from datetime import datetime
from urllib.parse import quote

# ==========================================
# MODAL APP — PUBLISHER
# Shared upload logic used by pipeline.py AND mixer.py
# No more copy-paste between files.
# ==========================================

app = modal.App("india20sixty-publisher")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]


# ==========================================
# SANITIZE FOR YOUTUBE
# YouTube API rejects emoji, Devanagari, smart quotes, em dashes.
# Applied to BOTH title and description before every upload.
# ==========================================

def sanitize_for_youtube(text: str) -> str:
    if not text:
        return ""

    replacements = [
        ('\u2019', "'"),   # right single quote
        ('\u2018', "'"),   # left single quote
        ('\u201c', '"'),   # left double quote
        ('\u201d', '"'),   # right double quote
        ('\u2013', '-'),   # en dash
        ('\u2014', '-'),   # em dash — REJECTED by YouTube API
        ('\u2026', '...'), # ellipsis
        ('\u00a0', ' '),   # non-breaking space
        ('\u20b9', 'Rs.'), # Indian rupee sign
        ('\u2022', '-'),   # bullet
        ('\u00b7', '-'),   # middle dot
    ]
    for bad, good in replacements:
        text = text.replace(bad, good)

    # Strip ElevenLabs emotion tags
    text = re.sub(r'</?(?:excited|happy|sad|whisper|angry)[^>]*>', '', text)
    # Control chars (keep \n and \t)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    # Zero-width and invisible formatting
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)
    # Emoji — supplementary plane
    text = re.sub(u'[\U0001F000-\U0001FFFF]', '', text)
    text = re.sub(u'[\U00020000-\U0002FA1F]', '', text)
    # BMP emoji and symbols
    text = re.sub(r'[\u2600-\u26FF]', '', text)
    text = re.sub(r'[\u2700-\u27BF]', '', text)
    # Devanagari and other Indic scripts
    text = re.sub(r'[\u0900-\u097F]', '', text)  # Devanagari
    text = re.sub(r'[\u0980-\u0D7F]', '', text)  # Bengali → Malayalam
    # Collapse whitespace
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ==========================================
# UPLOAD TO R2
# S3-compatible AWS4 signed PUT.
# Returns public URL (R2_BASE_URL + key) or endpoint URL.
# Non-fatal in TEST_MODE.
# ==========================================

@app.function(image=image, secrets=secrets, timeout=180)
def upload_to_r2(file_path: str, r2_key: str) -> str:
    R2_ACCOUNT_ID    = os.environ.get("R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
    R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET        = os.environ.get("R2_BUCKET", "india20sixty")
    R2_BASE_URL      = os.environ.get("R2_BASE_URL", "")
    TEST_MODE        = os.environ.get("TEST_MODE", "true").lower() == "true"

    print(f"\n[R2 Upload] {r2_key}")

    if not R2_ACCOUNT_ID:
        print("  R2 not configured — returning local path")
        return f"file://{file_path}"

    try:
        endpoint     = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
        url          = f"{endpoint}/{R2_BUCKET}/{r2_key}"
        with open(file_path, "rb") as f:
            data = f.read()

        now          = datetime.utcnow()
        date_str     = now.strftime("%Y%m%d")
        time_str     = now.strftime("%Y%m%dT%H%M%SZ")
        content_type = "video/mp4"
        payload_hash = hashlib.sha256(data).hexdigest()

        headers = {
            "Content-Type":         content_type,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date":           time_str,
            "Host":                 f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        }

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

        public_url = f"{R2_BASE_URL.rstrip('/')}/{r2_key}" if R2_BASE_URL else url
        print(f"  R2: {len(data)//1024}KB → {public_url}")
        return public_url

    except Exception as e:
        print(f"  R2 upload failed: {e}")
        if TEST_MODE:
            return f"r2-error://{r2_key}"
        raise


# ==========================================
# GET YOUTUBE TOKEN
# Shared OAuth refresh — called by pipeline and mixer.
# ==========================================

def get_youtube_token() -> str:
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     YOUTUBE_CLIENT_ID,
            "client_secret": YOUTUBE_CLIENT_SECRET,
            "refresh_token": YOUTUBE_REFRESH_TOKEN,
            "grant_type":    "refresh_token",
        },
        timeout=10,
    )
    print(f"  OAuth response ({r.status_code}): {r.text[:200]}")
    if not r.ok:
        raise Exception(f"YouTube OAuth failed {r.status_code}: {r.text[:200]}")
    token = r.json().get("access_token")
    if not token:
        raise Exception(f"No access_token in OAuth response: {r.text[:200]}")
    return token


# ==========================================
# UPLOAD TO YOUTUBE
# multipart/related — NOT multipart/form-data.
# Sanitizes title and description before upload.
# ==========================================

@app.function(image=image, secrets=secrets, timeout=300)
def upload_to_youtube(
    video_path: str,
    title: str,
    script: str,
    topic: str,
    fact_package: dict = None,
) -> str:
    print("\n[YouTube Upload]")

    token = get_youtube_token()

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

    metadata = {
        "snippet": {
            "title":       safe_title,
            "description": description[:5000],
            "tags":        ["Future India", "India innovation", "AI",
                            "Technology", "Shorts", "India2030"],
            "categoryId":  "28",
        },
        "status": {
            "privacyStatus":          "public",
            "selfDeclaredMadeForKids": False,
        },
    }

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
        timeout=300,
    )
    print(f"  YouTube response ({r.status_code}): {r.text[:400]}")
    r.raise_for_status()
    video_id = r.json()["id"]
    print(f"  UPLOADED: https://youtube.com/watch?v={video_id}")
    return video_id


# ==========================================
# UPLOAD TO YOUTUBE SCHEDULED
# Same as above but supports publishAt for scheduled publishing.
# Used by mixer.py for human voice mode.
# ==========================================

@app.function(image=image, secrets=secrets, timeout=300)
def upload_to_youtube_scheduled(
    video_path: str,
    title: str,
    script: str,
    topic: str,
    publish_at: str = None,  # ISO datetime string or None for immediate
) -> str:
    print(f"\n[YouTube Upload — {'scheduled: ' + publish_at if publish_at else 'immediate'}]")

    token = get_youtube_token()

    safe_title = sanitize_for_youtube(title or topic[:80])[:100]
    safe_desc  = sanitize_for_youtube(script or "")[:4500]
    description = (
        f"{safe_desc}\n\n"
        "India20Sixty - India's near future, explained.\n\n"
        "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
    )

    privacy_status = "private" if publish_at else "public"
    status_obj = {
        "privacyStatus":          privacy_status,
        "selfDeclaredMadeForKids": False,
    }
    if publish_at:
        status_obj["publishAt"] = publish_at

    metadata = {
        "snippet": {
            "title":       safe_title,
            "description": description[:5000],
            "tags":        ["Future India", "India innovation", "AI",
                            "Technology", "Shorts", "India2030"],
            "categoryId":  "28",
        },
        "status": status_obj,
    }

    boundary  = "india20sixty_boundary"
    meta_json = json.dumps(metadata).encode("utf-8")
    with open(video_path, "rb") as vf:
        video_bytes = vf.read()

    body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            ).encode() + meta_json
    body += (f"\r\n--{boundary}\r\nContent-Type: video/mp4\r\n\r\n").encode()
    body += video_bytes
    body += f"\r\n--{boundary}--".encode()

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
        timeout=300,
    )
    print(f"  YouTube response ({r.status_code}): {r.text[:200]}")
    r.raise_for_status()
    video_id = r.json()["id"]
    print(f"  UPLOADED: https://youtube.com/watch?v={video_id}")
    return video_id