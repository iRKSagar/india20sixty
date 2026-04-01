import modal
import os
import subprocess
import json
import requests
import traceback
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP — MIXER
# Final mix: video + voice + music → YouTube
# Called from dashboard after human records voice
# ==========================================

app = modal.App("india20sixty-mixer")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg", "curl")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR = "/tmp/mixer"

MUSIC_TRACKS = {
    "epic_01":      {"label": "Epic Rise",         "category": "Epic",      "bpm": 120},
    "hopeful_01":   {"label": "Hopeful Morning",   "category": "Hopeful",   "bpm": 95},
    "tech_01":      {"label": "Digital Pulse",     "category": "Tech",      "bpm": 128},
    "emotional_01": {"label": "Stirring Moment",   "category": "Emotional", "bpm": 85},
    "neutral_01":   {"label": "Subtle Background", "category": "Neutral",   "bpm": 100},
}

# ── HEALTH ────────────────────────────────────────────────────────

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="GET")
def health():
    return {
        "status":  "healthy",
        "service": "india20sixty-mixer",
        "version": "1.0",
        "tracks":  list(MUSIC_TRACKS.keys()),
    }


# ── MIX TRIGGER ───────────────────────────────────────────────────

@app.function(image=image, secrets=secrets)
@modal.fastapi_endpoint(method="POST")
def mix(data: dict):
    """
    Called from dashboard when human recording is done.
    data = {
        job_id:         str,
        video_url:      str,   # R2 signed URL for silent video
        voice_url:      str,   # R2 signed URL for voice recording
        music_track:    str,   # track key from MUSIC_TRACKS
        music_volume:   float, # 0.0–0.2, default 0.08
        publish_at:     str,   # ISO datetime or null for immediate
        voice_offset_ms: int,  # milliseconds to shift voice (default 0)
    }
    """
    job_id        = data.get("job_id")
    video_url     = data.get("video_url")
    voice_url     = data.get("voice_url")
    music_track   = data.get("music_track", "neutral_01")
    music_vol     = float(data.get("music_volume", 0.08))
    publish_at    = data.get("publish_at")
    voice_offset  = int(data.get("voice_offset_ms", 0))
    upload_only   = data.get("upload_only", False)  # CBDP: video already has audio
    title_override = data.get("title")              # CBDP: pre-generated title

    if not job_id or not video_url:
        return {"status": "error", "message": "Missing job_id or video_url"}

    # upload_only = CBDP review publish — video already rendered with audio
    if upload_only:
        if not voice_url and not upload_only:
            return {"status": "error", "message": "Missing voice_url"}

    print(f"{'UPLOAD-ONLY' if upload_only else 'MIX'} START: {job_id}")
    do_mix.spawn(
        job_id=job_id,
        video_url=video_url,
        voice_url=voice_url,
        music_track=music_track,
        music_vol=music_vol,
        publish_at=publish_at,
        voice_offset=voice_offset,
        upload_only=upload_only,
        title_override=title_override,
    )
    return {"status": "mixing" if not upload_only else "publishing", "job_id": job_id}


# ── MAIN MIX FUNCTION ─────────────────────────────────────────────

@app.function(image=image, secrets=secrets, cpu=2.0, memory=1024, timeout=300)
def do_mix(job_id, video_url, voice_url, music_track, music_vol,
           publish_at, voice_offset, upload_only=False, title_override=None):

    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    SUPABASE_URL          = os.environ["SUPABASE_URL"]
    SUPABASE_ANON_KEY     = os.environ["SUPABASE_ANON_KEY"]
    YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    R2_BASE_URL           = os.environ.get("R2_BASE_URL", "")
    TEST_MODE             = os.environ.get("TEST_MODE", "true").lower() == "true"

    video_path = f"{TMP_DIR}/{job_id}_video.mp4"
    voice_path = f"{TMP_DIR}/{job_id}_voice.webm"
    music_path = f"{TMP_DIR}/{job_id}_music.mp3"
    final_path = f"{TMP_DIR}/{job_id}_final.mp4"

    def sb_patch(endpoint, data):
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/{endpoint}",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal"},
            json=data, timeout=10
        )

    def sb_get(endpoint):
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{endpoint}",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
            timeout=10
        )
        return r.json()

    def update_status(status, extra=None):
        payload = {"status": status, "updated_at": datetime.utcnow().isoformat()}
        if extra:
            payload.update(extra)
        sb_patch(f"jobs?id=eq.{job_id}", payload)

    def run_ffmpeg(cmd, label, timeout=120):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            print(f"  ffmpeg [{label}] FAILED:\n{result.stderr[-400:]}")
            raise Exception(f"{label}: {result.stderr[-150:]}")
        return result

    def download(url, path, label):
        print(f"  Downloading {label}...")
        r = requests.get(url, timeout=60, stream=True)
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        size = os.path.getsize(path)
        print(f"  {label}: {size//1024}KB")
        return size

    def get_oauth_token():
        """Get YouTube OAuth token with clear readiness logging."""
        print(f"  OAuth check — client_id present: {bool(YOUTUBE_CLIENT_ID)}")
        print(f"  OAuth check — refresh_token present: {bool(YOUTUBE_REFRESH_TOKEN)}")
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id":     YOUTUBE_CLIENT_ID,
                  "client_secret":  YOUTUBE_CLIENT_SECRET,
                  "refresh_token":  YOUTUBE_REFRESH_TOKEN,
                  "grant_type":     "refresh_token"},
            timeout=10
        )
        print(f"  OAuth response ({r.status_code}): {r.text[:120]}")
        r.raise_for_status()
        token = r.json().get("access_token")
        if not token:
            raise Exception("OAuth returned no access_token")
        print("  OAuth OK — token acquired")
        return token

    def get_title_and_description():
        """Get job metadata for YouTube upload."""
        jobs = sb_get(f"jobs?id=eq.{job_id}&select=topic,script_package,council_score,cluster")
        job  = jobs[0] if jobs else {}
        topic       = job.get("topic", "India's Future")
        script_pkg  = job.get("script_package") or {}
        script_text = script_pkg.get("text", "")
        fact_anchor = script_pkg.get("fact_anchor") or {}
        source_line = f"\nSource: {fact_anchor.get('source','')}\n" \
                      if fact_anchor.get("found") else ""

        if title_override:
            title = title_override[:100]
        else:
            title = topic[:55]
            try:
                tr = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
                             "Content-Type": "application/json"},
                    json={"model": "gpt-4o-mini",
                          "messages": [{"role": "user",
                                        "content": f"Write a YouTube Shorts title for: {topic}\n"
                                                   "Under 60 chars. NO emoji. Plain English only. "
                                                   "Clickable and intriguing. Return only the title."}],
                          "temperature": 0.9, "max_tokens": 60},
                    timeout=15
                )
                if tr.status_code == 200:
                    title = tr.json()["choices"][0]["message"]["content"].strip().strip('"')[:95]
            except Exception as e:
                print(f"  Title gen failed (non-fatal): {e}")

        description = (
            f"{script_text}\n\n{source_line}"
            "India20Sixty - India's near future, explained.\n\n"
            "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
        )
        return title, description[:5000]

    def upload_to_youtube(video_file_path, title, description):
        """Upload video to YouTube using multipart/related."""
        import re as _re
        import json as _json

        def sanitize(text):
            if not text: return ""
            text = text.replace('\u2019',"'").replace('\u2018',"'")
            text = text.replace('\u201c','"').replace('\u201d','"')
            text = text.replace('\u2013','-').replace('\u2014','-')
            text = text.replace('\u2026','...').replace('\u00a0',' ')
            text = text.replace('\u20b9','Rs.').replace('₹','Rs.')
            text = _re.sub(r'</?(?:excited|happy|sad|whisper|angry)[^>]*>','',text)
            text = _re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]','',text)
            text = _re.sub(r'[\u200b-\u200f\u202a-\u202e\ufeff]','',text)
            text = _re.sub(u'[\U0001F000-\U0001FFFF]','',text)
            text = _re.sub(r'[\u2600-\u26FF\u2700-\u27BF]','',text)
            text = _re.sub(r'[\u0900-\u0D7F]','',text)
            return text.strip()

        safe_title = sanitize(title)[:100]
        safe_desc  = sanitize(description)[:5000]

        token = get_oauth_token()

        privacy_status = "private" if publish_at else "public"
        status_obj = {"privacyStatus": privacy_status,
                      "selfDeclaredMadeForKids": False}
        if publish_at:
            status_obj["publishAt"] = publish_at

        metadata = {
            "snippet": {
                "title":       safe_title,
                "description": safe_desc,
                "tags":        ["Future India","India innovation","AI",
                                "Technology","Shorts","India2030"],
                "categoryId":  "28"
            },
            "status": status_obj
        }

        boundary  = "india20sixty_boundary"
        meta_json = _json.dumps(metadata).encode("utf-8")
        with open(video_file_path, "rb") as vf:
            video_bytes = vf.read()

        body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                ).encode() + meta_json
        body += (f"\r\n--{boundary}\r\nContent-Type: video/mp4\r\n\r\n").encode()
        body += video_bytes
        body += f"\r\n--{boundary}--".encode()

        print(f"  Uploading {len(video_bytes)//1024}KB to YouTube...")
        r = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos"
            "?uploadType=multipart&part=snippet,status",
            headers={
                "Authorization":  f"Bearer {token}",
                "Content-Type":   f"multipart/related; boundary={boundary}",
                "Content-Length": str(len(body)),
            },
            data=body,
            timeout=300
        )
        print(f"  YouTube response ({r.status_code}): {r.text[:200]}")
        r.raise_for_status()
        return r.json()["id"]

    try:
        print(f"\n{'='*50}")
        print(f"{'UPLOAD-ONLY' if upload_only else 'MIXER'}: {job_id}")
        print(f"Publish at: {publish_at or 'immediate'}")
        print(f"{'='*50}\n")

        # ── PATH A: UPLOAD-ONLY ───────────────────────────────────
        # Video already has audio baked in — skip all mixing
        if upload_only:
            update_status("upload")

            # 1. Download video from R2
            download(video_url, video_path, "video")

            final_path = video_path  # use as-is

            if TEST_MODE:
                print("TEST MODE — skipping YouTube upload")
                update_status("complete", {"youtube_id": "TEST_UPLOAD_ONLY"})
                return

            # 2. Get metadata
            title, description = get_title_and_description()

            # 3. Upload directly to YouTube
            print(f"\n  Uploading: {title}")
            youtube_id = upload_to_youtube(final_path, title, description)
            print(f"  YouTube: https://youtube.com/watch?v={youtube_id}")

            # 4. Update job
            update_status("complete", {
                "youtube_id":   youtube_id,
                "scheduled_at": publish_at,
                "error":        None
            })

            # Analytics
            try:
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/analytics",
                    headers={"apikey": SUPABASE_ANON_KEY,
                             "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                             "Content-Type": "application/json"},
                    json={"video_id": job_id, "youtube_views": 0,
                          "youtube_likes": 0, "comment_count": 0,
                          "score": 0,
                          "created_at": datetime.utcnow().isoformat()},
                    timeout=5
                )
            except Exception:
                pass

            for f in [video_path]:
                try: os.remove(f)
                except Exception: pass

            print(f"\nUPLOAD-ONLY COMPLETE: {youtube_id}")
            return

        # ── PATH B: FULL MIX ──────────────────────────────────────
        update_status("mixing")

        # 1. Download video
        download(video_url, video_path, "video")

        # 2. Download voice recording
        download(voice_url, voice_path, "voice")

        # 3. Get music from R2
        music_url = f"{R2_BASE_URL}/music/{music_track}.mp3" if R2_BASE_URL else None
        has_music = False
        if music_url:
            try:
                download(music_url, music_path, "music")
                has_music = True
            except Exception as e:
                print(f"  Music download failed (non-fatal): {e}")

        # 4. Get video duration
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", video_path],
            capture_output=True, text=True, timeout=10
        )
        video_dur = float(probe.stdout.strip()) if probe.returncode == 0 else 30.0
        print(f"  Video duration: {video_dur:.1f}s")

        # 5. Convert voice to normalized WAV
        voice_wav = f"{TMP_DIR}/{job_id}_voice.wav"
        offset_s  = voice_offset / 1000.0

        run_ffmpeg([
            "ffmpeg", "-y",
            "-i", voice_path,
            "-af", "highpass=f=80,lowpass=f=12000,loudnorm=I=-16:TP=-1.5:LRA=11",
            "-ar", "44100", "-ac", "1",
            voice_wav
        ], "voice-normalize", timeout=60)

        # 6. Mix video + voice (+ music if available)
        fade_st = max(0, video_dur - 1.5)

        if has_music:
            filter_complex = (
                f"[1:a]adelay={voice_offset}|{voice_offset},volume=1.0[v];"
                f"[2:a]aloop=loop=-1:size=44100000,atrim=duration={video_dur:.3f},"
                f"volume={music_vol:.3f},"
                f"afade=t=out:st={fade_st:.3f}:d=1.5[m];"
                f"[v][m]amix=inputs=2:duration=longest:dropout_transition=2,"
                f"loudnorm=I=-16:TP=-1.5:LRA=11[aout]"
            )
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", video_path, "-i", voice_wav, "-i", music_path,
                "-filter_complex", filter_complex,
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                final_path
            ], "final-mix-music", timeout=120)
        else:
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", video_path, "-i", voice_wav,
                "-itsoffset", str(offset_s),
                "-map", "0:v", "-map", "1:a",
                "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                final_path
            ], "final-mix-voice-only", timeout=120)

        final_size = os.path.getsize(final_path)
        print(f"  Final: {final_size//1024}KB")
        if final_size < 100_000:
            raise Exception(f"Final video too small: {final_size}")

        if TEST_MODE:
            print("TEST MODE — skipping YouTube upload")
            update_status("complete", {"youtube_id": "TEST_MIX_COMPLETE"})
            return

        # 7. Get metadata + upload
        title, description = get_title_and_description()
        print(f"\n  Uploading: {title}")
        youtube_id = upload_to_youtube(final_path, title, description)
        print(f"  YouTube: https://youtube.com/watch?v={youtube_id}")

        # 8. Update job
        update_status("complete", {
            "youtube_id":   youtube_id,
            "scheduled_at": publish_at,
            "error":        None
        })

        # Analytics
        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/analytics",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json"},
                json={"video_id": job_id, "youtube_views": 0,
                      "youtube_likes": 0, "comment_count": 0,
                      "score": 0,
                      "created_at": datetime.utcnow().isoformat()},
                timeout=5
            )
        except Exception:
            pass

        # Cleanup
        for f in [video_path, voice_path, music_path, final_path]:
            try: os.remove(f)
            except Exception: pass
        try: os.remove(voice_wav)
        except Exception: pass

        print(f"\nMIX COMPLETE: {youtube_id}")

    except Exception as e:
        msg = str(e)
        print(f"\nMIX FAILED: {msg}\n{traceback.format_exc()}")
        update_status("failed", {"error": f"Mix failed: {msg[:400]}"})
        raise

    def sb_patch(endpoint, data):
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/{endpoint}",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal"},
            json=data, timeout=10
        )

    def sb_get(endpoint):
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{endpoint}",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
            timeout=10
        )
        return r.json()

    def update_status(status, extra=None):
        payload = {"status": status, "updated_at": datetime.utcnow().isoformat()}
        if extra:
            payload.update(extra)
        sb_patch(f"jobs?id=eq.{job_id}", payload)

    def run_ffmpeg(cmd, label, timeout=120):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            print(f"  ffmpeg [{label}] FAILED:\n{result.stderr[-400:]}")
            raise Exception(f"{label}: {result.stderr[-150:]}")
        return result

    def download(url, path, label):
        print(f"  Downloading {label}...")
        r = requests.get(url, timeout=60, stream=True)
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        size = os.path.getsize(path)
        print(f"  {label}: {size//1024}KB")
        return size

    try:
        update_status("upload" if upload_only else "mixing")
        print(f"\n{'='*50}")
        print(f"{'UPLOAD-ONLY (CBDP)' if upload_only else 'MIXER'}: {job_id}")
        print(f"Publish at: {publish_at or 'immediate'}")
        print(f"{'='*50}\n")

        # 1. Download video
        download(video_url, video_path, "video")

        # CBDP upload_only path — video already has audio baked in
        # Skip all mixing, go straight to YouTube upload
        if upload_only:
            final_path = video_path
            print("  Upload-only mode: skipping mix, uploading as-is")

        else:
            # 2. Download voice recording
            download(voice_url, voice_path, "voice")

            # 3. Get music from R2 or use silence if track not found
            music_url = f"{R2_BASE_URL}/music/{music_track}.mp3" if R2_BASE_URL else None
            has_music = False
            if music_url:
                try:
                    download(music_url, music_path, "music")
                    has_music = True
                except Exception as e:
                    print(f"  Music download failed (non-fatal): {e}")

            # 4. Get video duration
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                capture_output=True, text=True, timeout=10
            )
            video_dur = float(probe.stdout.strip()) if probe.returncode == 0 else 30.0
            print(f"  Video duration: {video_dur:.1f}s")

            # 5. Convert voice to WAV for reliable mixing
            voice_wav = f"{TMP_DIR}/{job_id}_voice.wav"
            offset_s  = voice_offset / 1000.0

            # Convert voice recording (WebM/WAV/any) → normalized WAV
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", voice_path,
                "-af", "highpass=f=80,lowpass=f=12000,"
                       "loudnorm=I=-16:TP=-1.5:LRA=11",
                "-ar", "44100", "-ac", "1",
                voice_wav
            ], "voice-normalize", timeout=60)

        # 6. Build ffmpeg mix command
        fade_st = max(0, video_dur - 1.5)

        if has_music:
            # Mix: video + offset voice + looped music
            filter_complex = (
                # Voice with offset delay if needed
                f"[1:a]adelay={voice_offset}|{voice_offset},volume=1.0[v];"
                # Music: loop to fill video duration, fade out at end
                f"[2:a]aloop=loop=-1:size=44100000,atrim=duration={video_dur:.3f},"
                f"volume={music_vol:.3f},"
                f"afade=t=out:st={fade_st:.3f}:d=1.5[m];"
                # Mix voice + music
                f"[v][m]amix=inputs=2:duration=longest:dropout_transition=2,"
                f"loudnorm=I=-16:TP=-1.5:LRA=11[aout]"
            )
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", video_path,
                "-i", voice_wav,
                "-i", music_path,
                "-filter_complex", filter_complex,
                "-map", "0:v",
                "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                final_path
            ], "final-mix-music", timeout=120)
        else:
            # Mix: video + voice only (no music)
            run_ffmpeg([
                "ffmpeg", "-y",
                "-i", video_path,
                "-i", voice_wav,
                f"-itsoffset", str(offset_s),
                "-map", "0:v", "-map", "1:a",
                "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
                "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                final_path
            ], "final-mix-voice-only", timeout=120)

        final_size = os.path.getsize(final_path)
        print(f"  Final: {final_size//1024}KB")

        if final_size < 100_000:
            raise Exception(f"Final video too small: {final_size}")

        if TEST_MODE:
            print("TEST MODE — skipping YouTube upload")
            update_status("complete", {"youtube_id": "TEST_MIX_COMPLETE"})
            return

        # 7. Get job details for title/description
        jobs = sb_get(f"jobs?id=eq.{job_id}&select=topic,script_package,council_score,cluster")
        job  = jobs[0] if jobs else {}
        topic        = job.get("topic", "India's Future")
        script_pkg   = job.get("script_package") or {}
        script_text  = script_pkg.get("text", "")
        fact_anchor  = script_pkg.get("fact_anchor") or {}
        source_line  = f"\nSource: {fact_anchor.get('source','')}\n" \
                       if fact_anchor.get("found") else ""

        # 8. YouTube OAuth
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={"client_id":     YOUTUBE_CLIENT_ID,
                  "client_secret":  YOUTUBE_CLIENT_SECRET,
                  "refresh_token":  YOUTUBE_REFRESH_TOKEN,
                  "grant_type":     "refresh_token"},
            timeout=10
        )
        r.raise_for_status()
        token = r.json()["access_token"]

        # 9. Build metadata
        description = (
            f"{script_text}\n\n{source_line}"
            "India20Sixty - India's near future, explained.\n\n"
            "#IndiaFuture #FutureTech #India #Shorts #AI #Technology #Innovation"
        )

        # Title: use override (CBDP) or generate fresh via GPT
        if title_override:
            title = title_override[:100]
        else:
            title = topic[:55]
            try:
                tr = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
                             "Content-Type": "application/json"},
                    json={"model": "gpt-4o-mini",
                          "messages": [{"role": "user",
                                        "content": f"Write a YouTube Shorts title for: {topic}\nUnder 60 chars, NO emoji, plain English only, clickable. Return only the title."}],
                          "temperature": 0.9, "max_tokens": 60},
                    timeout=15
                )
                if tr.status_code == 200:
                    title = tr.json()["choices"][0]["message"]["content"].strip().strip('"')[:95]
            except Exception as e:
                print(f"  Title gen failed (non-fatal): {e}")

        # Scheduled publish
        privacy_status = "private" if publish_at else "public"
        status_obj = {
            "privacyStatus":          privacy_status,
            "selfDeclaredMadeForKids": False
        }
        if publish_at:
            status_obj["publishAt"] = publish_at

        metadata = {
            "snippet": {
                "title":       title[:100],
                "description": description[:5000],
                "tags":        ["Future India", "India innovation", "AI",
                                "Technology", "Shorts", "India2030"],
                "categoryId":  "28"
            },
            "status": status_obj
        }

        # 10. Upload to YouTube
        print(f"\n  Uploading: {title}")
        with open(final_path, "rb") as vf:
            upload_r = requests.post(
                "https://www.googleapis.com/upload/youtube/v3/videos"
                "?uploadType=multipart&part=snippet,status",
                headers={"Authorization": f"Bearer {token}"},
                files={"snippet": (None, json.dumps(metadata), "application/json"),
                       "video":   ("video.mp4", vf, "video/mp4")},
                timeout=300
            )
        upload_r.raise_for_status()
        youtube_id = upload_r.json()["id"]
        print(f"  YouTube: https://youtube.com/watch?v={youtube_id}")

        # 11. Update job
        update_status("complete", {
            "youtube_id":    youtube_id,
            "scheduled_at":  publish_at,
            "error":         None
        })

        # 12. Insert analytics record
        try:
            requests.post(
                f"{SUPABASE_URL}/rest/v1/analytics",
                headers={"apikey": SUPABASE_ANON_KEY,
                         "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                         "Content-Type": "application/json"},
                json={"video_id": job_id, "youtube_views": 0, "youtube_likes": 0,
                      "comment_count": 0, "score": 0,
                      "created_at": datetime.utcnow().isoformat()},
                timeout=5
            )
        except Exception:
            pass

        # Cleanup
        for f in [video_path, voice_path, voice_wav, music_path, final_path]:
            try: os.remove(f)
            except Exception: pass

        print(f"\nMIX COMPLETE: {youtube_id}")

    except Exception as e:
        msg = str(e)
        print(f"\nMIX FAILED: {msg}\n{traceback.format_exc()}")
        update_status("failed", {"error": f"Mix failed: {msg[:400]}"})
        raise


# ── LOCAL TEST ────────────────────────────────────────────────────

@app.local_entrypoint()
def main():
    print("Mixer health check...")
    print(health.remote())