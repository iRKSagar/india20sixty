from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json
import time
import shutil
from pathlib import Path
import boto3
from datetime import datetime, timedelta

app = Flask(__name__)

# ==========================================
# ENVIRONMENT
# ==========================================

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
LEONARDO_API_KEY = os.environ.get("LEONARDO_API_KEY")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")

TEST_MODE = os.environ.get("TEST_MODE", "true").lower() == "true"
REVIEW_MODE = os.environ.get("REVIEW_MODE", "true").lower() == "true"

TMP_DIR = "/tmp/india20sixty"

Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

# ==========================================
# R2 CLIENT
# ==========================================

def get_r2_client():
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto'
    )

# ==========================================
# STATUS UPDATES
# ==========================================

def update_status(job_id, status, data=None):
    try:
        payload = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat()
        }
        if data:
            payload.update(data)
            
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal"
            },
            json=payload,
            timeout=10
        )
    except Exception as e:
        print(f"STATUS UPDATE FAILED: {e}")

def log_step(job_id, step, message):
    print(f"[{job_id}] {step}: {message}")
    update_status(job_id, step.lower())

# ==========================================
# SCRIPT GENERATION
# ==========================================

def generate_script(topic):
    print("SCRIPT START")
    
    prompt = f"""Create a viral 25-second YouTube Shorts script about: {topic}

LANGUAGE: 70 percent English, 30 percent Hinglish (Hindi phrases for emotions/CTAs)

STRUCTURE:
1. HOOK (3 sec): Hinglish like "Socho...", "Ek minute..."
2. CONTEXT (5 sec): English with Indian context
3. INSIGHT (8 sec): Mixed Hinglish-English
4. FUTURE (5 sec): English vision
5. CTA (4 sec): Hinglish like "Aapko kya lagta? Comment karo!"

Return ONLY script text, 40-50 words, no labels."""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.8,
                "max_tokens": 200
            },
            timeout=30
        )
        r.raise_for_status()
        
        text = r.json()["choices"][0]["message"]["content"].strip()
        text = text.replace('"', '').replace("'", "")
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        script = ' '.join(lines)
        
        print(f"SCRIPT DONE: {script[:80]}...")
        return script
        
    except Exception as e:
        print(f"SCRIPT GEN FAILED: {e}")
        return f"Socho, {topic} reality ban jaye. Ye future door nahi hai. India 2060 mein yeh normal hoga. Aapko kya lagta? Comment karo!"

# ==========================================
# VISUAL PROMPTS
# ==========================================

SCENE_TEMPLATES = {
    "hook": [
        "futuristic Indian megacity sunrise lotus-shaped skyscrapers flying vehicles with rangoli LED patterns diverse Indian crowd saffron teal colors cinematic",
        "advanced AI research center India engineers in kurtas with holographic interfaces temple architecture fused with glass buildings morning light cinematic",
        "Indian spaceport rocket launching ISRO logo visible traditional lamp ceremony crowd cheering dramatic lighting cinematic",
        "smart village India 2060 traditional huts with solar roofs robots helping farmers green fields sunrise warm colors cinematic"
    ],
    "context": [
        "AI hospital India doctor in white coat with AR glasses examining patient Ganesh statue in background clean modern interior soft lighting cinematic",
        "Indian classroom 2060 students in uniform with tablets AI teacher hologram Sanskrit script on digital blackboard bright lighting cinematic",
        "high-tech Indian railway station bullet train with peacock feather design passengers in diverse Indian clothing digital signage Hindi English cinematic",
        "vertical farm Mumbai traditional marigold flowers in hydroponic towers Indian farmer monitoring with tablet sunset colors cinematic"
    ],
    "insight": [
        "quantum computer Indian lab circuit patterns resembling mandala scientist in saree with safety goggles blue gold lighting cinematic",
        "Indian ocean cleanup robotic boats with traditional boat designs Mumbai skyline background morning mist cinematic",
        "desert solar farm Rajasthan panels arranged in geometric rangoli patterns camel in foreground golden hour cinematic",
        "Indian manufacturing robot arms decorated with mehndi patterns factory floor with diya lamps warm industrial lighting cinematic"
    ],
    "future": [
        "Mars colony with Indian flag dome habitat with temple architecture astronaut with Om symbol patch red planet landscape cinematic",
        "underwater city off Kerala coast glass domes with Kerala boat design marine life Indian family looking out blue-green lighting cinematic",
        "floating city above Ganges river platforms with Varanasi ghats design holy men with tech wearables sunrise golden light cinematic",
        "Himalayan research station snow-capped peaks monastery fused with observatory prayer flags with solar panels clear sky cinematic"
    ],
    "ending": [
        "panoramic view India 2060 diverse landscapes mountains to ocean Taj Mahal preserved with holographic protection sunset patriotic colors cinematic",
        "generation of Indians elder traditional clothes youth smart wear child with AR glasses looking at futuristic city golden hour emotional cinematic",
        "Indian flag waving on moon base earthrise in background astronaut doing namaste vast space awe-inspiring cinematic"
    ]
}

def generate_visual_scenes(topic):
    scenes = []
    for stage in ["hook", "context", "insight", "future", "ending"]:
        templates = SCENE_TEMPLATES[stage]
        idx = hash(topic + stage) % len(templates)
        scenes.append({
            "stage": stage,
            "prompt": templates[idx],
            "duration": 5
        })
    return scenes

# ==========================================
# LEONARDO IMAGES
# ==========================================

def generate_image_with_retry(prompt, output_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.post(
                "https://cloud.leonardo.ai/api/rest/v1/generations",
                headers={
                    "Authorization": f"Bearer {LEONARDO_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "prompt": prompt,
                    "modelId": "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
                    "width": 1080,
                    "height": 1920,
                    "num_images": 1,
                    "presetStyle": "CINEMATIC"
                },
                timeout=30
            )
            r.raise_for_status()
            data = r.json()
            
            if "sdGenerationJob" not in data:
                raise Exception(f"Invalid response: {data}")
            
            generation_id = data["sdGenerationJob"]["generationId"]
            
            for poll in range(30):
                time.sleep(2)
                
                r = requests.get(
                    f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                    timeout=10
                )
                r.raise_for_status()
                result = r.json()
                
                if "generations_by_pk" in result:
                    images = result["generations_by_pk"].get("generated_images", [])
                    if images:
                        img_url = images[0]["url"]
                        img_r = requests.get(img_url, timeout=30)
                        img_r.raise_for_status()
                        
                        with open(output_path, "wb") as f:
                            f.write(img_r.content)
                        return True
            
            raise Exception("Generation timeout")
            
        except Exception as e:
            print(f"Image attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    
    return False

def generate_all_images(scenes, job_id):
    image_paths = []
    for i, scene in enumerate(scenes):
        log_step(job_id, "IMAGES", f"Generating scene {i+1}/5: {scene['stage']}")
        
        path = f"{TMP_DIR}/{job_id}_{i}.png"
        
        try:
            generate_image_with_retry(scene["prompt"], path)
            image_paths.append(path)
        except Exception as e:
            print(f"Failed to generate image {i}: {e}")
            if image_paths:
                shutil.copy(image_paths[-1], path)
                image_paths.append(path)
            else:
                raise Exception(f"First image failed: {e}")
    
    return image_paths

# ==========================================
# VOICE GENERATION
# ==========================================

def generate_voice(script, job_id):
    log_step(job_id, "VOICE", "Generating audio...")
    
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{VOICE_ID}"
    
    r = requests.post(
        url,
        headers={
            "xi-api-key": ELEVENLABS_API_KEY,
            "Content-Type": "application/json"
        },
        json={
            "text": script,
            "model_id": "eleven_multilingual_v2",
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.75
            }
        },
        timeout=60
    )
    r.raise_for_status()
    
    audio_path = f"{TMP_DIR}/{job_id}.mp3"
    with open(audio_path, "wb") as f:
        f.write(r.content)
    
    return audio_path

# ==========================================
# VIDEO RENDERING
# ==========================================

def get_audio_duration(audio_path):
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", audio_path
        ], capture_output=True, text=True, timeout=10)
        return float(result.stdout.strip())
    except:
        return 25

def render_video(images, audio, job_id):
    log_step(job_id, "RENDER", "Starting video render...")
    
    video_path = f"{TMP_DIR}/{job_id}.mp4"
    audio_duration = get_audio_duration(audio)
    scene_duration = audio_duration / len(images)
    
    inputs = []
    filter_parts = []
    
    for i, img in enumerate(images):
        inputs.extend(["-loop", "1", "-t", str(scene_duration), "-i", img])
        
        zoom_start = 1.0
        zoom_end = 1.15
        x_start = 0
        x_end = (i % 2) * 20
        
        filter_parts.append(
            f"[{i}:v]scale=1080:1920:force_original_aspect_ratio=decrease,"
            f"zoompan=z='min(zoom+0.0015,{zoom_end})':"
            f"x='iw/2-(iw/zoom/2)+{x_start}+({x_end}-{x_start})*on/{scene_duration*30}':"
            f"y='ih/2-(ih/zoom/2)':"
            f"d={int(scene_duration * 30)}:s=1080x1920[v{i}];"
        )
    
    concat_inputs = "".join([f"[v{i}]" for i in range(len(images))])
    filter_parts.append(f"{concat_inputs}concat=n={len(images)}:v=1:a=0[video];")
    
    cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-i", audio,
        "-filter_complex", "".join(filter_parts) + "[1:a]anull[audio]",
        "-map", "[video]",
        "-map", "[audio]",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-shortest",
        "-movflags", "+faststart",
        video_path
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    
    if result.returncode != 0:
        print(f"FFMPEG ERROR: {result.stderr}")
        raise Exception("Video render failed")
    
    if not os.path.exists(video_path) or os.path.getsize(video_path) < 100000:
        raise Exception("Video file too small or missing")
    
    return video_path

# ==========================================
# R2 UPLOAD
# ==========================================

def upload_to_r2(video_path, job_id, folder="review"):
    try:
        s3 = get_r2_client()
        key = f"{folder}/{job_id}.mp4"
        
        s3.upload_file(video_path, R2_BUCKET, key)
        
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': R2_BUCKET, 'Key': key},
            ExpiresIn=604800
        )
        
        return url
    except Exception as e:
        print(f"R2 upload failed: {e}")
        return None

# ==========================================
# YOUTUBE UPLOAD
# ==========================================

def get_youtube_token():
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": YOUTUBE_CLIENT_ID,
            "client_secret": YOUTUBE_CLIENT_SECRET,
            "refresh_token": YOUTUBE_REFRESH_TOKEN,
            "grant_type": "refresh_token"
        },
        timeout=10
    )
    r.raise_for_status()
    return r.json()["access_token"]

def upload_to_youtube(video_path, title, script, job_id):
    log_step(job_id, "UPLOAD", "Uploading to YouTube...")
    
    token = get_youtube_token()
    
    description = f"""{script}

Exploring India's future by 2060

India2060 FutureTech IndiaFuture Shorts AI Technology

Follow for daily glimpses into India's future!

This content is AI-generated for educational and entertainment purposes."""
    
    metadata = {
        "snippet": {
            "title": title[:100],
            "description": description[:5000],
            "tags": ["India 2060", "Future India", "AI", "Technology", "Shorts", "India Future"],
            "categoryId": "28"
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False
        }
    }
    
    with open(video_path, "rb") as video_file:
        r = requests.post(
            "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status",
            headers={"Authorization": f"Bearer {token}"},
            files={
                "snippet": (None, json.dumps(metadata), "application/json"),
                "video": ("video.mp4", video_file, "video/mp4")
            },
            timeout=300
        )
    
    r.raise_for_status()
    return r.json()["id"]

# ==========================================
# MAIN PIPELINE
# ==========================================

@app.route("/full-pipeline", methods=["POST"])
def pipeline():
    data = request.json
    job_id = data.get("job_id", str(uuid.uuid4()))
    topic = data.get("topic", "Future India")
    skip_review = data.get("skip_review", False)
    
    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"{'='*60}\n")
    
    try:
        update_status(job_id, "script", {"topic": topic})
        script = generate_script(topic)
        
        scenes = generate_visual_scenes(topic)
        images = generate_all_images(scenes, job_id)
        audio = generate_voice(script, job_id)
        video = render_video(images, audio, job_id)
        
        review_url = upload_to_r2(video, job_id, "review")
        print(f"REVIEW URL: {review_url}")
        
        should_review = REVIEW_MODE and not skip_review and not TEST_MODE
        
        if TEST_MODE:
            print("TEST MODE: Video generated, not uploading")
            video_id = "TEST_MODE"
            final_status = "test_complete"
        elif should_review:
            print("REVIEW MODE: Video pending approval")
            video_id = "PENDING_REVIEW"
            final_status = "pending_review"
        else:
            title = f"{topic} India2060 shorts"
            video_id = upload_to_youtube(video, title, script, job_id)
            final_status = "complete"
        
        update_status(job_id, final_status, {
            "youtube_id": video_id,
            "script": script,
            "review_url": review_url
        })
        
        # Cleanup temp files
        for img in images:
            try:
                os.remove(img)
            except Exception as e:
                print(f"Failed to remove image: {e}")
        
        try:
            os.remove(audio)
        except Exception as e:
            print(f"Failed to remove audio: {e}")
        
        print(f"\n✅ PIPELINE COMPLETE: {video_id}\n")
        
        return jsonify({
            "status": final_status,
            "job_id": job_id,
            "youtube_id": video_id,
            "review_url": review_url,
            "script": script
        })
        
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ PIPELINE FAILED: {error_msg}\n")
        
        update_status(job_id, "failed", {"error": error_msg})
        
        return jsonify({
            "status": "failed",
            "job_id": job_id,
            "error": error_msg
        }), 500

@app.route("/approve-and-publish", methods=["POST"])
def approve_and_publish():
    data = request.json
    job_id = data.get("job_id")
    
    if not job_id:
        return jsonify({"error": "job_id required"}), 400
    
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}"
            },
            timeout=10
        )
        jobs = r.json()
        
        if not jobs:
            return jsonify({"error": "Job not found"}), 404
        
        job = jobs[0]
        
        if job["status"] != "pending_review":
            return jsonify({"error": f"Job status is {job['status']}, not pending_review"}), 400
        
        s3 = get_r2_client()
        video_path = f"{TMP_DIR}/{job_id}_approve.mp4"
        
        s3.download_file(R2_BUCKET, f"review/{job_id}.mp4", video_path)
        
        title = f"{job['topic']} India2060 shorts"
        video_id = upload_to_youtube(video_path, title, job.get("script", ""), job_id)
        
        s3.copy_object(
            Bucket=R2_BUCKET,
            CopySource={'Bucket': R2_BUCKET, 'Key': f"review/{job_id}.mp4"},
            Key=f"published/{job_id}.mp4"
        )
        s3.delete_object(Bucket=R2_BUCKET, Key=f"review/{job_id}.mp4")
        
        update_status(job_id, "complete", {
            "youtube_id": video_id,
            "published_at": datetime.utcnow().isoformat()
        })
        
        try:
            os.remove(video_path)
        except Exception as e:
            print(f"Failed to remove video: {e}")
        
        return jsonify({
            "status": "published",
            "job_id": job_id,
            "youtube_id": video_id
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cleanup", methods=["POST"])
def cleanup():
    data = request.json
    mode = data.get("mode", "weekly")
    
    try:
        s3 = get_r2_client()
        
        if mode == "monthly":
            days = 30
        else:
            days = 7
        
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        deleted = []
        
        response = s3.list_objects_v2(Bucket=R2_BUCKET, Prefix="review/")
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['LastModified'].replace(tzinfo=None) < cutoff:
                    s3.delete_object(Bucket=R2_BUCKET, Key=obj['Key'])
                    deleted.append(obj['Key'])
        
        old_jobs = requests.get(
            f"{SUPABASE_URL}/rest/v1/jobs?status=eq.failed&created_at=lt.{cutoff.isoformat()}",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}"
            }
        ).json()
        
        for job in old_jobs:
            requests.delete(
                f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job['id']}",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}"
                }
            )
        
        return jsonify({
            "status": "cleaned",
            "mode": mode,
            "r2_files_deleted": len(deleted),
            "old_jobs_deleted": len(old_jobs)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "test_mode": TEST_MODE,
        "review_mode": REVIEW_MODE
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
