from flask import Flask, request, jsonify
import requests
import os
import subprocess
import json
import time
import uuid
from pathlib import Path

app = Flask(__name__)

# ==========================================
# ENVIRONMENT
# ==========================================

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
LEONARDO_API_KEY = os.environ.get("LEONARDO_API_KEY")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY")
VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")  # Adam voice

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

YOUTUBE_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

TEST_MODE = os.environ.get("TEST_MODE", "true").lower() == "true"
TMP_DIR = "/tmp/india20sixty"

# Ensure temp directory exists
Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

# ==========================================
# STATUS UPDATES
# ==========================================

def update_status(job_id, status, data=None):
    """Update job status in Supabase with optional data"""
    try:
        payload = {
            "status": status,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
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
    """Log step to console and update status"""
    print(f"[{job_id}] {step}: {message}")
    update_status(job_id, step.lower())

# ==========================================
# SCRIPT GENERATION (Hook-First)
# ==========================================

def generate_script(topic):
    """Generate viral short-form script with pattern interrupt"""
    
    prompt = f"""Create a viral 25-second YouTube Shorts script about: {topic}

CRITICAL RULES:
- FIRST LINE must be a pattern interrupt (shocking fact, bold prediction, or curiosity gap)
- Use short punchy sentences (5-8 words max)
- Include 1 mind-blowing statistic or prediction
- End with open-loop question
- Total: 40-50 words max

STRUCTURE:
Line 1: HOOK (pattern interrupt)
Line 2-3: Context
Line 4-5: Insight/shocking fact
Line 6: Future prediction
Line 7: Question to audience

EXAMPLE HOOKS:
"India's about to disappear 80% of its doctors..."
"By 2060, Mumbai won't exist where it is today..."
"This Indian village has zero teachers. Yet students rank #1..."

Write only the script, no labels, no markdown."""

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
                "max_tokens": 150
            },
            timeout=30
        )
        r.raise_for_status()
        script = r.json()["choices"][0]["message"]["content"].strip()
        
        # Clean up script
        script = script.replace('"', '').replace("'", "")
        lines = [line.strip() for line in script.split('\n') if line.strip()]
        script = ' '.join(lines)
        
        print(f"GENERATED SCRIPT: {script[:100]}...")
        return script
        
    except Exception as e:
        print(f"SCRIPT GEN FAILED: {e}")
        # Fallback script
        return f"{topic}. Imagine how India transforms by 2060. The future is closer than you think. What do you believe?"

# ==========================================
# VISUAL SCENE GENERATION
# ==========================================

SCENE_TEMPLATES = {
    "hook": [
        "cinematic wide shot futuristic Indian city skyline 2060, neon lights, flying vehicles, ultra detailed, 8k, dramatic lighting",
        "close up Indian astronaut helmet reflection showing space station, cinematic lighting, ultra realistic",
        "aerial view smart Indian megacity with vertical gardens, sunset, cinematic, 8k"
    ],
    "context": [
        "futuristic Indian hospital interior with holographic displays, doctors with AR glasses, cinematic",
        "Indian farmer controlling drone swarm over green fields, sunset, cinematic composition",
        "modern Indian classroom with AI teacher hologram, students with tablets, bright lighting"
    ],
    "insight": [
        "futuristic Indian laboratory with quantum computer, scientists in traditional clothes, cinematic",
        "Indian hyperloop station with passengers boarding, sleek design, morning light",
        "underwater view of floating Indian city with glass domes, marine life, cinematic"
    ],
    "future": [
        "Indian space elevator reaching into clouds, sunrise, monumental scale, cinematic",
        "Mars colony with Indian architecture, red planet landscape, earth in sky, cinematic",
        "futuristic Indian temple with holographic rituals, blend of tradition and tech, sunset"
    ],
    "ending": [
        "wide shot Indian flag on moon base, earthrise in background, patriotic, cinematic",
        "time lapse futuristic Indian city day to night, lights coming on, cinematic",
        "portrait hopeful Indian child looking at futuristic city, golden hour, cinematic"
    ]
}

def generate_visual_scenes(topic):
    """Generate 5 cinematic scenes based on topic keywords"""
    
    # Extract keyword from topic
    keyword = topic.lower()
    
    # Select scenes based on topic relevance
    if "doctor" in keyword or "hospital" in keyword or "health" in keyword:
        theme = "health"
    elif "space" in keyword or "mars" in keyword or "moon" in keyword:
        theme = "space"
    elif "city" in keyword or "urban" in keyword or "traffic" in keyword:
        theme = "city"
    elif "farm" in keyword or "agriculture" in keyword or "food" in keyword:
        theme = "agriculture"
    else:
        theme = "general"
    
    scenes = []
    for stage in ["hook", "context", "insight", "future", "ending"]:
        templates = SCENE_TEMPLATES[stage]
        # Rotate through templates based on job hash
        idx = hash(topic + stage) % len(templates)
        base_prompt = templates[idx]
        
        # Add topic-specific modifier
        if theme != "general":
            base_prompt = base_prompt.replace("Indian", f"Indian {theme}")
        
        scenes.append({
            "stage": stage,
            "prompt": base_prompt,
            "duration": 5
        })
    
    return scenes

# ==========================================
# LEONARDO IMAGE GENERATION (With Retry)
# ==========================================

def generate_image_with_retry(prompt, output_path, max_retries=3):
    """Generate image with exponential backoff"""
    
    for attempt in range(max_retries):
        try:
            # Step 1: Create generation
            r = requests.post(
                "https://cloud.leonardo.ai/api/rest/v1/generations",
                headers={
                    "Authorization": f"Bearer {LEONARDO_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "prompt": prompt,
                    "modelId": "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",  # Leonardo Kino XL
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
            
            # Step 2: Poll for completion
            for poll in range(30):  # Max 60 seconds
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
                        
                        # Download image
                        img_r = requests.get(img_url, timeout=30)
                        img_r.raise_for_status()
                        
                        with open(output_path, "wb") as f:
                            f.write(img_r.content)
                        
                        return True
            
            raise Exception("Generation timeout")
            
        except Exception as e:
            print(f"Image attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
    
    return False

def generate_all_images(scenes, job_id):
    """Generate all 5 images with progress tracking"""
    
    image_paths = []
    for i, scene in enumerate(scenes):
        log_step(job_id, "IMAGES", f"Generating scene {i+1}/5: {scene['stage']}")
        
        path = f"{TMP_DIR}/{job_id}_{i}.png"
        
        try:
            generate_image_with_retry(scene["prompt"], path)
            image_paths.append(path)
        except Exception as e:
            print(f"Failed to generate image {i}: {e}")
            # Create fallback: use previous image or solid color
            if image_paths:
                # Copy previous image
                import shutil
                shutil.copy(image_paths[-1], path)
                image_paths.append(path)
            else:
                raise Exception(f"First image failed: {e}")
    
    return image_paths

# ==========================================
# VOICE GENERATION
# ==========================================

def generate_voice(script, job_id):
    """Generate voice with ElevenLabs"""
    
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
# VIDEO RENDERING (Ken Burns + Subtitles)
# ==========================================

def get_audio_duration(audio_path):
    """Get audio duration using ffprobe"""
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", audio_path
        ], capture_output=True, text=True, timeout=10)
        return float(result.stdout.strip())
    except:
        return 25  # Default

def render_video(images, audio, script, job_id):
    """Render video with Ken Burns effect and subtitles"""
    
    log_step(job_id, "RENDER", "Starting video render...")
    
    video_path = f"{TMP_DIR}/{job_id}.mp4"
    audio_duration = get_audio_duration(audio)
    
    # Calculate scene duration
    scene_duration = audio_duration / len(images)
    
    # Create complex filter for Ken Burns + concat
    filter_parts = []
    inputs = []
    
    for i, img in enumerate(images):
        inputs.extend(["-loop", "1", "-t", str(scene_duration), "-i", img])
        
        # Ken Burns: slow zoom and pan
        zoom_start = 1.0
        zoom_end = 1.15
        x_start = 0
        x_end = (i % 2) * 20  # Alternate pan direction
        
        filter_parts.append(
            f"[{i}:v]scale=1080:1920:force_original_aspect_ratio=decrease,"
            f"zoompan=z='min(zoom+0.0015,{zoom_end})':"
            f"x='iw/2-(iw/zoom/2)+{x_start}+({x_end}-{x_start})*on/{scene_duration*30}':"
            f"y='ih/2-(ih/zoom/2)':"
            f"d={int(scene_duration * 30)}:s=1080x1920[v{i}];"
        )
    
    # Concatenate all scenes
    concat_inputs = "".join([f"[v{i}]" for i in range(len(images))])
    filter_parts.append(f"{concat_inputs}concat=n={len(images)}:v=1:a=0[video];")
    
    # Build ffmpeg command
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
    
    # Verify output
    if not os.path.exists(video_path) or os.path.getsize(video_path) < 100000:
        raise Exception("Video file too small or missing")
    
    return video_path

# ==========================================
# YOUTUBE UPLOAD
# ==========================================

def get_youtube_token():
    """Refresh YouTube access token"""
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
    """Upload video to YouTube"""
    
    log_step(job_id, "UPLOAD", "Uploading to YouTube...")
    
    token = get_youtube_token()
    
    # Create SEO-optimized description
    description = f"""{script}

🚀 Exploring India's future by 2060

#India2060 #FutureTech #IndiaFuture #Shorts #AI #Technology

Follow for daily glimpses into India's future!

This content is AI-generated for educational and entertainment purposes."""

    metadata = {
        "snippet": {
            "title": title[:100],  # YouTube limit
            "description": description[:5000],
            "tags": ["India 2060", "Future India", "AI", "Technology", "Shorts", "India Future"],
            "categoryId": "28"  # Science & Technology
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False
        }
    }
    
    # Multipart upload
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
    result = r.json()
    
    return result["id"]

# ==========================================
# MAIN PIPELINE
# ==========================================

@app.route("/full-pipeline", methods=["POST"])
def pipeline():
    data = request.json
    job_id = data.get("job_id", str(uuid.uuid4()))
    topic = data.get("topic", "Future India")
    
    print(f"\n{'='*60}")
    print(f"PIPELINE START: {job_id}")
    print(f"TOPIC: {topic}")
    print(f"{'='*60}\n")
    
    try:
        # Step 1: Generate script
        update_status(job_id, "script", {"topic": topic})
        script = generate_script(topic)
        
        # Step 2: Generate visual scenes
        scenes = generate_visual_scenes(topic)
        
        # Step 3: Generate images
        images = generate_all_images(scenes, job_id)
        
        # Step 4: Generate voice
        audio = generate_voice(script, job_id)
        
        # Step 5: Render video
        video = render_video(images, scenes, audio, script, job_id)
        
        # Step 6: Upload (or skip in test mode)
        if TEST_MODE:
            print("TEST MODE: Skipping YouTube upload")
            video_id = "TEST_MODE"
        else:
            title = f"{topic} 🇮🇳 #shorts"
            video_id = upload_to_youtube(video, title, script, job_id)
        
        # Success
        update_status(job_id, "complete", {
            "youtube_id": video_id,
            "script
