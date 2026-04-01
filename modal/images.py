import modal
import os
import re
import time
import random
import requests
from pathlib import Path

# ==========================================
# MODAL APP — IMAGES
# 6-tier fallback chain for a SINGLE image.
# The orchestrator (pipeline.py) calls generate_one_image.spawn()
# three times simultaneously → 3x speedup vs sequential.
#
# Tier order (free → paid):
#   1. Pollinations.ai   — free, no key
#   2. HuggingFace       — free key
#   3. Pixabay           — free key, stock photos
#   4. Together AI       — cheap ~$0.01/img
#   5. Replicate         — cheap ~$0.003/img
#   6. Leonardo          — paid API credits, best quality
#   F. R2 Library        — fallback from saved images
# ==========================================

app = modal.App("india20sixty-images")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("requests")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR    = "/tmp/images"
IMG_WIDTH  = 864
IMG_HEIGHT = 1536

LEONARDO_MODELS = [
    "aa77f04e-3eec-4034-9c07-d0f619684628",
    "1e60896f-3c26-4296-8ecc-53e2afecc132",
    "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
]


# ==========================================
# GENERATE ONE IMAGE
# Called 3x in parallel by the orchestrator.
# Returns: local path to downloaded image file.
# ==========================================

@app.function(
    image=image,
    secrets=secrets,
    cpu=0.5,
    memory=512,
    timeout=120,
)
def generate_one_image(
    prompt:      str,
    scene_idx:   int,
    job_id:      str,
    topic:       str,
    exclude_r2:  list = None,
) -> str:
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

    LEONARDO_API_KEY  = os.environ.get("LEONARDO_API_KEY", "")
    HF_API_KEY        = os.environ.get("HF_API_KEY", "")
    TOGETHER_API_KEY  = os.environ.get("TOGETHER_API_KEY", "")
    REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY", "")
    PIXABAY_API_KEY   = os.environ.get("PIXABAY_API_KEY", "")
    SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
    SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
    R2_BASE_URL       = os.environ.get("R2_BASE_URL", "")

    output_path = f"{TMP_DIR}/{job_id}_{scene_idx}.png"
    print(f"\n[Image {scene_idx+1}] {prompt[:70]}...")

    # ── KEYWORD EXTRACTOR ────────────────────────────────────────
    def extract_keywords(p, n=4):
        skip = {
            'cinematic','ultra','realistic','dramatic','hyperrealistic',
            'photorealistic','epic','wide','shot','arri','alexa','grain',
            'film','8k','4k','high','contrast','indian','india','futuristic',
            'vibrant','moody','golden','hour','aerial','stunning','detailed',
            'sharp','focus','bokeh','lighting','dark','bright','color',
            'create','image','prompt','scene','showing','depicting',
        }
        words = re.sub(r'[^a-zA-Z\s]', '', p).lower().split()
        return [w for w in words if len(w) > 3 and w not in skip][:n]

    # ── TIER 1: POLLINATIONS ─────────────────────────────────────
    def try_pollinations():
        try:
            import urllib.parse
            safe = urllib.parse.quote(prompt[:400])
            url  = (f"https://image.pollinations.ai/prompt/{safe}"
                    f"?width={IMG_WIDTH}&height={IMG_HEIGHT}"
                    f"&model=flux&nologo=true&seed={random.randint(1,99999)}")
            r = requests.get(url, timeout=60, stream=True)
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            if size < 50_000:
                return False
            print(f"  [1/Pollinations] OK {size//1024}KB")
            return True
        except Exception as e:
            print(f"  [1/Pollinations] failed: {e}")
            return False

    # ── TIER 2: HUGGING FACE ─────────────────────────────────────
    def try_huggingface():
        if not HF_API_KEY:
            return False
        models = [
            "black-forest-labs/FLUX.1-schnell",
            "stabilityai/stable-diffusion-xl-base-1.0",
        ]
        for model in models:
            try:
                r = requests.post(
                    f"https://api-inference.huggingface.co/models/{model}",
                    headers={"Authorization": f"Bearer {HF_API_KEY}"},
                    json={"inputs": prompt[:500],
                          "parameters": {"width": IMG_WIDTH, "height": IMG_HEIGHT}},
                    timeout=60,
                )
                if r.status_code in (503, 429):
                    continue
                if r.status_code != 200:
                    continue
                with open(output_path, "wb") as f:
                    f.write(r.content)
                size = os.path.getsize(output_path)
                if size < 50_000:
                    continue
                print(f"  [2/HuggingFace:{model.split('/')[-1]}] OK {size//1024}KB")
                return True
            except Exception as e:
                print(f"  [2/HuggingFace] {e}")
                continue
        return False

    # ── TIER 3: PIXABAY ──────────────────────────────────────────
    def try_pixabay():
        if not PIXABAY_API_KEY:
            return False
        try:
            keywords = extract_keywords(prompt)
            query    = ' '.join(keywords) if keywords else 'technology india'
            r = requests.get(
                "https://pixabay.com/api/",
                params={
                    "key": PIXABAY_API_KEY, "q": query,
                    "image_type": "photo", "orientation": "vertical",
                    "min_width": 800, "min_height": 1200,
                    "safesearch": "true", "per_page": 5,
                },
                timeout=15,
            )
            r.raise_for_status()
            hits = r.json().get("hits", [])
            if not hits:
                return False
            img_url = hits[0].get("largeImageURL") or hits[0].get("webformatURL")
            if not img_url:
                return False
            img_r = requests.get(img_url, timeout=30, stream=True)
            img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            if size < 50_000:
                return False
            print(f"  [3/Pixabay:'{query}'] OK {size//1024}KB")
            return True
        except Exception as e:
            print(f"  [3/Pixabay] {e}")
            return False

    # ── TIER 4: TOGETHER AI ──────────────────────────────────────
    def try_together():
        if not TOGETHER_API_KEY:
            return False
        try:
            r = requests.post(
                "https://api.together.xyz/v1/images/generations",
                headers={"Authorization": f"Bearer {TOGETHER_API_KEY}",
                         "Content-Type": "application/json"},
                json={"model": "black-forest-labs/FLUX.1-schnell-Free",
                      "prompt": prompt[:500],
                      "width": IMG_WIDTH, "height": IMG_HEIGHT,
                      "steps": 4, "n": 1},
                timeout=60,
            )
            if r.status_code != 200:
                return False
            data    = r.json()
            img_url = data.get("data", [{}])[0].get("url")
            if not img_url:
                b64 = data.get("data", [{}])[0].get("b64_json")
                if b64:
                    import base64
                    with open(output_path, "wb") as f:
                        f.write(base64.b64decode(b64))
                    size = os.path.getsize(output_path)
                    if size > 50_000:
                        print(f"  [4/Together] OK b64 {size//1024}KB")
                        return True
                return False
            img_r = requests.get(img_url, timeout=30, stream=True)
            img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(output_path)
            if size < 50_000:
                return False
            print(f"  [4/Together] OK {size//1024}KB")
            return True
        except Exception as e:
            print(f"  [4/Together] {e}")
            return False

    # ── TIER 5: REPLICATE ────────────────────────────────────────
    def try_replicate():
        if not REPLICATE_API_KEY:
            return False
        try:
            r = requests.post(
                "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
                headers={"Authorization": f"Token {REPLICATE_API_KEY}",
                         "Content-Type": "application/json"},
                json={"input": {"prompt": prompt[:500], "aspect_ratio": "9:16",
                                "output_format": "png", "num_outputs": 1}},
                timeout=30,
            )
            if r.status_code not in (200, 201):
                return False
            pred_id = r.json().get("id")
            if not pred_id:
                return False
            for _ in range(30):
                time.sleep(2)
                poll   = requests.get(
                    f"https://api.replicate.com/v1/predictions/{pred_id}",
                    headers={"Authorization": f"Token {REPLICATE_API_KEY}"},
                    timeout=10,
                )
                data   = poll.json()
                status = data.get("status")
                if status == "succeeded":
                    urls = data.get("output", [])
                    if not urls:
                        return False
                    img_r = requests.get(urls[0], timeout=30, stream=True)
                    img_r.raise_for_status()
                    with open(output_path, "wb") as f:
                        for chunk in img_r.iter_content(8192):
                            f.write(chunk)
                    size = os.path.getsize(output_path)
                    if size < 50_000:
                        return False
                    print(f"  [5/Replicate] OK {size//1024}KB")
                    return True
                elif status in ("failed", "canceled"):
                    return False
            return False
        except Exception as e:
            print(f"  [5/Replicate] {e}")
            return False

    # ── TIER 6: LEONARDO ─────────────────────────────────────────
    def try_leonardo():
        if not LEONARDO_API_KEY:
            return False
        for model_id in LEONARDO_MODELS:
            try:
                r = requests.post(
                    "https://cloud.leonardo.ai/api/rest/v1/generations",
                    headers={"Authorization": f"Bearer {LEONARDO_API_KEY}",
                             "Content-Type": "application/json"},
                    json={"prompt": prompt, "modelId": model_id,
                          "width": IMG_WIDTH, "height": IMG_HEIGHT,
                          "num_images": 1, "presetStyle": "CINEMATIC"},
                    timeout=30,
                )
                if r.status_code == 402:
                    print("  [6/Leonardo] API credits exhausted")
                    return False
                if r.status_code != 200:
                    print(f"  [6/Leonardo] {r.status_code}")
                    time.sleep(5)
                    continue
                data = r.json()
                if "sdGenerationJob" not in data:
                    continue
                gen_id = data["sdGenerationJob"]["generationId"]
                print(f"  [6/Leonardo] gen_id={gen_id}")
                # Poll for completion
                for poll in range(80):
                    time.sleep(3)
                    pr = requests.get(
                        f"https://cloud.leonardo.ai/api/rest/v1/generations/{gen_id}",
                        headers={"Authorization": f"Bearer {LEONARDO_API_KEY}"},
                        timeout=15,
                    )
                    pr.raise_for_status()
                    gen    = pr.json().get("generations_by_pk", {})
                    status = gen.get("status", "UNKNOWN")
                    if status == "FAILED":
                        raise Exception("Leonardo generation FAILED")
                    if status == "COMPLETE":
                        imgs = gen.get("generated_images", [])
                        if not imgs:
                            raise Exception("No images in response")
                        img_r = requests.get(imgs[0]["url"], timeout=30)
                        img_r.raise_for_status()
                        with open(output_path, "wb") as f:
                            f.write(img_r.content)
                        size = os.path.getsize(output_path)
                        print(f"  [6/Leonardo] OK {size//1024}KB")
                        return True
                raise Exception("Leonardo polling timeout")
            except Exception as e:
                if "credits_exhausted" in str(e) or "402" in str(e):
                    return False
                print(f"  [6/Leonardo] model failed: {str(e)[:60]}")
                time.sleep(5)
        return False

    # ── FALLBACK: R2 LIBRARY ─────────────────────────────────────
    def try_r2_library():
        if not SUPABASE_URL:
            return False
        try:
            words = extract_keywords(prompt)
            rows  = []
            for term in words[:2]:
                r = requests.get(
                    f"{SUPABASE_URL}/rest/v1/image_cache"
                    f"?topic=ilike.*{term}*&select=r2_key,public_url,topic"
                    f"&limit=5&order=created_at.desc",
                    headers={"apikey": SUPABASE_ANON_KEY,
                             "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                    timeout=5,
                )
                if r.status_code == 200:
                    rows.extend(r.json())
            if not rows:
                r = requests.get(
                    f"{SUPABASE_URL}/rest/v1/image_cache"
                    f"?select=r2_key,public_url,topic&limit=10&order=created_at.desc",
                    headers={"apikey": SUPABASE_ANON_KEY,
                             "Authorization": f"Bearer {SUPABASE_ANON_KEY}"},
                    timeout=5,
                )
                if r.status_code == 200:
                    rows = r.json()
            seen, unique = set(), []
            for row in rows:
                k = row.get("r2_key", "")
                if k and k not in seen and k not in (exclude_r2 or []):
                    seen.add(k)
                    unique.append(row)
            if not unique:
                return False
            chosen  = unique[0]
            img_url = chosen.get("public_url") or f"{R2_BASE_URL.rstrip('/')}/{chosen['r2_key']}"
            print(f"  [F/R2Library] '{chosen.get('topic','?')[:40]}'")
            img_r = requests.get(img_url, timeout=30, stream=True)
            img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192):
                    f.write(chunk)
            return os.path.getsize(output_path) > 10_000
        except Exception as e:
            print(f"  [F/R2Library] {e}")
            return False

    # ── FALLBACK: BLACK FRAME ─────────────────────────────────────
    def make_black_frame():
        import subprocess as sp
        sp.run([
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", f"color=c=0x0d1117:s={IMG_WIDTH}x{IMG_HEIGHT}:d=1",
            "-frames:v", "1", output_path,
        ], capture_output=True, timeout=15)
        print("  [F/BlackFrame] using placeholder")

    # ── RUN CHAIN ────────────────────────────────────────────────
    TIERS = [
        try_pollinations,
        try_huggingface,
        try_pixabay,
        try_together,
        try_replicate,
        try_leonardo,
    ]
    for fn in TIERS:
        try:
            if fn():
                break
        except Exception as e:
            print(f"  tier exception: {e}")
    else:
        # All tiers failed — try R2 library
        if not try_r2_library():
            make_black_frame()

    return output_path


# ==========================================
# SAVE IMAGE TO R2
# Called after successful generation to build the image library.
# Non-fatal — pipeline continues even if this fails.
# ==========================================

@app.function(image=image, secrets=secrets, timeout=60)
def save_image_to_r2(local_path: str, topic: str, job_id: str, scene_idx: int) -> tuple:
    """Upload generated image to R2 and record in image_cache. Returns (key, url)."""
    from workers.publisher import upload_to_r2
    import re as _re

    SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
    SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

    try:
        topic_slug = _re.sub(r'[^a-z0-9]+', '-', topic.lower())[:40]
        key        = f"images/{topic_slug}/{job_id}_{scene_idx}.png"
        public_url = upload_to_r2.remote(local_path, key)

        requests.post(
            f"{SUPABASE_URL}/rest/v1/image_cache",
            headers={"apikey": SUPABASE_ANON_KEY,
                     "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal"},
            json={"job_id": job_id, "topic": topic,
                  "r2_key": key, "public_url": public_url,
                  "scene_idx": scene_idx,
                  "created_at": datetime.utcnow().isoformat()},
            timeout=5,
        )
        print(f"  Saved to R2 library: {key}")
        return key, public_url
    except Exception as e:
        print(f"  R2 library save failed (non-fatal): {e}")
        return None, None
