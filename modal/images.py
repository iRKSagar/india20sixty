import modal
import io
import os
import re
import time
import random
import requests
from pathlib import Path
from datetime import datetime

# ==========================================
# MODAL APP — IMAGES
# india20sixty channel
#
# Tier 0:  FLUX.1-schnell on A10G GPU     free, ~16s, Apache 2.0
# Tier 1:  Pollinations.ai               free, no key
# Tier 2:  HuggingFace Inference API     free key
# Tier 3:  Pixabay                       free key, stock fallback
# Tier 4:  Together AI                   ~$0.01/image
# Tier 5:  Replicate                     ~$0.003/image
# Tier 6:  Leonardo AI                   paid, highest quality
# Final:   R2 Library                    saved from past runs
#
# Every successful image → auto-saved to R2 + image_cache table
# Organized by cluster for the Library tab.
# ==========================================

app = modal.App("india20sixty-images")

# ── CHANNEL VISUAL IDENTITY ───────────────────────────────────
# This is what makes india20sixty images look distinct.
# Crafted specifically for Indian tech/innovation documentary content.
# Edit STYLE_PREFIX to change the visual character of this channel.

CHANNEL_NAME = "india20sixty"

STYLE_PREFIX = (
    "photorealistic modern India 2025, "
    "contemporary Indian professionals in real urban settings, "
    "Indian faces natural expressions diverse ages, "
    "Indian cities tech parks offices research labs metro stations, "
    "natural daylight or soft artificial lighting, "
    "sharp focus high detail clean composition, "
    "authentic grounded realistic not stylized, "
    "no text no logos no watermarks, "
)

NEGATIVE_PROMPT = (
    "golden hour, orange tint, saffron palette, warm orange grade, ochre, "
    "ARRI cinematic, anamorphic flare, "
    "blurry, cartoon, anime, painting, watermark, text overlay, logo, "
    "traditional religious imagery unless relevant, "
    "western faces, european, low quality, overexposed, "
    "nsfw, ugly, distorted anatomy, extra limbs, deformed hands, "
    "stock photo look, generic clipart, "
    "jpeg artifacts, chromatic aberration, "
    "marigold hues, lotus motif unless relevant, "
)

import re as _re

_BAD_PROMPT_WORDS = [
    "golden hour", "golden light", "warm golden", "saffron", "ochre",
    "ARRI", "anamorphic", "8K", "8k",
    "marigold", "warm palette", "warm tones", "warm hues",
    "HDR", "high dynamic range", "dramatic volumetric",
]

def _sanitize_prompt(prompt):
    clean = prompt
    for bad in _BAD_PROMPT_WORDS:
        clean = clean.replace(bad, "").replace(bad.lower(), "").replace(bad.upper(), "")
    clean = _re.sub(r"  +", " ", clean).strip().strip(",").strip()
    return clean


def _check_image_quality(image_path: str, prompt: str, cluster: str, openai_key: str) -> tuple:
    """
    GPT-4o Vision quality check on generated image.
    Returns (ok: bool, reason: str)
    Checks:
    - Subject matches topic (no rocket-as-tilak, no random symbols)
    - Image is photorealistic India setting
    - No text/watermarks visible
    - No absurd combinations
    """
    if not openai_key:
        return True, "no key — skipped"

    import base64, json
    import requests as req

    try:
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()

        check_prompt = f"""You are a quality controller for an Indian YouTube channel.
Check this AI-generated image against the prompt: "{prompt[:200]}"
Topic cluster: {cluster}

Answer with ONLY valid JSON — no markdown:
{{
  "ok": true or false,
  "reason": "one sentence explaining why it passed or failed",
  "issues": ["list any specific problems found"]
}}

Fail (ok=false) if ANY of these are true:
- Subject is absurd or wrong (e.g. rocket shown as a facial mark or tilak, satellite shown as jewellery, logo as a facial feature)
- Image has obvious text, watermarks, or logos burned in
- Image looks like clip art, cartoon, or illustration (not photorealistic)
- The image has nothing to do with India or the topic
- Multiple faces melted together or deformed anatomy

Pass (ok=true) if:
- Image is a plausible, grounded depiction of the topic
- Indian context is visible (faces, architecture, technology)
- Photorealistic style"""

        r = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "low"}},
                    {"type": "text", "text": check_prompt}
                ]}]
            },
            timeout=30,
        )
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown if GPT adds it
        raw = _re.sub(r"```json|```", "", raw).strip()
        data = json.loads(raw)
        ok = bool(data.get("ok", True))
        reason = data.get("reason", "")
        issues = data.get("issues", [])
        if issues:
            reason += " | Issues: " + ", ".join(issues)
        print(f"  Quality check: {'PASS' if ok else 'FAIL'} — {reason}")
        return ok, reason
    except Exception as e:
        print(f"  Quality check error (skipping): {e}")
        return True, f"check failed: {e}"


def _make_safe_prompt(original_prompt: str, cluster: str) -> str:
    """
    Create a more literal, explicit prompt to avoid absurd combinations.
    Strips abstract metaphors, focuses on concrete photorealistic scene.
    """
    # Cluster-specific safe anchors
    safe_anchors = {
        "Space":    "Indian rocket engineer at ISRO control centre, large screens showing satellite data, modern facility",
        "AI":       "Indian software engineer at computer screen showing code, modern tech office, Bengaluru",
        "Gadgets":  "Indian consumer holding new smartphone, modern retail store, contemporary Indian setting",
        "DeepTech": "Indian scientist in laboratory with equipment, clean modern research facility",
        "GreenTech":"Indian solar farm with technicians, rows of solar panels, rural India, natural daylight",
        "Startups": "Indian entrepreneur in meeting room, startup office, whiteboard, young professionals",
    }
    anchor = safe_anchors.get(cluster, "photorealistic modern India, Indian professionals at work")
    # Keep first 60 chars of original for context, add safe anchor
    snippet = original_prompt[:60].split(",")[0].strip()
    return f"{anchor}, context: {snippet}, natural daylight, sharp focus, no text"

IMG_WIDTH        = 864
IMG_HEIGHT       = 1536
INFERENCE_STEPS  = 8      # 8 steps: significantly better than 4, ~16s on A10G
GUIDANCE_SCALE   = 0.0    # schnell requires 0 guidance — do not change
# ─────────────────────────────────────────────────────────────

TMP_DIR = "/tmp/india20sixty-images"

# ── FLUX MODEL — baked into container at build time ───────────

def _download_flux():
    import os
    from diffusers import FluxPipeline
    import torch
    hf_token = os.environ.get("HF_TOKEN", "")
    print("Downloading FLUX.1-schnell weights...")
    FluxPipeline.from_pretrained(
        "black-forest-labs/FLUX.1-schnell",
        torch_dtype=torch.bfloat16,
        token=hf_token or None,
    )
    print("FLUX.1-schnell cached in container.")

flux_image = (
    modal.Image.from_registry("pytorch/pytorch:2.6.0-cuda12.4-cudnn9-runtime")
    .pip_install(
        "diffusers>=0.32.0",
        "transformers>=4.48.0",
        "accelerate>=1.3.0",
        "safetensors>=0.4.5",
        "sentencepiece>=0.2.0",
        "Pillow",
        "requests",
    )
    .run_function(
        _download_flux,
        secrets=[modal.Secret.from_name("india20sixty-secrets")],
    )
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]


# ==========================================
# MAIN — called by pipeline.py
# ==========================================

@app.function(
    image=flux_image,
    gpu="A10G",
    timeout=180,
    secrets=secrets,
)
@modal.concurrent(max_inputs=1)   # one image per container — prevents CUDA OOM
def generate_single_image(
    prompt: str,
    scene_idx: int,
    job_id: str,
    cluster: str = "AI",
    job_type: str = "shorts",    # "shorts" | "longform"
    engine_mode: str = "inbuilt", # "inbuilt" | "external"
) -> dict:
    """
    Generate one image. Auto-saves to R2 + image_cache after success.
    Returns: { success, local_path, tier_used, scene_idx, r2_url }
    """
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    output_path = f"{TMP_DIR}/{job_id}_{scene_idx}.png"

    HF_API_KEY        = os.environ.get("HF_API_KEY", "")
    TOGETHER_API_KEY  = os.environ.get("TOGETHER_API_KEY", "")
    REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY", "")
    LEONARDO_API_KEY  = os.environ.get("LEONARDO_API_KEY", "")
    PIXABAY_API_KEY   = os.environ.get("PIXABAY_API_KEY", "")
    SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
    SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
    R2_BASE_URL       = os.environ.get("R2_BASE_URL", "").rstrip("/")
    R2_ACCOUNT_ID     = os.environ.get("R2_ACCOUNT_ID", "")
    R2_ACCESS_KEY_ID  = os.environ.get("R2_ACCESS_KEY_ID", "")
    R2_SECRET_KEY     = os.environ.get("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET         = os.environ.get("R2_BUCKET", "india20sixty")

    print(f"\n[Image {scene_idx+1}] job={job_id} cluster={cluster} mode={engine_mode}")
    print(f"  Prompt: {prompt[:80]}...")

    tier_used = None

    if engine_mode == "inbuilt":
        # ── TIER 0: FLUX on-device ────────────────────────────
        print("  [Tier 0: FLUX.1-schnell × 8 steps on A10G]")
        try:
            if _try_flux(prompt, output_path):
                tier_used = "FLUX-A10G"
        except Exception as e:
            print(f"  FLUX exception: {e}")

    if not tier_used:
        # ── TIERS 1–6: External fallbacks ─────────────────────
        tiers = [
            ("Pollinations", lambda p,o: _try_pollinations(p, o)),
            ("HuggingFace",  lambda p,o: _try_huggingface(p, o, HF_API_KEY)),
            ("Pixabay",      lambda p,o: _try_pixabay(p, o, PIXABAY_API_KEY)),
            ("Together",     lambda p,o: _try_together(p, o, TOGETHER_API_KEY)),
            ("Replicate",    lambda p,o: _try_replicate(p, o, REPLICATE_API_KEY)),
            ("Leonardo",     lambda p,o: _try_leonardo(p, o, LEONARDO_API_KEY)),
        ]
        for name, fn in tiers:
            print(f"  [Tier: {name}]")
            try:
                if fn(prompt, output_path):
                    tier_used = name
                    break
            except Exception as e:
                print(f"  {name} exception: {e}")

    if not tier_used:
        # ── FINAL: R2 Library ──────────────────────────────────
        print("  [Tier: R2 Library]")
        if _try_r2_library(prompt, output_path, SUPABASE_URL, SUPABASE_ANON_KEY, R2_BASE_URL):
            tier_used = "R2Library"

    if not tier_used:
        print(f"  ✗ All tiers failed for scene {scene_idx}")
        return {"success": False, "local_path": None, "tier_used": None,
                "scene_idx": scene_idx, "r2_url": None, "image_bytes": None}

    size = os.path.getsize(output_path)
    print(f"  ✓ {tier_used} — {size//1024}KB")

    # ── IMAGE QUALITY BRAIN — GPT Vision check ────────────────
    # Checks: right subject, no gibberish, no absurd combinations
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
    quality_ok, quality_reason = _check_image_quality(
        output_path, prompt, cluster, OPENAI_API_KEY
    )
    if not quality_ok:
        print(f"  ✗ Quality check FAILED: {quality_reason}")
        print(f"  Regenerating with refined prompt...")
        # Try once more with a more explicit, literal prompt
        refined = _make_safe_prompt(prompt, cluster)
        print(f"  Refined: {refined[:80]}...")
        tier_used = None
        if engine_mode == "inbuilt":
            try:
                if _try_flux(refined, output_path):
                    tier_used = "FLUX-retry"
            except Exception as e:
                print(f"  FLUX retry exception: {e}")
        if not tier_used:
            try:
                if _try_pollinations(refined, output_path):
                    tier_used = "Pollinations-retry"
            except Exception as e:
                print(f"  Pollinations retry: {e}")
        if tier_used:
            size = os.path.getsize(output_path)
            print(f"  ✓ Regenerated {tier_used} — {size//1024}KB")
        else:
            print(f"  Keeping original despite quality issue")
            tier_used = tier_used or "original"

    # Read bytes to return — renderer runs in different container, can't share /tmp/
    with open(output_path, "rb") as f:
        image_bytes = f.read()

    # ── AUTO-SAVE TO R2 + image_cache ─────────────────────────
    r2_url = None
    has_r2 = bool(R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_KEY)
    print(f"  R2 creds: account={'✓' if R2_ACCOUNT_ID else '✗MISSING'} key={'✓' if R2_ACCESS_KEY_ID else '✗MISSING'} secret={'✓' if R2_SECRET_KEY else '✗MISSING'} supabase={'✓' if SUPABASE_URL else '✗MISSING'}")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    r2_key    = f"images/{cluster}/{job_id}_{scene_idx}_{timestamp}.png"

    # Step 1: Try R2 upload
    if has_r2 and tier_used != "R2Library":
        r2_url = _upload_to_r2(
            output_path, r2_key,
            R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_KEY,
            R2_BUCKET, R2_BASE_URL
        )

    # Step 2: Always insert to image_cache (with or without R2 URL)
    if SUPABASE_URL and tier_used != "R2Library":
        public_url = r2_url or ""
        # Extract topic from job_id (format: job_id or job_id_segN)
        topic_hint = job_id.replace("-","").replace("_","")[:20]
        _insert_image_cache(
            SUPABASE_URL, SUPABASE_ANON_KEY,
            job_id, r2_key, public_url, cluster,
            prompt, tier_used, job_type, scene_idx
        )
        if not r2_url:
            print(f"  ⚠ image_cache saved without R2 URL — R2 creds missing from Modal secrets")

    return {"success": True, "local_path": output_path, "tier_used": tier_used,
            "scene_idx": scene_idx, "r2_url": r2_url, "image_bytes": image_bytes}


# ==========================================
# TIER 0 — FLUX ON-DEVICE
# ==========================================

def _try_flux(prompt: str, output_path: str) -> bool:
    import torch
    from diffusers import FluxPipeline

    full_prompt = f"{STYLE_PREFIX} {_sanitize_prompt(prompt)}"
    print(f"  Full prompt ({len(full_prompt)} chars): {full_prompt[:120]}...")

    torch.cuda.empty_cache()

    pipe = FluxPipeline.from_pretrained(
        "black-forest-labs/FLUX.1-schnell",
        torch_dtype=torch.bfloat16,
    )

    # Sequential CPU offload: keeps model on CPU, streams one layer at a time to GPU
    # Peak VRAM ~2-4GB instead of 22GB — reliable on any GPU
    pipe.enable_sequential_cpu_offload()

    try:
        result = pipe(
            prompt=full_prompt,
            width=IMG_WIDTH,
            height=IMG_HEIGHT,
            num_inference_steps=4,
            guidance_scale=GUIDANCE_SCALE,
            generator=torch.Generator("cpu").manual_seed(random.randint(0, 2**32 - 1)),
        )
        img = result.images[0]
        img.save(output_path, format="PNG", optimize=True)
        success = os.path.getsize(output_path) > 100_000
    finally:
        del pipe
        import gc
        gc.collect()
        torch.cuda.empty_cache()

    return success


# ==========================================
# AUTO-SAVE TO R2 + IMAGE_CACHE
# ==========================================

def _upload_to_r2(local_path, r2_key, r2_account_id, r2_access_key,
                  r2_secret, r2_bucket, r2_base) -> str | None:
    """Upload image to R2. Returns public URL or None on failure."""
    import hmac, hashlib, urllib.parse
    endpoint = f"https://{r2_account_id}.r2.cloudflarestorage.com"
    url      = f"{endpoint}/{r2_bucket}/{r2_key}"
    now      = datetime.utcnow()
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%Y%m%dT%H%M%SZ")
    try:
        with open(local_path, "rb") as f:
            data = f.read()
        payload_hash   = hashlib.sha256(data).hexdigest()
        signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
        canonical = "\n".join([
            "PUT",
            f"/{r2_bucket}/{urllib.parse.quote(r2_key, safe='/')}",
            "",
            f"content-type:image/png",
            f"host:{r2_account_id}.r2.cloudflarestorage.com",
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
        sk = sign(sign(sign(sign(f"AWS4{r2_secret}".encode(), date_str), "auto"), "s3"), "aws4_request")
        sig = hmac.new(sk, string_to_sign.encode(), hashlib.sha256).hexdigest()
        r = requests.put(url, data=data, headers={
            "Content-Type":         "image/png",
            "x-amz-content-sha256": payload_hash,
            "x-amz-date":           time_str,
            "Host":                 f"{r2_account_id}.r2.cloudflarestorage.com",
            "Authorization":        f"AWS4-HMAC-SHA256 Credential={r2_access_key}/{cred_scope},SignedHeaders={signed_headers},Signature={sig}",
        }, timeout=60)
        print(f"  R2 PUT {r.status_code}: {r2_key}")
        if not r.ok:
            print(f"  R2 error: {r.text[:200]}")
            return None
        public_url = f"{r2_base.rstrip('/')}/{r2_key}" if r2_base else url
        print(f"  R2 ✓ {len(data)//1024}KB → {r2_key}")
        return public_url
    except Exception as e:
        print(f"  R2 upload failed: {e}")
        return None


def _insert_image_cache(supabase_url, supabase_key, job_id, r2_key,
                        public_url, cluster, prompt, engine, job_type, scene_idx):
    """Insert image record into Supabase image_cache. Always called — even without R2."""
    try:
        r = requests.post(
            f"{supabase_url}/rest/v1/image_cache",
            headers={
                "apikey":        supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type":  "application/json",
                "Prefer":        "return=minimal",
            },
            json={
                "job_id":    job_id,
                "r2_key":    r2_key,
                "public_url":public_url,
                "cluster":   cluster,
                "prompt":    prompt[:500],
                "engine":    engine,
                "job_type":  job_type,
                "scene_idx": scene_idx,
                "created_at":datetime.utcnow().isoformat(),
            },
            timeout=10,
        )
        if r.ok:
            print(f"  image_cache ✓ scene={scene_idx} cluster={cluster}")
        else:
            print(f"  image_cache insert failed {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"  image_cache insert error: {e}")


# ==========================================
# TIER 1–6 EXTERNAL FALLBACKS
# ==========================================

def _extract_keywords(prompt: str, n: int = 4) -> list:
    skip = {
        "cinematic","ultra","realistic","dramatic","hyperrealistic","photorealistic",
        "epic","wide","shot","arri","alexa","grain","film","8k","4k","high","contrast",
        "indian","india","futuristic","vibrant","moody","golden","hour","aerial",
        "stunning","beautiful","detailed","sharp","focus","bokeh","lighting",
        "dark","bright","colorful","create","image","prompt","scene","showing",
        "featuring","depicting","must","look","documentary","quality","saffron",
        "camera","lens","hdr","hyperrealistic","anamorphic","volumetric",
    }
    words = re.sub(r"[^a-zA-Z\s]", "", prompt).lower().split()
    return [w for w in words if len(w) > 3 and w not in skip][:n]


def _try_pollinations(prompt: str, output_path: str) -> bool:
    import urllib.parse
    # Pollinations has URL length limit — keep prompt short
    short_prompt = f"{STYLE_PREFIX} {prompt}"[:300]
    safe = urllib.parse.quote(short_prompt)
    url = (f"https://image.pollinations.ai/prompt/{safe}"
           f"?width={IMG_WIDTH}&height={IMG_HEIGHT}"
           f"&model=flux&nologo=true&seed={random.randint(1,99999)}")
    r = requests.get(url, timeout=90, stream=True)
    r.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in r.iter_content(8192): f.write(chunk)
    return os.path.getsize(output_path) > 50_000


def _try_huggingface(prompt: str, output_path: str, api_key: str) -> bool:
    if not api_key: return False
    full_prompt = f"{STYLE_PREFIX} {prompt}"
    for model in ["black-forest-labs/FLUX.1-schnell",
                  "stabilityai/stable-diffusion-xl-base-1.0"]:
        try:
            r = requests.post(
                f"https://api-inference.huggingface.co/models/{model}",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"inputs": full_prompt[:500],
                      "parameters": {"width": IMG_WIDTH, "height": IMG_HEIGHT}},
                timeout=90,
            )
            if r.status_code in (503, 429): continue
            if r.status_code != 200: continue
            with open(output_path, "wb") as f: f.write(r.content)
            if os.path.getsize(output_path) > 50_000: return True
        except Exception: continue
    return False


def _try_pixabay(prompt: str, output_path: str, api_key: str) -> bool:
    if not api_key: return False
    keywords = _extract_keywords(prompt)
    query = " ".join(keywords) if keywords else "india technology innovation"
    r = requests.get("https://pixabay.com/api/", params={
        "key": api_key, "q": query, "image_type": "photo",
        "orientation": "vertical", "min_width": 800, "min_height": 1200,
        "safesearch": "true", "per_page": 5}, timeout=15)
    r.raise_for_status()
    hits = r.json().get("hits", [])
    if not hits: return False
    img_url = hits[0].get("largeImageURL") or hits[0].get("webformatURL")
    if not img_url: return False
    img_r = requests.get(img_url, timeout=30, stream=True); img_r.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in img_r.iter_content(8192): f.write(chunk)
    return os.path.getsize(output_path) > 50_000


def _try_together(prompt: str, output_path: str, api_key: str) -> bool:
    if not api_key: return False
    full_prompt = f"{STYLE_PREFIX} {prompt}"
    r = requests.post(
        "https://api.together.xyz/v1/images/generations",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": "black-forest-labs/FLUX.1-schnell-Free",
              "prompt": full_prompt[:500], "width": IMG_WIDTH,
              "height": IMG_HEIGHT, "steps": 4, "n": 1},
        timeout=90,
    )
    if r.status_code != 200: return False
    data = r.json()
    img_url = data.get("data", [{}])[0].get("url")
    if not img_url:
        b64 = data.get("data", [{}])[0].get("b64_json")
        if b64:
            import base64
            with open(output_path, "wb") as f: f.write(base64.b64decode(b64))
            return os.path.getsize(output_path) > 50_000
        return False
    img_r = requests.get(img_url, timeout=30, stream=True); img_r.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in img_r.iter_content(8192): f.write(chunk)
    return os.path.getsize(output_path) > 50_000


def _try_replicate(prompt: str, output_path: str, api_key: str) -> bool:
    if not api_key: return False
    full_prompt = f"{STYLE_PREFIX} {prompt}"
    r = requests.post(
        "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
        headers={"Authorization": f"Token {api_key}", "Content-Type": "application/json"},
        json={"input": {"prompt": full_prompt[:500], "aspect_ratio": "9:16",
                        "output_format": "png", "num_outputs": 1}}, timeout=30)
    if r.status_code not in (200, 201): return False
    pred_id = r.json().get("id")
    if not pred_id: return False
    for _ in range(30):
        time.sleep(2)
        poll = requests.get(f"https://api.replicate.com/v1/predictions/{pred_id}",
                            headers={"Authorization": f"Token {api_key}"}, timeout=10)
        data = poll.json(); status = data.get("status")
        if status == "succeeded":
            urls = data.get("output", [])
            if not urls: return False
            img_r = requests.get(urls[0], timeout=30, stream=True); img_r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in img_r.iter_content(8192): f.write(chunk)
            return os.path.getsize(output_path) > 50_000
        elif status in ("failed", "canceled"): return False
    return False


LEONARDO_MODELS = [
    "aa77f04e-3eec-4034-9c07-d0f619684628",
    "1e60896f-3c26-4296-8ecc-53e2afecc132",
    "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",
]

def _try_leonardo(prompt: str, output_path: str, api_key: str) -> bool:
    if not api_key: return False
    full_prompt = f"{STYLE_PREFIX} {prompt}"
    for model_id in LEONARDO_MODELS:
        try:
            r = requests.post(
                "https://cloud.leonardo.ai/api/rest/v1/generations",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"prompt": full_prompt, "modelId": model_id,
                      "width": IMG_WIDTH, "height": IMG_HEIGHT,
                      "num_images": 1, "presetStyle": "CINEMATIC"}, timeout=30)
            if r.status_code == 402: return False
            if r.status_code != 200: time.sleep(5); continue
            data = r.json()
            if "sdGenerationJob" not in data: continue
            gen_id = data["sdGenerationJob"]["generationId"]
            for _ in range(80):
                time.sleep(3)
                pr = requests.get(
                    f"https://cloud.leonardo.ai/api/rest/v1/generations/{gen_id}",
                    headers={"Authorization": f"Bearer {api_key}"}, timeout=15)
                pr.raise_for_status()
                gen = pr.json().get("generations_by_pk", {})
                if gen.get("status") == "FAILED": raise Exception("Leonardo FAILED")
                if gen.get("status") == "COMPLETE":
                    imgs = gen.get("generated_images", [])
                    if not imgs: raise Exception("No images")
                    img_r = requests.get(imgs[0]["url"], timeout=30); img_r.raise_for_status()
                    with open(output_path, "wb") as f: f.write(img_r.content)
                    return os.path.getsize(output_path) > 50_000
        except Exception as e:
            if "402" in str(e) or "credits" in str(e): return False
            time.sleep(5)
    return False


def _try_r2_library(prompt: str, output_path: str,
                    supabase_url: str, supabase_key: str, r2_base: str) -> bool:
    if not supabase_url: return False
    try:
        words = _extract_keywords(prompt)
        rows = []
        for term in words[:2]:
            r = requests.get(
                f"{supabase_url}/rest/v1/image_cache"
                f"?topic=ilike.*{term}*&select=r2_key,public_url&limit=5&order=created_at.desc",
                headers={"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"},
                timeout=5)
            if r.status_code == 200: rows.extend(r.json())
        if not rows:
            r = requests.get(
                f"{supabase_url}/rest/v1/image_cache"
                f"?select=r2_key,public_url&limit=10&order=created_at.desc",
                headers={"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"},
                timeout=5)
            if r.status_code == 200: rows = r.json()
        if not rows: return False
        img_url = rows[0].get("public_url") or f"{r2_base.rstrip('/')}/{rows[0]['r2_key']}"
        img_r = requests.get(img_url, timeout=30, stream=True); img_r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in img_r.iter_content(8192): f.write(chunk)
        return os.path.getsize(output_path) > 10_000
    except Exception as e:
        print(f"  R2 Library: {e}"); return False


@app.local_entrypoint()
def main():
    result = generate_single_image.remote(
        prompt="ISRO scientists celebrating successful spacecraft launch, mission control room, "
               "screens showing rocket trajectory, Indian engineers in celebration, "
               "dramatic red and gold lighting",
        scene_idx=0, job_id="test-flux-001",
        cluster="Space", job_type="shorts", engine_mode="inbuilt",
    )
    print(f"Tier: {result['tier_used']} | R2: {result['r2_url']}")