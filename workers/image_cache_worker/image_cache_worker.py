import json
import hashlib
from pathlib import Path


# ----------------------------------
# PATH SETUP
# ----------------------------------

CACHE_DIR = Path("data/image_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_FILE = CACHE_DIR / "cache.json"


# ----------------------------------
# LOAD CACHE
# ----------------------------------

def load_cache():

    if not CACHE_FILE.exists():

        return {"cache": {}}

    with open(CACHE_FILE, "r") as f:

        return json.load(f)


# ----------------------------------
# SAVE CACHE
# ----------------------------------

def save_cache(cache):

    with open(CACHE_FILE, "w") as f:

        json.dump(cache, f, indent=2)


# ----------------------------------
# HASH PROMPT
# ----------------------------------

def prompt_hash(prompt):

    return hashlib.md5(prompt.encode()).hexdigest()


# ----------------------------------
# CHECK CACHE
# ----------------------------------

def check_cache(prompt):

    cache = load_cache()

    key = prompt_hash(prompt)

    return cache["cache"].get(key)


# ----------------------------------
# ADD TO CACHE
# ----------------------------------

def add_cache(prompt, image_path):

    cache = load_cache()

    key = prompt_hash(prompt)

    cache["cache"][key] = image_path

    save_cache(cache)


# ----------------------------------
# PROCESS JOB
# ----------------------------------

def process_job(job):

    prompts = job["visual_prompts"]

    cached_images = []
    uncached_prompts = []

    for prompt in prompts:

        cached = check_cache(prompt)

        if cached:

            cached_images.append(cached)

        else:

            uncached_prompts.append(prompt)

    job["cached_images"] = cached_images

    job["uncached_prompts"] = uncached_prompts

    job["status"] = "cache_checked"

    return job


# ----------------------------------
# TEST RUN
# ----------------------------------

if __name__ == "__main__":

    job = {

        "visual_prompts": [

            "AI robotics lab India futuristic lighting",
            "futuristic hospital India 2060 cinematic lighting"

        ]

    }

    job = process_job(job)

    print("\nCache Result:\n")

    print(job)
