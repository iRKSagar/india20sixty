import random
import json
import uuid


# ----------------------------------
# BASE STYLE
# ----------------------------------

BASE_STYLE = (
    "futuristic India, advanced technology, cinematic lighting, "
    "ultra realistic, modern Indian environment, blue neon accents"
)


# ----------------------------------
# SCENE POOLS
# ----------------------------------

HOOK_SCENES = [

    "Indian futuristic city skyline at sunrise",
    "advanced AI hospital entrance India",
    "robotic laboratory India",
    "high-tech smart city control room"

]

TREND_SCENES = [

    "AI medical scanner analyzing patient",
    "robotic system assisting doctor",
    "AI classroom with digital teacher",
    "smart city traffic control center"

]

INSIGHT_SCENES = [

    "AI analyzing medical data on holographic screen",
    "robot performing precise surgery",
    "AI system monitoring thousands of sensors",
    "advanced research lab India"

]

FUTURE_SCENES = [

    "futuristic hospital India 2060",
    "fully autonomous AI healthcare system",
    "futuristic Indian megacity skyline 2060",
    "AI powered infrastructure India future"

]

ENDING_SCENES = [

    "wide cinematic shot of futuristic Indian city",
    "Indian skyline glowing with advanced technology",
    "India 2060 futuristic megacity",
    "sunset view of advanced smart India"

]


# ----------------------------------
# PROMPT BUILDER
# ----------------------------------

def build_prompt(scene):

    return f"{scene}, {BASE_STYLE}"


# ----------------------------------
# PROMPT GENERATOR
# ----------------------------------

def generate_prompts():

    prompts = [

        build_prompt(random.choice(HOOK_SCENES)),
        build_prompt(random.choice(TREND_SCENES)),
        build_prompt(random.choice(INSIGHT_SCENES)),
        build_prompt(random.choice(FUTURE_SCENES)),
        build_prompt(random.choice(ENDING_SCENES))

    ]

    return prompts


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    prompts = generate_prompts()

    job["visual_prompts"] = prompts

    job["status"] = "visual_prompts_ready"

    return job


# ----------------------------------
# TEST RUN
# ----------------------------------

if __name__ == "__main__":

    job = {

        "job_id": str(uuid.uuid4()),
        "topic": "AI doctors in India"

    }

    job = process_job(job)

    print("\nGenerated Visual Prompts:\n")

    print(json.dumps(job["visual_prompts"], indent=2))
