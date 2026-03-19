import random
import uuid
import json


# ----------------------------------
# BASE STYLE
# ----------------------------------

BASE_STYLE = (
    "futuristic India, advanced technology, cinematic lighting, "
    "ultra realistic, 8k, dramatic lighting, modern Indian environment, "
    "blue neon accents"
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

def build_prompt(scene, topic):

    return f"{scene}, {topic}, {BASE_STYLE}"


# ----------------------------------
# SCENE OBJECT BUILDER
# ----------------------------------

def build_scene(scene_type, scene_pool, topic):

    scene = random.choice(scene_pool)

    return {

        "scene_type": scene_type,
        "prompt": build_prompt(scene, topic),
        "duration": 5

    }


# ----------------------------------
# PROMPT GENERATOR
# ----------------------------------

def generate_scenes(topic):

    scenes = [

        build_scene("hook", HOOK_SCENES, topic),
        build_scene("trend", TREND_SCENES, topic),
        build_scene("insight", INSIGHT_SCENES, topic),
        build_scene("future", FUTURE_SCENES, topic),
        build_scene("ending", ENDING_SCENES, topic)

    ]

    return scenes


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    topic = job["topic"]

    scenes = generate_scenes(topic)

    job["scenes"] = scenes

    job["visual_prompts"] = [scene["prompt"] for scene in scenes]

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

    print("\nGenerated Scenes:\n")

    print(json.dumps(job["scenes"], indent=2))
