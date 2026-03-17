import uuid
import random
import json
import time


# ---------------------------------
# VISUAL STYLE BASE
# ---------------------------------

BASE_STYLE = (
    "futuristic India, advanced technology, cinematic lighting, "
    "ultra realistic, modern Indian environment, blue neon accents"
)


# ---------------------------------
# CATEGORY LIBRARY
# ---------------------------------

AI_SCENES = [
    "AI robotics lab in India",
    "AI medical diagnostic system",
    "robotic hospital operating room",
    "AI research center India"
]

CITY_SCENES = [
    "futuristic Indian smart city skyline",
    "AI traffic management system India",
    "smart city infrastructure India",
]

SPACE_SCENES = [
    "Indian space station orbiting Earth",
    "satellite network above India",
]

INDUSTRY_SCENES = [
    "robotic factory India",
    "AI powered warehouse India"
]


SCENE_LIBRARY = AI_SCENES + CITY_SCENES + SPACE_SCENES + INDUSTRY_SCENES


# ---------------------------------
# PROMPT GENERATION
# ---------------------------------

def build_prompt(scene):

    return f"{scene}, {BASE_STYLE}"


def generate_prompts(topic):

    prompts = []

    # hook visual
    scene1 = random.choice(SCENE_LIBRARY)
    prompts.append(build_prompt(scene1))

    # trend visual
    scene2 = random.choice(SCENE_LIBRARY)
    prompts.append(build_prompt(scene2))

    # insight visual
    scene3 = random.choice(SCENE_LIBRARY)
    prompts.append(build_prompt(scene3))

    # future visual
    scene4 = random.choice(SCENE_LIBRARY)
    prompts.append(build_prompt(scene4))

    # ending visual
    scene5 = random.choice(SCENE_LIBRARY)
    prompts.append(build_prompt(scene5))

    return prompts


# ---------------------------------
# JOB PROCESSOR
# ---------------------------------

def process_job(job):

    topic = job["topic"]

    prompts = generate_prompts(topic)

    job["visual_prompts"] = prompts

    job["status"] = "visual_prompts_ready"

    return job


# ---------------------------------
# SIMULATED QUEUE LOOP
# ---------------------------------

def run_worker():

    print("Visual Prompt Worker Started")

    while True:

        try:

            job = {

                "job_id": str(uuid.uuid4()),

                "topic": "AI doctors in India",

                "script": {},

                "status": "script_complete"
            }

            print("\nProcessing Job:", job["job_id"])

            job = process_job(job)

            print("\nGenerated Prompts:\n")

            print(json.dumps(job["visual_prompts"], indent=2))

            time.sleep(5)

        except Exception as e:

            print("Worker error:", e)

            time.sleep(5)


# ---------------------------------
# ENTRY POINT
# ---------------------------------

if __name__ == "__main__":

    run_worker()
