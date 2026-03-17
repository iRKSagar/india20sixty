import random
import uuid
import json
import time


# ---------------------------------
# CONFIG
# ---------------------------------

SCRIPT_MIN_WORDS = 50
SCRIPT_MAX_WORDS = 65

TWIST_PROBABILITY = 0.35


# ---------------------------------
# SCRIPT GENERATION
# ---------------------------------

def generate_trend(topic):

    templates = [
        f"Aaj {topic} se related technology already develop ho rahi hai.",
        f"Abhi bhi {topic} se related systems real world mein test ho rahe hain.",
        f"Researchers aur startups {topic} par rapidly kaam kar rahe hain."
    ]

    return random.choice(templates)


def generate_insight(topic):

    templates = [
        f"AI systems thousands of data points seconds mein analyse kar sakte hain.",
        f"Machines complex decisions humans se faster process kar sakti hain.",
        f"Technology gradually industries ko transform kar rahi hai."
    ]

    insight = random.choice(templates)

    # subtle twist
    if random.random() < TWIST_PROBABILITY:

        twist_templates = [
            "Kabhi kabhi lagta hai machines humans se zyada patience rakhti hain.",
            "Aur machines ko coffee break bhi nahi chahiye.",
            "Aur shayad machines kabhi complain bhi nahi karti."
        ]

        insight += " " + random.choice(twist_templates)

    return insight


def generate_future(topic):

    templates = [
        f"2060 tak {topic} India ke daily life ka normal part ban sakta hai.",
        f"Agar development isi speed se chala, to 2060 tak {topic} common ho sakta hai.",
        f"Future mein {topic} millions logon ki life change kar sakta hai."
    ]

    return random.choice(templates)


def generate_question(topic):

    templates = [
        f"Kya India ready hai is future ke liye?",
        f"Kya aap imagine kar sakte ho ye future?",
        f"Agar ye reality ban gaya to life kaise change hogi?"
    ]

    return random.choice(templates)


# ---------------------------------
# SCRIPT BUILDER
# ---------------------------------

def build_script(topic, hook):

    trend = generate_trend(topic)

    insight = generate_insight(topic)

    future = generate_future(topic)

    question = generate_question(topic)

    script = {
        "hook": hook,
        "trend": trend,
        "insight": insight,
        "future": future,
        "question": question
    }

    return script


# ---------------------------------
# JOB PROCESSOR
# ---------------------------------

def process_job(job):

    topic = job["topic"]

    hook = job["hook"]

    script = build_script(topic, hook)

    job["script"] = script

    job["status"] = "script_complete"

    return job


# ---------------------------------
# SIMULATED QUEUE LOOP
# ---------------------------------

def run_worker():

    print("Script Worker Started")

    while True:

        try:

            # simulate queue job
            job = {
                "job_id": str(uuid.uuid4()),
                "topic": "AI doctors in India",
                "hook": "Socho agar AI doctors India mein common ho jayein",
                "status": "script_pending"
            }

            print("\nProcessing Job:", job["job_id"])

            job = process_job(job)

            print("\nGenerated Script:\n")

            print(json.dumps(job["script"], indent=2))

            time.sleep(5)

        except Exception as e:

            print("Worker error:", e)

            time.sleep(5)


# ---------------------------------
# ENTRY POINT
# ---------------------------------

if __name__ == "__main__":

    run_worker()
