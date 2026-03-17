import re


# ----------------------------------
# BLOCKED KEYWORDS
# ----------------------------------

BLOCKED_KEYWORDS = [

    "war",
    "military",
    "weapon",
    "missile",
    "politics",
    "election",
    "government control",
    "religion",
    "terror",
    "conflict",
    "surveillance"
]


# ----------------------------------
# TOPIC CHECK
# ----------------------------------

def is_safe(topic):

    topic_lower = topic.lower()

    for word in BLOCKED_KEYWORDS:

        if re.search(word, topic_lower):

            return False

    return True


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    topic = job["topic"]

    if not is_safe(topic):

        raise Exception(f"Unsafe topic detected: {topic}")

    job["status"] = "topic_safe"

    return job


# ----------------------------------
# TEST RUN
# ----------------------------------

if __name__ == "__main__":

    job = {

        "topic": "AI doctors in India"

    }

    job = process_job(job)

    print("\nTopic Passed Safety Filter")

    print(job)
