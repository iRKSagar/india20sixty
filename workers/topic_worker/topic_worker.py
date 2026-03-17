import json
import uuid
from pathlib import Path


# ----------------------------------
# PATH SETUP
# ----------------------------------

DATA_PATH = Path("data/topics")
DATA_PATH.mkdir(parents=True, exist_ok=True)

STATE_FILE = DATA_PATH / "topic_state.json"


# ----------------------------------
# CLUSTER DEFINITIONS
# ----------------------------------

CLUSTERS = {

    "AI Future": [

        "AI doctors in India",
        "AI teachers in India",
        "AI engineers in India",
        "AI hospitals in India",
        "AI farming systems in India"

    ],

    "Future Cities": [

        "smart cities in India",
        "AI traffic systems in India",
        "flying taxis in India",
        "robot delivery in Indian cities",
        "hyperloop transportation India"

    ],

    "Space India": [

        "Indian moon base",
        "India space stations",
        "satellite internet India",
        "space mining by India",
        "India Mars missions"

    ],

    "Future Jobs": [

        "jobs in India by 2060",
        "AI designers in India",
        "robot engineers in India",
        "space engineers India",
        "future entrepreneurs India"

    ]
}


CLUSTER_NAMES = list(CLUSTERS.keys())


# ----------------------------------
# STATE MANAGEMENT
# ----------------------------------

def load_state():

    if not STATE_FILE.exists():

        return {

            "cluster_index": 0,
            "topic_index": 0
        }

    with open(STATE_FILE, "r") as f:

        return json.load(f)


def save_state(state):

    with open(STATE_FILE, "w") as f:

        json.dump(state, f, indent=2)


# ----------------------------------
# HOOK GENERATOR
# ----------------------------------

def generate_hook(topic):

    hooks = [

        f"Socho agar {topic} reality ban jaye…",

        f"Ek interesting future idea: {topic}",

        f"2035 tak {topic} possible ho sakta hai…",

        f"Kya {topic} India mein common ho sakta hai?"

    ]

    return hooks[hash(topic) % len(hooks)]


# ----------------------------------
# TOPIC SELECTION
# ----------------------------------

def select_topic():

    state = load_state()

    cluster_index = state["cluster_index"]

    topic_index = state["topic_index"]

    cluster_name = CLUSTER_NAMES[cluster_index]

    topics = CLUSTERS[cluster_name]

    topic = topics[topic_index]

    # update state

    topic_index += 1

    if topic_index >= len(topics):

        topic_index = 0

        cluster_index = (cluster_index + 1) % len(CLUSTER_NAMES)

    new_state = {

        "cluster_index": cluster_index,
        "topic_index": topic_index
    }

    save_state(new_state)

    return cluster_name, topic


# ----------------------------------
# JOB CREATOR
# ----------------------------------

def create_job():

    cluster, topic = select_topic()

    hook = generate_hook(topic)

    job = {

        "job_id": str(uuid.uuid4()),

        "cluster": cluster,

        "topic": topic,

        "hook": hook,

        "status": "topic_generated"
    }

    return job


# ----------------------------------
# WORKER ENTRY
# ----------------------------------

def process_job():

    job = create_job()

    return job


# ----------------------------------
# TEST RUN
# ----------------------------------

if __name__ == "__main__":

    job = process_job()

    print("\nGenerated Topic Job:\n")

    print(json.dumps(job, indent=2))
