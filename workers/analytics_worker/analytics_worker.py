import json
import os
from pathlib import Path


# ----------------------------------
# PATH
# ----------------------------------

DATA_PATH = Path("data/analytics")

DATA_PATH.mkdir(parents=True, exist_ok=True)

FILE_PATH = DATA_PATH / "performance.json"


# ----------------------------------
# LOAD DATA
# ----------------------------------

def load_data():

    if not FILE_PATH.exists():

        return {"videos": []}

    with open(FILE_PATH, "r") as f:

        return json.load(f)


# ----------------------------------
# SAVE DATA
# ----------------------------------

def save_data(data):

    with open(FILE_PATH, "w") as f:

        json.dump(data, f, indent=2)


# ----------------------------------
# SCORE CALCULATION
# ----------------------------------

def calculate_score(views, likes, comments):

    score = (
        views * 0.6 +
        likes * 0.3 +
        comments * 0.1
    )

    return score


# ----------------------------------
# PROCESS JOB
# ----------------------------------

def process_job(job):

    topic = job["topic"]

    youtube_views = job.get("youtube_views", 0)

    youtube_likes = job.get("youtube_likes", 0)

    youtube_comments = job.get("youtube_comments", 0)

    instagram_views = job.get("instagram_views", 0)

    instagram_likes = job.get("instagram_likes", 0)

    instagram_comments = job.get("instagram_comments", 0)

    total_views = youtube_views + instagram_views

    total_likes = youtube_likes + instagram_likes

    total_comments = youtube_comments + instagram_comments

    score = calculate_score(total_views, total_likes, total_comments)

    data = load_data()

    record = {

        "topic": topic,

        "youtube_views": youtube_views,
        "youtube_likes": youtube_likes,

        "instagram_views": instagram_views,
        "instagram_likes": instagram_likes,

        "score": score
    }

    data["videos"].append(record)

    save_data(data)

    job["performance_score"] = score

    job["status"] = "analytics_recorded"

    return job


# ----------------------------------
# WORKER
# ----------------------------------

def run_worker():

    print("Analytics Worker Started")

    job = {

        "topic": "AI doctors in India",

        "youtube_views": 15000,
        "youtube_likes": 820,
        "youtube_comments": 75,

        "instagram_views": 9400,
        "instagram_likes": 510,
        "instagram_comments": 33
    }

    job = process_job(job)

    print("\nPerformance score:")

    print(job["performance_score"])


if __name__ == "__main__":

    run_worker()
