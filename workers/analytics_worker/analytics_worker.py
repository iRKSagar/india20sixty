```python
import json
from pathlib import Path


# ----------------------------------
# PATH SETUP
# ----------------------------------

BASE_PATH = Path("data")
ANALYTICS_PATH = BASE_PATH / "analytics"
FILE_PATH = ANALYTICS_PATH / "performance.json"

ANALYTICS_PATH.mkdir(parents=True, exist_ok=True)


# ----------------------------------
# SAFE JSON LOAD
# ----------------------------------

def load_data():
    try:
        if not FILE_PATH.exists():
            return {"videos": []}

        with open(FILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    except Exception:
        # corrupted JSON protection
        return {"videos": []}


# ----------------------------------
# SAFE JSON SAVE
# ----------------------------------

def save_data(data):

    try:
        with open(FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    except Exception as e:
        print("Analytics save error:", e)


# ----------------------------------
# SCORE CALCULATION
# ----------------------------------

def calculate_score(views, likes, comments):

    try:
        score = (
            views * 0.6 +
            likes * 0.3 +
            comments * 0.1
        )
        return round(score, 2)

    except Exception:
        return 0


# ----------------------------------
# PROCESS JOB
# ----------------------------------

def process_job(job):

    try:

        topic = job.get("topic", "unknown")

        youtube_views = job.get("youtube_views", 0)
        youtube_likes = job.get("youtube_likes", 0)
        youtube_comments = job.get("youtube_comments", 0)

        instagram_views = job.get("instagram_views", 0)
        instagram_likes = job.get("instagram_likes", 0)
        instagram_comments = job.get("instagram_comments", 0)

        total_views = youtube_views + instagram_views
        total_likes = youtube_likes + instagram_likes
        total_comments = youtube_comments + instagram_comments

        score = calculate_score(
            total_views,
            total_likes,
            total_comments
        )

        data = load_data()

        record = {

            "topic": topic,

            "youtube_views": youtube_views,
            "youtube_likes": youtube_likes,
            "youtube_comments": youtube_comments,

            "instagram_views": instagram_views,
            "instagram_likes": instagram_likes,
            "instagram_comments": instagram_comments,

            "total_views": total_views,
            "total_likes": total_likes,
            "total_comments": total_comments,

            "performance_score": score
        }

        data["videos"].append(record)

        save_data(data)

        job["performance_score"] = score
        job["status"] = "analytics_recorded"

        return job

    except Exception as e:

        job["status"] = "analytics_error"
        job["analytics_error"] = str(e)

        return job


# ----------------------------------
# LOCAL TEST WORKER
# ----------------------------------

def run_worker():

    print("Analytics Worker Started\n")

    test_job = {

        "topic": "AI doctors in India",

        "youtube_views": 15000,
        "youtube_likes": 820,
        "youtube_comments": 75,

        "instagram_views": 9400,
        "instagram_likes": 510,
        "instagram_comments": 33
    }

    result = process_job(test_job)

    print("Worker Result:\n")
    print(json.dumps(result, indent=2))


# ----------------------------------
# ENTRYPOINT
# ----------------------------------

if __name__ == "__main__":

    run_worker()
```
