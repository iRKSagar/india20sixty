import requests
import uuid
from pathlib import Path


# ----------------------------------
# CONFIG
# ----------------------------------

INSTAGRAM_ACCESS_TOKEN = "YOUR_TOKEN"

INSTAGRAM_USER_ID = "YOUR_INSTAGRAM_ID"

GRAPH_API = "https://graph.facebook.com/v18.0"


# ----------------------------------
# CAPTION BUILDER
# ----------------------------------

def build_caption(job):

    topic = job["topic"]

    caption = (
        f"Socho agar {topic} reality ban jaye...\n\n"
        "#FutureIndia #India2060 #IndiaTech #TechFuture"
    )

    return caption


# ----------------------------------
# CREATE MEDIA CONTAINER
# ----------------------------------

def create_container(video_url, caption):

    url = f"{GRAPH_API}/{INSTAGRAM_USER_ID}/media"

    params = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "access_token": INSTAGRAM_ACCESS_TOKEN
    }

    response = requests.post(url, params=params)

    return response.json()["id"]


# ----------------------------------
# PUBLISH MEDIA
# ----------------------------------

def publish_container(container_id):

    url = f"{GRAPH_API}/{INSTAGRAM_USER_ID}/media_publish"

    params = {
        "creation_id": container_id,
        "access_token": INSTAGRAM_ACCESS_TOKEN
    }

    response = requests.post(url, params=params)

    return response.json()


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    video_path = job["video_path"]

    caption = build_caption(job)

    # NOTE
    # Instagram requires a public URL
    # video must be hosted temporarily

    video_url = job["video_public_url"]

    container_id = create_container(video_url, caption)

    result = publish_container(container_id)

    job["instagram_post_id"] = result["id"]

    job["status"] = "instagram_uploaded"

    return job


# ----------------------------------
# WORKER LOOP
# ----------------------------------

def run_worker():

    print("Instagram Upload Worker Started")

    job = {

        "job_id": str(uuid.uuid4()),

        "topic": "AI doctors in India",

        "video_path": "assets/videos/test_video.mp4",

        "video_public_url": "https://example.com/test_video.mp4"
    }

    job = process_job(job)

    print("\nInstagram Post ID:")

    print(job["instagram_post_id"])


if __name__ == "__main__":

    run_worker()
