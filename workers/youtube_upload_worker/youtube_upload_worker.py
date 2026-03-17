import os
import uuid

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


# ----------------------------------
# CONFIG
# ----------------------------------

CLIENT_SECRET_FILE = "core/client_secret.json"

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

CATEGORY_ID = "28"   # Science & Technology


# ----------------------------------
# AUTHENTICATION
# ----------------------------------

def get_authenticated_service():

    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRET_FILE,
        SCOPES
    )

    credentials = flow.run_local_server(port=0)

    return build("youtube", "v3", credentials=credentials)


# ----------------------------------
# METADATA GENERATOR
# ----------------------------------

def build_metadata(job):

    topic = job["topic"]

    title = f"{topic} by 2060?"

    description = (
        "What could India look like by 2060?\n\n"
        "#FutureIndia #India2060 #IndiaTech"
    )

    tags = [
        "Future India",
        "India 2060",
        "AI India",
        "Future technology"
    ]

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": CATEGORY_ID
        },
        "status": {
            "privacyStatus": "public"
        }
    }

    return body


# ----------------------------------
# VIDEO UPLOAD
# ----------------------------------

def upload_video(job):

    youtube = get_authenticated_service()

    video_file = job["video_path"]

    body = build_metadata(job)

    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media
    )

    response = request.execute()

    video_id = response["id"]

    youtube_url = f"https://youtube.com/watch?v={video_id}"

    return youtube_url


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    url = upload_video(job)

    job["youtube_url"] = url

    job["status"] = "youtube_uploaded"

    return job


# ----------------------------------
# WORKER LOOP
# ----------------------------------

def run_worker():

    print("YouTube Upload Worker Started")

    job = {

        "job_id": str(uuid.uuid4()),

        "topic": "AI doctors in India",

        "video_path": "assets/videos/test_video.mp4"
    }

    job = process_job(job)

    print("\nUploaded to YouTube:")

    print(job["youtube_url"])


if __name__ == "__main__":

    run_worker()
