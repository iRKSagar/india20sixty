import os
import uuid
import requests

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

TMP_DIR = "/tmp/youtube_upload"
os.makedirs(TMP_DIR, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


# --------------------------------------------------
# AUTH
# --------------------------------------------------

def get_authenticated_service():

    flow = InstalledAppFlow.from_client_secrets_file(
        "client_secret.json",
        SCOPES
    )

    credentials = flow.run_local_server(port=0)

    youtube = build(
        "youtube",
        "v3",
        credentials=credentials
    )

    return youtube


# --------------------------------------------------
# DOWNLOAD VIDEO
# --------------------------------------------------

def download_video(url, job_id):

    path = f"{TMP_DIR}/{job_id}.mp4"

    r = requests.get(url)

    with open(path, "wb") as f:
        f.write(r.content)

    return path


# --------------------------------------------------
# UPLOAD VIDEO
# --------------------------------------------------

def upload_video(youtube, video_path, title, description):

    request = youtube.videos().insert(

        part="snippet,status",

        body={
            "snippet": {
                "title": title,
                "description": description,
                "tags": ["AI", "India2060", "FutureTech"],
                "categoryId": "28"
            },

            "status": {
                "privacyStatus": "public"
            }
        },

        media_body=MediaFileUpload(
            video_path,
            chunksize=-1,
            resumable=True
        )

    )

    response = request.execute()

    return response


# --------------------------------------------------
# JOB PROCESSOR
# --------------------------------------------------

def process_job(job):

    youtube = get_authenticated_service()

    video_url = job["video"]

    job_id = job["job_id"]

    video_path = download_video(video_url, job_id)

    title = job.get("title", "Future of India 2060 🇮🇳")

    description = job.get(
        "description",
        "What could India look like by 2060?"
    )

    response = upload_video(

        youtube,
        video_path,
        title,
        description

    )

    job["youtube_id"] = response["id"]

    job["status"] = "uploaded"

    return job


# --------------------------------------------------
# TEST RUN
# --------------------------------------------------

def run_worker():

    job = {

        "job_id": str(uuid.uuid4()),

        "video": "VIDEO_URL_FROM_RENDER",

        "title": "AI Doctors in India 🇮🇳",

        "description": "Future of AI healthcare in India by 2060"
    }

    job = process_job(job)

    print("\nUploaded Video ID:")

    print(job["youtube_id"])


if __name__ == "__main__":

    run_worker()
