import os
import uuid
import requests

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

TMP_DIR = "/tmp/youtube_upload"
TOKEN_FILE = "/tmp/youtube_token.json"

os.makedirs(TMP_DIR, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# --------------------------------------------------
# AUTH
# --------------------------------------------------

def get_authenticated_service():

    client_config = {
        "installed": {
            "client_id": os.environ["YOUTUBE_CLIENT_ID"],
            "project_id": os.environ["YOUTUBE_PROJECT_ID"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": os.environ["YOUTUBE_CLIENT_SECRET"],
            "redirect_uris": ["http://localhost"]
        }
    }

    credentials = None

    # reuse token if exists
    if os.path.exists(TOKEN_FILE):

        credentials = Credentials.from_authorized_user_file(
            TOKEN_FILE,
            SCOPES
        )

    if not credentials or not credentials.valid:

        flow = InstalledAppFlow.from_client_config(
            client_config,
            SCOPES
        )

        credentials = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())

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

    privacy = os.environ.get("YOUTUBE_PRIVACY", "public")

    request = youtube.videos().insert(

        part="snippet,status",

        body={

            "snippet": {

                "title": title,
                "description": description,
                "tags": [
                    "India2060",
                    "FutureTech",
                    "AI",
                    "IndiaFuture"
                ],

                "categoryId": "28"

            },

            "status": {

                "privacyStatus": privacy

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

    title = job.get(
        "title",
        "Future of India 2060 🇮🇳"
    )

    description = job.get(
        "description",
        "Exploring what India could look like by 2060."
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
