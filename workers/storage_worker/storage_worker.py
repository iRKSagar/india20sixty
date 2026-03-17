import boto3
from pathlib import Path
from core.config import (
    R2_ACCOUNT_ID,
    R2_ACCESS_KEY,
    R2_SECRET_KEY,
    R2_BUCKET,
    R2_PUBLIC_URL
)

# ----------------------------------
# CONNECT R2
# ----------------------------------

def get_r2_client():

    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )


# ----------------------------------
# UPLOAD VIDEO
# ----------------------------------

def upload_video(video_path, job_id):

    client = get_r2_client()

    key = f"videos/{job_id}.mp4"

    client.upload_file(video_path, R2_BUCKET, key)

    public_url = f"{R2_PUBLIC_URL}/{key}"

    return public_url


# ----------------------------------
# PROCESS JOB
# ----------------------------------

def process_job(job):

    video_path = job["video_path"]

    job_id = job["job_id"]

    public_url = upload_video(video_path, job_id)

    job["video_public_url"] = public_url

    job["status"] = "video_uploaded"

    return job
