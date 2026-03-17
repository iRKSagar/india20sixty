import subprocess
import uuid
import os
import json
from pathlib import Path


# ----------------------------------
# PATH CONFIG
# ----------------------------------

IMAGE_FOLDER = Path("assets/images")
VIDEO_FOLDER = Path("assets/videos")
MUSIC_PATH = Path("assets/music/ambient_future.mp3")

VIDEO_FOLDER.mkdir(parents=True, exist_ok=True)


# ----------------------------------
# MOTION GENERATOR
# ----------------------------------

def create_motion_clip(image_path, output_path):

    cmd = [
        "ffmpeg",
        "-loop", "1",
        "-i", image_path,
        "-vf",
        "zoompan=z='min(zoom+0.0015,1.08)':d=150:s=1080x1920",
        "-t", "5",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-y",
        output_path
    ]

    subprocess.run(cmd)


# ----------------------------------
# MERGE CLIPS
# ----------------------------------

def merge_clips(clips, output):

    inputs = []

    for clip in clips:
        inputs.extend(["-i", clip])

    filter_complex = (
        "[0:v][1:v]xfade=transition=fade:duration=0.3:offset=4.7[v1];"
        "[v1][2:v]xfade=transition=fade:duration=0.3:offset=9.4[v2];"
        "[v2][3:v]xfade=transition=fade:duration=0.3:offset=14.1[v3];"
        "[v3][4:v]xfade=transition=fade:duration=0.3:offset=18.8[v]"
    )

    cmd = [
        "ffmpeg",
        *inputs,
        "-filter_complex", filter_complex,
        "-map", "[v]",
        "-y",
        output
    ]

    subprocess.run(cmd)


# ----------------------------------
# ADD AUDIO
# ----------------------------------

def add_audio(video_path, voice_path, output_path):

    cmd = [
        "ffmpeg",
        "-i", video_path,
        "-i", voice_path,
        "-i", str(MUSIC_PATH),
        "-filter_complex",
        "[1:a][2:a]amix=inputs=2:duration=longest[a]",
        "-map", "0:v",
        "-map", "[a]",
        "-c:v", "libx264",
        "-c:a", "aac",
        "-shortest",
        "-y",
        output_path
    ]

    subprocess.run(cmd)


# ----------------------------------
# ADD SUBTITLES
# ----------------------------------

def add_subtitles(video_path, subtitle_path, output):

    cmd = [
        "ffmpeg",
        "-i", video_path,
        "-vf", f"subtitles={subtitle_path}",
        "-c:a", "copy",
        "-y",
        output
    ]

    subprocess.run(cmd)


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    job_id = job["job_id"]

    images = job["images"]

    voice = job["audio_voice"]

    subtitles = job["subtitles"]

    temp_clips = []

    for i, image in enumerate(images):

        clip = f"temp_clip_{i}.mp4"

        create_motion_clip(image, clip)

        temp_clips.append(clip)

    merged_video = f"merged_{job_id}.mp4"

    merge_clips(temp_clips, merged_video)

    audio_video = f"audio_{job_id}.mp4"

    add_audio(merged_video, voice, audio_video)

    final_video = VIDEO_FOLDER / f"{job_id}.mp4"

    add_subtitles(audio_video, subtitles, str(final_video))

    job["video_path"] = str(final_video)

    job["status"] = "video_ready"

    return job


# ----------------------------------
# WORKER LOOP
# ----------------------------------

def run_worker():

    print("Render Worker Started")

    while True:

        try:

            job = {

                "job_id": str(uuid.uuid4()),

                "images": [
                    "assets/images/test/image1.png",
                    "assets/images/test/image2.png",
                    "assets/images/test/image3.png",
                    "assets/images/test/image4.png",
                    "assets/images/test/image5.png"
                ],

                "audio_voice": "voice.mp3",

                "subtitles": "subtitles.srt"
            }

            job = process_job(job)

            print("\nVideo created:")

            print(job["video_path"])

            break

        except Exception as e:

            print("Render error:", e)


if __name__ == "__main__":

    run_worker()
