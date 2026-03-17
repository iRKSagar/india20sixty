import uuid

from workers.topic_worker.topic_worker import process_job as topic_worker
from workers.safety_worker.safety_worker import process_job as safety_worker
from workers.script_worker.script_worker import process_job as script_worker
from workers.visual_prompt_worker.visual_prompt_worker import process_job as visual_worker
from workers.image_cache_worker.image_cache_worker import process_job as cache_worker
from workers.image_worker.image_worker import process_job as image_worker
from workers.voice_worker.voice_worker import process_job as voice_worker
from workers.subtitle_worker.subtitle_worker import process_job as subtitle_worker
from workers.render_worker.render_worker import process_job as render_worker
from workers.youtube_upload_worker.youtube_upload_worker import process_job as youtube_worker
from workers.instagram_upload_worker.instagram_upload_worker import process_job as instagram_worker
from workers.analytics_worker.analytics_worker import process_job as analytics_worker


# ----------------------------------
# CONFIG
# ----------------------------------

TEST_MODE = True


# ----------------------------------
# PIPELINE
# ----------------------------------

def run_pipeline():

    print("\nStarting India20Sixty Pipeline\n")

    job = topic_worker()

    print("Topic Worker:", job["topic"])

    job = safety_worker(job)

    print("Safety Worker Passed")

    job = script_worker(job)

    print("Script Generated")

    job = visual_worker(job)

    print("Visual Prompts Created")

    job = cache_worker(job)

    print("Image Cache Checked")

    job = image_worker(job)

    print("Images Generated")

    job = voice_worker(job)

    print("Voice Generated")

    job = subtitle_worker(job)

    print("Subtitles Created")

    job = render_worker(job)

    print("Video Rendered")

    if not TEST_MODE:

        job = youtube_worker(job)
        print("Uploaded to YouTube")

        job = instagram_worker(job)
        print("Uploaded to Instagram")

    else:

        print("\nTEST MODE ENABLED")
        print("Upload workers skipped")

    job = analytics_worker(job)

    print("Analytics Recorded")

    print("\nPipeline Finished")

    return job


# ----------------------------------
# ENTRY
# ----------------------------------

if __name__ == "__main__":

    result = run_pipeline()

    print("\nFinal Job Output:\n")

    print(result)
