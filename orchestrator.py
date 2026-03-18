```python
import traceback


# -----------------------------------
# WORKER IMPORTS
# -----------------------------------

from workers.topic_worker.topic_worker import process_job as topic_worker
from workers.safety_worker.safety_worker import process_job as safety_worker
from workers.script_worker.script_worker import process_job as script_worker
from workers.visual_prompt_worker.visual_prompt_worker import process_job as visual_prompt_worker
from workers.image_cache_worker.image_cache_worker import process_job as image_cache_worker
from workers.image_worker.image_worker import process_job as image_worker
from workers.voice_worker.voice_worker import process_job as voice_worker
from workers.subtitle_worker.subtitle_worker import process_job as subtitle_worker
from workers.render_worker.render_worker import process_job as render_worker
from workers.storage_worker.storage_worker import process_job as storage_worker
from workers.youtube_upload_worker.youtube_upload_worker import process_job as youtube_upload_worker
from workers.instagram_upload_worker.instagram_upload_worker import process_job as instagram_upload_worker
from workers.analytics_worker.analytics_worker import process_job as analytics_worker


# -----------------------------------
# PIPELINE ORDER
# -----------------------------------

PIPELINE = [

    ("topic_worker", topic_worker),
    ("safety_worker", safety_worker),
    ("script_worker", script_worker),
    ("visual_prompt_worker", visual_prompt_worker),
    ("image_cache_worker", image_cache_worker),
    ("image_worker", image_worker),
    ("voice_worker", voice_worker),
    ("subtitle_worker", subtitle_worker),
    ("render_worker", render_worker),
    ("storage_worker", storage_worker),
    ("youtube_upload_worker", youtube_upload_worker),
    ("instagram_upload_worker", instagram_upload_worker),
    ("analytics_worker", analytics_worker)

]


# -----------------------------------
# SAFE WORKER EXECUTION
# -----------------------------------

def run_worker(worker_name, worker_func, job):

    try:

        print(f"\nRunning {worker_name}")

        job = worker_func(job)

        if not isinstance(job, dict):
            raise Exception("Worker did not return job dictionary")

        return job

    except Exception as e:

        print(f"\nERROR in {worker_name}")
        print(str(e))
        print(traceback.format_exc())

        job["status"] = f"{worker_name}_failed"
        job["error"] = str(e)

        return job


# -----------------------------------
# RUN FULL PIPELINE
# -----------------------------------

def run_pipeline(job):

    for worker_name, worker_func in PIPELINE:

        job = run_worker(worker_name, worker_func, job)

        if "failed" in job.get("status", ""):

            print("\nPipeline stopped due to failure")

            return job

    job["status"] = "pipeline_complete"

    return job


# -----------------------------------
# TEST JOB GENERATOR
# -----------------------------------

def create_test_job():

    return {

        "topic": None,
        "hook": None,
        "script": None,

        "visual_prompts": [],
        "images": [],

        "voice": None,
        "subtitles": [],

        "video": None,

        "youtube_views": 0,
        "youtube_likes": 0,
        "youtube_comments": 0,

        "instagram_views": 0,
        "instagram_likes": 0,
        "instagram_comments": 0
    }


# -----------------------------------
# MAIN ENTRY
# -----------------------------------

if __name__ == "__main__":

    print("\nIndia20Sixty Pipeline Starting\n")

    job = create_test_job()

    result = run_pipeline(job)

    print("\nPipeline Finished\n")

    print(result)
```
