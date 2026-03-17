import uuid

# Import workers
from workers.script_worker.script_worker import process_job as script_worker
from workers.visual_prompt_worker.visual_prompt_worker import process_job as visual_worker
from workers.image_worker.image_worker import process_job as image_worker
from workers.voice_worker.voice_worker import process_job as voice_worker
from workers.subtitle_worker.subtitle_worker import process_job as subtitle_worker
from workers.render_worker.render_worker import process_job as render_worker
from workers.youtube_upload_worker.youtube_upload_worker import process_job as youtube_worker
from workers.instagram_upload_worker.instagram_upload_worker import process_job as instagram_worker
from workers.analytics_worker.analytics_worker import process_job as analytics_worker


# ----------------------------------
# CREATE JOB
# ----------------------------------

def create_job(topic):

    job = {

        "job_id": str(uuid.uuid4()),

        "topic": topic,

        "hook": f"Socho agar {topic} reality ban jaye…",

        "status": "created"
    }

    return job


# ----------------------------------
# PIPELINE EXECUTION
# ----------------------------------

def run_pipeline(topic):

    job = create_job(topic)

    print("\nJOB CREATED")
    print(job["job_id"])

    try:

        print("\nRunning Script Worker")
        job = script_worker(job)

        print("\nRunning Visual Prompt Worker")
        job = visual_worker(job)

        print("\nRunning Image Worker")
        job = image_worker(job)

        print("\nRunning Voice Worker")
        job = voice_worker(job)

        print("\nRunning Subtitle Worker")
        job = subtitle_worker(job)

        print("\nRunning Render Worker")
        job = render_worker(job)

        print("\nRunning YouTube Upload Worker")
        job = youtube_worker(job)

        print("\nRunning Instagram Upload Worker")
        job = instagram_worker(job)

        print("\nRunning Analytics Worker")
        job = analytics_worker(job)

        print("\nPIPELINE COMPLETE")

        return job

    except Exception as e:

        print("\nPIPELINE FAILED:", e)

        job["status"] = "failed"

        return job


# ----------------------------------
# MAIN ENTRY
# ----------------------------------

if __name__ == "__main__":

    topic = "AI doctors in India"

    final_job = run_pipeline(topic)

    print("\nFinal Job Status:")

    print(final_job)
