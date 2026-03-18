```python
from workers.topic_worker.topic_worker import process_job as topic_worker
from workers.safety_worker.safety_worker import process_job as safety_worker
from workers.script_worker.script_worker import process_job as script_worker
from workers.visual_prompt_worker.visual_prompt_worker import process_job as prompt_worker
from workers.image_worker.image_worker import process_job as image_worker
from workers.voice_worker.voice_worker import process_job as voice_worker
from workers.subtitle_worker.subtitle_worker import process_job as subtitle_worker
from workers.render_worker.render_worker import process_job as render_worker
from workers.storage_worker.storage_worker import process_job as storage_worker
from workers.youtube_upload_worker.youtube_upload_worker import process_job as youtube_upload
from workers.analytics_worker.analytics_worker import process_job as analytics_worker


def run_pipeline(job):

    job = topic_worker(job)
    job = safety_worker(job)
    job = script_worker(job)
    job = prompt_worker(job)
    job = image_worker(job)
    job = voice_worker(job)
    job = subtitle_worker(job)
    job = render_worker(job)
    job = storage_worker(job)
    job = youtube_upload(job)
    job = analytics_worker(job)

    return job


if __name__ == "__main__":

    job = {
        "topic": None,
        "script": None,
        "visual_prompts": [],
        "images": [],
        "voice": None,
        "subtitles": [],
        "video": None
    }

    result = run_pipeline(job)

    print("Pipeline completed")
    print(result)
```
