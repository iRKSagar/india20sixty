# India20Sixty Project Skeleton

india20sixty/

    config/
        settings.yaml
        prompts.yaml

    data/
        signals/
        topics/
        jobs/

    queue/
        queue_manager.py
        job_schema.py

    workers/
        radar_worker.py
        topic_worker.py
        safety_worker.py
        hook_worker.py
        script_worker.py
        visual_prompt_worker.py
        image_worker.py
        motion_worker.py
        voice_worker.py
        subtitle_worker.py
        render_worker.py
        upload_worker.py
        analytics_worker.py

    render/
        ffmpeg_templates/
        motion_presets/

    assets/
        music/
        overlays/
        fonts/

    videos/
        drafts/
        rendered/
        published/

    logs/
        worker_logs/
        error_logs/

    main/
        orchestrator.py


# queue_manager.py

class QueueManager:
    def __init__(self):
        self.topic_queue = []
        self.script_queue = []
        self.image_queue = []
        self.video_queue = []
        self.upload_queue = []

    def push(self, queue, job):
        queue.append(job)

    def pop(self, queue):
        if queue:
            return queue.pop(0)
        return None


# job_schema.py

class Job:
    def __init__(self, topic):
        self.topic = topic
        self.hook = None
        self.script = None
        self.visual_prompts = []
        self.images = []
        self.voice = None
        self.video = None
        self.status = "pending"


# orchestrator.py

from queue.queue_manager import QueueManager

queue = QueueManager()


def run_pipeline():
    while True:
        job = queue.pop(queue.topic_queue)

        if job:
            print("Processing topic:", job.topic)


if __name__ == "__main__":
    run_pipeline()
