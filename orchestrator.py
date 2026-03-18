from queue.queue_manager import QueueManager
from queue.job_schema import Job

queue = QueueManager()


def seed_jobs():

    topics = [
        "AI doctors in India",
        "India space stations",
        "robot farming India"
    ]

    for topic in topics:

        job = Job(topic)

        queue.push(queue.topic_queue, job)


def run_pipeline():

    seed_jobs()

    while True:

        job = queue.pop(queue.topic_queue)

        if job:

            print("Processing topic:", job.topic)

            job.status = "topic_processed"

            queue.push(queue.script_queue, job)


if __name__ == "__main__":
    run_pipeline()
