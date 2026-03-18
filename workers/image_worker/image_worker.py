def process_job(job):

    prompts = job.visual_prompts
    images = []

    for prompt in prompts:
        images.append({
            "prompt": prompt,
            "image_url": None
        })

    job.images = images
    job.status = "images_generated"

    return job
