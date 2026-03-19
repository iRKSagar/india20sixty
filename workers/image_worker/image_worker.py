import os
import requests

LEONARDO_API_KEY = os.getenv("LEONARDO_API_KEY")

def process_job(job):

    prompts = job["visual_prompts"]

    images = []

    for i,prompt in enumerate(prompts):

        r = requests.post(
            "https://cloud.leonardo.ai/api/rest/v1/generations",
            headers={
                "Authorization": f"Bearer {LEONARDO_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "prompt": prompt,
                "width":1080,
                "height":1920,
                "num_images":1
            }
        )

        data = r.json()

        img_url = data["generations_by_pk"]["generated_images"][0]["url"]

        img = requests.get(img_url).content

        path = f"/tmp/img_{i}.png"

        with open(path,"wb") as f:
            f.write(img)

        images.append(path)

    job["images"] = images

    return job
