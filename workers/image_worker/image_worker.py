import os
import uuid
import time
import requests

LEONARDO_API_KEY = os.getenv("LEONARDO_API_KEY")

LEONARDO_URL = "https://cloud.leonardo.ai/api/rest/v1/generations"


# --------------------------------------
# GENERATE IMAGE FROM LEONARDO
# --------------------------------------

def generate_image(prompt, image_id):

    headers = {
        "Authorization": f"Bearer {LEONARDO_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "prompt": prompt,
        "width": 1080,
        "height": 1920,
        "num_images": 1
    }

    try:

        r = requests.post(LEONARDO_URL, headers=headers, json=payload)

        data = r.json()

        generation_id = data["sdGenerationJob"]["generationId"]

        # wait for generation

        time.sleep(5)

        r = requests.get(
            f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}",
            headers=headers
        )

        result = r.json()

        image_url = result["generations_by_pk"]["generated_images"][0]["url"]

        img = requests.get(image_url).content

        local_path = f"/tmp/{image_id}.png"

        with open(local_path, "wb") as f:
            f.write(img)

        return image_url, local_path

    except Exception as e:

        print("IMAGE GENERATION ERROR:", str(e))

        return None, None


# --------------------------------------
# WORKER ENTRY
# --------------------------------------

def process_job(job):

    prompts = job.get("visual_prompts", [])

    images = []

    for prompt in prompts:

        image_id = str(uuid.uuid4())

        print("Generating image for prompt:", prompt)

        image_url, local_path = generate_image(prompt, image_id)

        images.append({

            "id": image_id,
            "prompt": prompt,
            "image_url": image_url,
            "local_path": local_path

        })

    job["images"] = images

    job["status"] = "images_generated"

    return job
