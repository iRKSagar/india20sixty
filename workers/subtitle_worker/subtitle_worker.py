import uuid
from pathlib import Path


# ----------------------------------
# PATH
# ----------------------------------

SUBTITLE_FOLDER = Path("assets/subtitles")
SUBTITLE_FOLDER.mkdir(parents=True, exist_ok=True)


# ----------------------------------
# KEYWORD EXTRACTION
# ----------------------------------

def extract_keywords(script):

    subtitles = []

    subtitles.append("SOCHO AGAR")
    subtitles.append("AI TREND")
    subtitles.append("SMART TECHNOLOGY")
    subtitles.append("INDIA 2060")
    subtitles.append("INDIA READY?")

    return subtitles


# ----------------------------------
# SRT GENERATOR
# ----------------------------------

def build_srt(subtitles, job_id):

    times = [
        ("00:00:00,000", "00:00:03,000"),
        ("00:00:04,000", "00:00:07,000"),
        ("00:00:08,000", "00:00:12,000"),
        ("00:00:13,000", "00:00:18,000"),
        ("00:00:19,000", "00:00:25,000"),
    ]

    lines = []

    for i, text in enumerate(subtitles):

        start, end = times[i]

        block = f"{i+1}\n{start} --> {end}\n{text}\n"

        lines.append(block)

    srt_content = "\n".join(lines)

    file_path = SUBTITLE_FOLDER / f"{job_id}.srt"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(srt_content)

    return str(file_path)


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):

    job_id = job["job_id"]

    script = job["script"]

    subtitles = extract_keywords(script)

    srt_file = build_srt(subtitles, job_id)

    job["subtitles"] = srt_file

    job["status"] = "subtitles_ready"

    return job


# ----------------------------------
# WORKER LOOP
# ----------------------------------

def run_worker():

    print("Subtitle Worker Started")

    job = {

        "job_id": str(uuid.uuid4()),

        "script": {
            "hook": "Socho agar AI doctors India mein common ho jayein",
            "trend": "AI already hospitals mein scans analyse kar raha hai",
            "insight": "Machines thousands of reports seconds mein process kar sakti hain",
            "future": "2060 tak AI doctors rural India tak healthcare pahucha sakte hain",
            "question": "Kya India ready hai AI healthcare revolution ke liye"
        }
    }

    job = process_job(job)

    print("\nSubtitle file created:")

    print(job["subtitles"])


if __name__ == "__main__":

    run_worker()
