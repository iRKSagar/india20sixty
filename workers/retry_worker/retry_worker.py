import time


# ----------------------------------
# CONFIG
# ----------------------------------

MAX_RETRIES = 3
RETRY_DELAY = 3


# ----------------------------------
# RETRY WRAPPER
# ----------------------------------

def retry(task_function, *args, **kwargs):

    attempt = 0

    while attempt < MAX_RETRIES:

        try:

            return task_function(*args, **kwargs)

        except Exception as e:

            attempt += 1

            print(f"Retry attempt {attempt} failed: {e}")

            if attempt >= MAX_RETRIES:

                raise Exception("Max retries reached")

            time.sleep(RETRY_DELAY)
