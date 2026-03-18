import random

def process_job(topic):

    hooks = [

        f"Socho agar {topic} reality ban jaye...",

        f"Sach bataun... {topic} India mein possible hai",

        f"2035 tak {topic} common ho sakta hai",

        f"Kya India {topic} ke liye ready hai?"

    ]

    hook = random.choice(hooks)

    script = {

        "topic": topic,

        "hook": hook,

        "trend":
        "India mein technology rapidly evolve ho rahi hai.",

        "insight":
        f"{topic} jaise innovations already research stage mein hain.",

        "future":
        "2060 tak ye system India ke millions logon ki life change kar sakta hai.",

        "question":
        "Aapko kya lagta hai — kya India ready hoga?"

    }

    return script
