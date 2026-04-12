import modal
import os
import subprocess
import re
from pathlib import Path

# ==========================================
# MODAL APP — RENDERER
# ffmpeg only. No GPT. No network calls.
# Receives images + audio already on disk (or as paths).
# Applies mood presets — colorchannelmixer, eq, unsharp, noise, vignette.
# Returns path to rendered video.
#
# All ffmpeg filters confirmed safe on Modal debian ffmpeg 5.x.
# See Engineering Journal for banned filters.
# ==========================================

app = modal.App("india20sixty-renderer")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg", "fonts-liberation", "fonts-dejavu-core", "fonts-noto")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR    = "/tmp/india20sixty-renderer"
OUT_WIDTH  = 1080
OUT_HEIGHT = 1920
FPS        = 30          # was 25 — smoother motion
XFADE_DUR  = 0.35        # slightly longer for cinematic feel
CRF        = 18          # was 22 — better quality encode
ENCODE_PRESET = "slow"   # was fast — better compression quality

# ==========================================
# MOTIONS — confirmed safe on Modal debian ffmpeg 5.x
# speed: slow | medium | fast — drives how much the frame moves
# ==========================================

MOTIONS = {
    # ── HORIZONTAL PANS ───────────────────────────────────────
    "pan_right_slow":   {"hpct":1.18,"x":lambda dx,dy,n:f"{dx}*n/{n}",            "y":lambda dx,dy,n:f"{dy//2}",              "speed":"slow"},
    "pan_right_med":    {"hpct":1.24,"x":lambda dx,dy,n:f"{dx}*n/{n}",            "y":lambda dx,dy,n:f"{dy//2}",              "speed":"medium"},
    "pan_right_fast":   {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}*n/{n}",            "y":lambda dx,dy,n:f"{dy//3}",              "speed":"fast"},
    "pan_left_slow":    {"hpct":1.18,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}",       "y":lambda dx,dy,n:f"{dy//2}",              "speed":"slow"},
    "pan_left_med":     {"hpct":1.24,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}",       "y":lambda dx,dy,n:f"{dy//2}",              "speed":"medium"},
    "pan_left_fast":    {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}",       "y":lambda dx,dy,n:f"{dy//3}",              "speed":"fast"},

    # ── VERTICAL PANS ─────────────────────────────────────────
    "pan_up_slow":      {"hpct":1.22,"x":lambda dx,dy,n:f"{dx//2}",               "y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}",      "speed":"slow"},
    "pan_up_fast":      {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//2}",               "y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}",      "speed":"fast"},
    "pan_down_slow":    {"hpct":1.22,"x":lambda dx,dy,n:f"{dx//2}",               "y":lambda dx,dy,n:f"{dy}*n/{n}",           "speed":"slow"},
    "pan_down_fast":    {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//2}",               "y":lambda dx,dy,n:f"{dy}*n/{n}",           "speed":"fast"},

    # ── DIAGONAL PANS ─────────────────────────────────────────
    "diagonal_tl_br":   {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}*n/{n}",            "y":lambda dx,dy,n:f"{dy}*n/{n}",           "speed":"medium"},
    "diagonal_tr_bl":   {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}",       "y":lambda dx,dy,n:f"{dy}*n/{n}",           "speed":"medium"},
    "diagonal_bl_tr":   {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}*n/{n}",            "y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}",      "speed":"medium"},
    "diagonal_br_tl":   {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}",       "y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}",      "speed":"medium"},
    "diagonal_tl_br_fast":{"hpct":1.36,"x":lambda dx,dy,n:f"{dx}*n/{n}",          "y":lambda dx,dy,n:f"{dy}*n/{n}",           "speed":"fast"},
    "diagonal_bl_tr_fast":{"hpct":1.36,"x":lambda dx,dy,n:f"{dx}*n/{n}",          "y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}",      "speed":"fast"},

    # ── ZOOM SIMULATIONS ──────────────────────────────────────
    "zoom_in_slow":     {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//2}-{dx//5}*n/{n}", "y":lambda dx,dy,n:f"{dy//2}-{dy//5}*n/{n}","speed":"slow"},
    "zoom_in_med":      {"hpct":1.35,"x":lambda dx,dy,n:f"{dx//2}-{dx//4}*n/{n}", "y":lambda dx,dy,n:f"{dy//2}-{dy//4}*n/{n}","speed":"medium"},
    "zoom_in_fast":     {"hpct":1.42,"x":lambda dx,dy,n:f"{dx//2}-{dx//3}*n/{n}", "y":lambda dx,dy,n:f"{dy//2}-{dy//3}*n/{n}","speed":"fast"},
    "pull_back_slow":   {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//5}+{dx//5}*n/{n}", "y":lambda dx,dy,n:f"{dy//5}+{dy//5}*n/{n}","speed":"slow"},
    "pull_back_med":    {"hpct":1.35,"x":lambda dx,dy,n:f"{dx//4}+{dx//4}*n/{n}", "y":lambda dx,dy,n:f"{dy//4}+{dy//4}*n/{n}","speed":"medium"},
    "pull_back_fast":   {"hpct":1.42,"x":lambda dx,dy,n:f"{dx//3}+{dx//3}*n/{n}", "y":lambda dx,dy,n:f"{dy//3}+{dy//3}*n/{n}","speed":"fast"},

    # ── SPECIALTY ─────────────────────────────────────────────
    "drift_slow":       {"hpct":1.12,"x":lambda dx,dy,n:f"{dx//3}*n/{n}",         "y":lambda dx,dy,n:f"{dy//5}",              "speed":"slow"},
    "drift_up_right":   {"hpct":1.15,"x":lambda dx,dy,n:f"{dx//4}*n/{n}",         "y":lambda dx,dy,n:f"{dy//3}-{dy//4}*n/{n}","speed":"slow"},
    "drift_down_left":  {"hpct":1.15,"x":lambda dx,dy,n:f"{dx//3}-{dx//4}*n/{n}", "y":lambda dx,dy,n:f"{dy//4}*n/{n}",        "speed":"slow"},
    "static_hold":      {"hpct":1.05,"x":lambda dx,dy,n:f"{dx//2}",               "y":lambda dx,dy,n:f"{dy//2}",              "speed":"static"},
    "static_breathe":   {"hpct":1.08,"x":lambda dx,dy,n:f"{dx//2}",               "y":lambda dx,dy,n:f"{dy//2}",              "speed":"static"},
}

# Confirmed-working xfade transitions on Modal debian ffmpeg 5.x
# Grouped by feel
XFADE_TRANSITIONS = {
    "hard":     ["slideleft","slideright","wipeleft","wiperight"],
    "soft":     ["dissolve","fade"],
    "dramatic": ["fadeblack","wiperight","wipeleft"],
    "smooth":   ["dissolve","fade","slideleft"],
}

def _all_xfade():
    seen = set()
    result = []
    for v in XFADE_TRANSITIONS.values():
        for t in v:
            if t not in seen:
                seen.add(t)
                result.append(t)
    return result

# ==========================================
# MOOD PRESETS — redesigned with motion POOLS
# Each mood defines pools — scenes pick randomly, no consecutive repeats
# Energy drives clip duration: high=fast cuts, low=slow lingering
# ==========================================

MOOD_PRESETS = {

    # ── CINEMATIC EPIC ────────────────────────────────────────
    # Space, defence, scale — powerful, high energy, dramatic
    "cinematic_epic": {
        "label": "Cinematic Epic",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.05:rg=0.0:rb=-0.05:gr=0.0:gg=0.95:gb=0.05:br=-0.10:bg=0.03:bb=1.07",
            "eq":      "eq=contrast=1.38:brightness=-0.03:saturation=0.82",
            "sharp":   "unsharp=7:7:1.2:3:3:0.0",
            "noise":   "noise=c0s=18:c0f=t+u",
            "vignette":"vignette=angle=0.6",
        },
        "energy": "high",          # drives pace: high=5-7s clips, medium=7-9s, low=9-12s
        "motion_pools": {
            # Scene 0 — hook: aggressive, fast, grab attention
            0: ["zoom_in_fast","diagonal_bl_tr_fast","diagonal_tl_br_fast","pan_right_fast","pull_back_fast"],
            # Scene 1 — story: dynamic, directional
            1: ["pan_left_fast","diagonal_tr_bl","diagonal_br_tl","zoom_in_med","pan_up_fast"],
            # Scene 2 — resolve: pull back, reveal scale
            2: ["pull_back_slow","pull_back_med","diagonal_bl_tr","pan_right_med","drift_up_right"],
        },
        "transition_pools": {
            0: ["wiperight","slideleft","slideright"],
            1: ["wipeleft","wiperight","slideright"],
            2: ["dissolve","fade"],
        },
        "caption": "box",
    },

    # ── BREAKING NEWS ─────────────────────────────────────────
    # Urgent, fast, choppy — maximum cuts, nervous energy
    "breaking_news": {
        "label": "Breaking News",
        "grade": {
            "ccm":     "colorchannelmixer=rr=0.90:rg=0.05:rb=0.05:gr=0.0:gg=0.95:gb=0.05:br=0.05:bg=0.08:bb=0.87",
            "eq":      "eq=contrast=1.28:brightness=0.0:saturation=0.68",
            "sharp":   "unsharp=5:5:1.1:3:3:0.0",
            "noise":   "noise=c0s=10:c0f=t+u",
            "vignette":"vignette=angle=0.45",
        },
        "energy": "high",
        "motion_pools": {
            0: ["pan_right_fast","diagonal_tl_br_fast","zoom_in_fast","pan_left_fast"],
            1: ["pan_left_fast","diagonal_tr_bl","diagonal_bl_tr_fast","pan_up_fast"],
            2: ["diagonal_tr_bl","static_hold","pan_right_med","zoom_in_med"],
        },
        "transition_pools": {
            0: ["slideleft","wipeleft","wiperight"],
            1: ["wipeleft","slideleft","slideright"],
            2: ["fadeblack","dissolve"],
        },
        "caption": "box",
    },

    # ── HOPEFUL FUTURE ────────────────────────────────────────
    # GreenTech, startups, optimistic — slow, breathing, expansive
    "hopeful_future": {
        "label": "Hopeful Future",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.08:rg=0.05:rb=-0.03:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=-0.02:bb=0.97",
            "eq":      "eq=contrast=1.12:brightness=0.04:saturation=1.35",
            "sharp":   "unsharp=3:3:0.7:3:3:0.0",
            "noise":   "noise=c0s=8:c0f=t+u",
            "vignette":"vignette=angle=0.30",
        },
        "energy": "low",
        "motion_pools": {
            0: ["pan_right_slow","drift_slow","zoom_in_slow","pan_up_slow"],
            1: ["drift_up_right","pan_left_slow","diagonal_bl_tr","pull_back_slow"],
            2: ["pull_back_slow","drift_slow","static_breathe","pan_up_slow"],
        },
        "transition_pools": {
            0: ["dissolve","fade"],
            1: ["fade","dissolve"],
            2: ["dissolve","fade"],
        },
        "caption": "plain",
    },

    # ── DARK SERIOUS ──────────────────────────────────────────
    # Heavy topics — slow, weighty, ominous
    "dark_serious": {
        "label": "Dark Serious",
        "grade": {
            "ccm":     "colorchannelmixer=rr=0.95:rg=0.0:rb=0.05:gr=0.0:gg=0.88:gb=0.12:br=0.08:bg=0.05:bb=0.87",
            "eq":      "eq=contrast=1.45:brightness=-0.06:saturation=0.52",
            "sharp":   "unsharp=7:7:1.0:3:3:0.0",
            "noise":   "noise=c0s=24:c0f=t+u",
            "vignette":"vignette=angle=0.70",
        },
        "energy": "low",
        "motion_pools": {
            0: ["drift_slow","pan_left_slow","static_hold","drift_down_left"],
            1: ["diagonal_tr_bl","static_hold","pan_up_slow","drift_slow"],
            2: ["pull_back_slow","static_breathe","pan_up_slow","drift_down_left"],
        },
        "transition_pools": {
            0: ["fadeblack","dissolve"],
            1: ["dissolve","fade"],
            2: ["fadeblack","fade"],
        },
        "caption": "box",
    },

    # ── COLD TECH ─────────────────────────────────────────────
    # AI, DeepTech — precise, controlled, clinical
    "cold_tech": {
        "label": "Cold Tech",
        "grade": {
            "ccm":     "colorchannelmixer=rr=0.88:rg=0.05:rb=0.07:gr=-0.03:gg=0.95:gb=0.08:br=0.0:bg=0.05:bb=1.15",
            "eq":      "eq=contrast=1.22:brightness=0.0:saturation=0.88",
            "sharp":   "unsharp=5:5:1.0:3:3:0.0",
            "noise":   "noise=c0s=12:c0f=t+u",
            "vignette":"vignette=angle=0.42",
        },
        "energy": "medium",
        "motion_pools": {
            0: ["diagonal_tl_br","zoom_in_med","pan_right_med","diagonal_bl_tr"],
            1: ["pan_left_med","diagonal_br_tl","zoom_in_slow","pan_right_fast"],
            2: ["pull_back_med","drift_slow","static_hold","pan_up_slow"],
        },
        "transition_pools": {
            0: ["slideleft","wipeleft","dissolve"],
            1: ["wipeleft","slideright","dissolve"],
            2: ["dissolve","fade"],
        },
        "caption": "box",
    },

    # ── VIBRANT POP ───────────────────────────────────────────
    # Gadgets, consumer, energetic — fast, punchy, colourful
    "vibrant_pop": {
        "label": "Vibrant Pop",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.05:rg=0.0:rb=0.0:gr=0.05:gg=1.08:gb=0.0:br=0.0:bg=0.0:bb=1.05",
            "eq":      "eq=contrast=1.08:brightness=0.06:saturation=1.72",
            "sharp":   "unsharp=3:3:0.6:3:3:0.0",
            "noise":   "noise=c0s=6:c0f=t+u",
            "vignette":"vignette=angle=0.22",
        },
        "energy": "high",
        "motion_pools": {
            0: ["diagonal_tl_br_fast","pan_right_fast","zoom_in_fast","diagonal_bl_tr_fast"],
            1: ["pan_left_fast","zoom_in_med","diagonal_tr_bl","diagonal_br_tl"],
            2: ["diagonal_bl_tr","zoom_in_med","pan_up_fast","pan_right_med"],
        },
        "transition_pools": {
            0: ["wiperight","slideright","slideleft"],
            1: ["slideright","wiperight","wipeleft"],
            2: ["dissolve","wiperight"],
        },
        "caption": "box",
    },

    # ── NOSTALGIC FILM ────────────────────────────────────────
    # Heritage, history, emotion — slow, filmic, warm
    "nostalgic_film": {
        "label": "Nostalgic Film",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.12:rg=0.05:rb=-0.08:gr=0.05:gg=1.0:gb=-0.05:br=-0.03:bg=0.0:bb=0.93",
            "eq":      "eq=contrast=1.18:brightness=0.03:saturation=1.12",
            "sharp":   "unsharp=3:3:0.5:3:3:0.0",
            "noise":   "noise=c0s=26:c0f=t+u",
            "vignette":"vignette=angle=0.65",
        },
        "energy": "low",
        "motion_pools": {
            0: ["pan_right_slow","drift_slow","zoom_in_slow","drift_up_right"],
            1: ["diagonal_bl_tr","pan_up_slow","drift_down_left","pan_left_slow"],
            2: ["zoom_in_slow","static_breathe","pull_back_slow","drift_slow"],
        },
        "transition_pools": {
            0: ["dissolve","fade"],
            1: ["fade","dissolve"],
            2: ["dissolve","fadeblack"],
        },
        "caption": "plain",
    },

    # ── WARM HUMAN ────────────────────────────────────────────
    # Healthcare, education, community — gentle, intimate
    "warm_human": {
        "label": "Warm Human",
        "grade": {
            "ccm":     "colorchannelmixer=rr=1.10:rg=0.05:rb=-0.05:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=0.0:bb=0.95",
            "eq":      "eq=contrast=1.10:brightness=0.05:saturation=1.32",
            "sharp":   "unsharp=3:3:0.5:3:3:0.0",
            "noise":   "noise=c0s=8:c0f=t+u",
            "vignette":"vignette=angle=0.28",
        },
        "energy": "low",
        "motion_pools": {
            0: ["pan_right_slow","drift_slow","zoom_in_slow","drift_up_right"],
            1: ["drift_slow","pan_up_slow","drift_down_left","zoom_in_slow"],
            2: ["static_breathe","pull_back_slow","drift_slow","pan_up_slow"],
        },
        "transition_pools": {
            0: ["dissolve","fade"],
            1: ["fade","dissolve"],
            2: ["dissolve","fade"],
        },
        "caption": "plain",
    },
}

CLUSTER_MOOD_DEFAULTS = {
    "Space":    "cinematic_epic",
    "DeepTech": "cold_tech",
    "AI":       "cold_tech",
    "Gadgets":  "vibrant_pop",
    "GreenTech":"hopeful_future",
    "Startups": "hopeful_future",
}

# Energy → clip duration range in seconds (for 25s video / 3 scenes)
ENERGY_PACE = {
    "high":   (5.5, 7.5),   # fast cuts — high energy
    "medium": (7.0, 9.0),   # balanced
    "low":    (8.5, 11.0),  # slow lingering
}



# ==========================================
# BACKGROUND MUSIC — mood-mapped ambient tracks
# Stored in R2 at music/track_name.mp3
# Mixed at 8% volume under voice in final mux
# ==========================================

MUSIC_TRACKS = {
    "cinematic_epic":  "cinematic_rise.mp3",
    "breaking_news":   "urgent_pulse.mp3",
    "hopeful_future":  "ambient_hope.mp3",
    "cold_tech":       "minimal_tech.mp3",
    "vibrant_pop":     "upbeat_energy.mp3",
    "nostalgic_film":  "soft_acoustic.mp3",
    "warm_human":      "gentle_piano.mp3",
    "dark_serious":    "low_drone.mp3",
}

MUSIC_VOLUME = 0.08   # 8% — audible but never competes with voice

def _fetch_music(mood: str, job_id: str) -> str | None:
    """Download mood-matched music track from R2. Returns local path or None."""
    import requests as _req
    r2_base = os.environ.get("R2_BASE_URL", "").rstrip("/")
    if not r2_base:
        print("  Music: R2_BASE_URL not set — skipping")
        return None
    track = MUSIC_TRACKS.get(mood, "ambient_hope.mp3")
    url   = f"{r2_base}/music/{track}"
    path  = f"{TMP_DIR}/{job_id}_music.mp3"
    try:
        r = _req.get(url, timeout=15, stream=True)
        if not r.ok:
            print(f"  Music: {track} not found on R2 ({r.status_code}) — skipping")
            return None
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192): f.write(chunk)
        print(f"  Music: downloaded {track} ({os.path.getsize(path)//1024}KB)")
        return path
    except Exception as e:
        print(f"  Music: download failed ({e}) — skipping")
        return None


# ==========================================

def _build_word_captions(script, audio_dur):
    import re
    if not script or not script.strip(): return []
    script = re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>", "", script).strip()
    # Remove em dashes and ellipsis — don't show in captions
    script = script.replace('—', ' ').replace('...', ' ')
    script = re.sub(r'\s+', ' ', script).strip()
    words = script.split()
    if not words: return []

    # Group into short phrases — max 4 words OR break on punctuation
    # Short phrases fit on one line at font size 48 on 1080px width
    phrases, chunk = [], []
    for w in words:
        chunk.append(w)
        ends_phrase = w.endswith(('.','?','!',',',':'))
        if len(chunk) >= 4 or ends_phrase:
            phrase = ' '.join(chunk).strip(' ,')
            if phrase: phrases.append(phrase)
            chunk = []
    if chunk:
        phrase = ' '.join(chunk).strip(' ,')
        if phrase: phrases.append(phrase)

    if not phrases: return []

    total_words = sum(len(p.split()) for p in phrases)
    captions, t = [], 0.15  # tiny delay before first caption
    for phrase in phrases:
        # Each phrase shown for time proportional to its word count
        dur = (len(phrase.split()) / total_words) * (audio_dur - 0.4)
        dur = max(dur, 0.5)  # minimum 0.5s per phrase
        captions.append((phrase, round(t, 3), round(t + dur, 3)))
        t += dur

    return captions

def _make_subtitle_filters(captions, cap_y, cap_size, style='box'):
    parts = []
    bw = 14 if style == 'box' else 5
    bc = 'black@0.78' if style == 'box' else 'black@0.85'
    for text, t_s, t_e in captions:
        if not text.strip(): continue
        escaped = _escape_dt(text)
        parts.append(
            f"drawtext=text='{escaped}':fontsize={cap_size}:fontcolor=white"
            f":borderw={bw}:bordercolor={bc}"
            f":x=(w-text_w)/2:y={cap_y}"
            f":enable='between(t,{t_s:.3f},{t_e:.3f})'"
        )
    return parts

def _make_end_card_filters(question, audio_dur, start_offset=3.0):
    if not question or not question.strip(): return []
    import re as _re
    t_s = max(0, audio_dur - start_offset)
    t_e = audio_dur - 0.2
    q = _re.sub(r"</?[^>]+>", "", question).strip().rstrip('?').strip() + '?'
    q = q.replace('—', '-').replace('...', '')
    words = q.split()
    if not words: return []
    mid = max(1, len(words) // 2)
    line1 = _escape_dt(' '.join(words[:mid]))
    line2 = _escape_dt(' '.join(words[mid:])) if words[mid:] else None
    # Lower third — above YouTube channel name UI
    y1 = int(OUT_HEIGHT * 0.72)
    y2 = y1 + 68
    filters = [
        f"drawtext=text='{line1}':fontsize=50:fontcolor=white:borderw=14:bordercolor=black@0.85:x=(w-text_w)/2:y={y1}:enable='between(t,{t_s:.3f},{t_e:.3f})'",
    ]
    if line2:
        filters.append(f"drawtext=text='{line2}':fontsize=50:fontcolor=white:borderw=14:bordercolor=black@0.85:x=(w-text_w)/2:y={y2}:enable='between(t,{t_s:.3f},{t_e:.3f})'")
    return filters

def _pick_motion(pool_key: int, preset: dict, used: set) -> str:
    """Pick a motion from the pool, avoiding recently used ones."""
    import random
    pool = preset["motion_pools"].get(pool_key, list(MOTIONS.keys()))
    available = [m for m in pool if m not in used]
    if not available:
        available = pool  # reset if all used
    choice = random.choice(available)
    used.add(choice)
    return choice

def _pick_transition(scene_idx: int, preset: dict) -> str:
    """Pick transition for this scene from the mood's transition pool."""
    import random
    pool = preset["transition_pools"].get(scene_idx, ["dissolve","fade"])
    return random.choice(pool)


# ==========================================
# RENDER WITH AUDIO (AI Voice mode)
# ==========================================

@app.function(image=image, secrets=secrets, cpu=4.0, memory=4096, timeout=360)
def render_with_audio(
    job_id: str,
    image_paths: list,
    audio_path: str,
    audio_dur: float,
    captions: list,
    mood: str,
    image_bytes_list: list = None,
    audio_bytes: bytes = None,
    script: str = "",           # full script text for word-synced captions
    end_question: str = "",     # debate question for end card
) -> str:
    """
    Render 3 images + audio into a final MP4.
    Accepts bytes directly — no cross-container /tmp/ sharing needed.
    Returns path to rendered video.
    """
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    video_path = f"{TMP_DIR}/{job_id}.mp4"

    # Write image bytes to THIS container's /tmp/
    resolved_paths = []
    for i in range(len(image_paths)):
        p = f"{TMP_DIR}/{job_id}_{i}.png"
        img_bytes = (image_bytes_list[i] if image_bytes_list and i < len(image_bytes_list) else None)
        if img_bytes:
            with open(p, "wb") as f: f.write(img_bytes)
            resolved_paths.append(p)
        else:
            # Black frame fallback
            black = f"{TMP_DIR}/{job_id}_{i}_black.png"
            subprocess.run([
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", "color=c=0x0d1117:s=864x1536:d=1",
                "-frames:v", "1", black
            ], capture_output=True, timeout=15)
            resolved_paths.append(black)
            print(f"  Scene {i}: black frame fallback")
    image_paths = resolved_paths

    # Write audio bytes to THIS container's /tmp/
    if audio_bytes:
        audio_path = f"{TMP_DIR}/{job_id}.mp3"
        with open(audio_path, "wb") as f: f.write(audio_bytes)

    scene_dur = audio_dur / len(image_paths)

    print(f"\n[Render] job={job_id} mood={mood} dur={audio_dur:.1f}s")
    print(f"  {len(image_paths)} scenes x {scene_dur:.1f}s")

    import random
    preset = MOOD_PRESETS.get(mood, MOOD_PRESETS.get("hopeful_future"))
    used_motions = set()

    # Build word-synced captions from script — strip emotion tags first
    import re as _re
    clean_script = _re.sub(r"</?(?:excited|happy|sad|whisper|angry)[^>]*>", "", script).strip() if script else ""
    word_captions = _build_word_captions(clean_script, audio_dur) if clean_script else []
    print(f"  Word captions: {len(word_captions)} phrases")

    # Build end card filters from debate question
    end_card_filters = _make_end_card_filters(end_question, audio_dur) if end_question else []
    if end_card_filters:
        print(f"  End card: last 3s — '{end_question[:50]}...'")

    # Pre-select varied motions — no consecutive repeats across scenes
    scene_motions = []
    for i in range(len(image_paths)):
        m_a = _pick_motion(i, preset, used_motions)
        used_motions.add(m_a)
        m_b = _pick_motion(i, preset, used_motions.copy())
        used_motions.add(m_b)
        scene_motions.append((m_a, m_b))

    print(f"  Motions: {' | '.join(a+'+'+b for a,b in scene_motions)}")

    # Assign captions per scene based on time windows
    scene_word_caps = [[], [], []]
    for phrase, t_s, t_e in word_captions:
        scene_idx = min(int(t_s / scene_dur), 2)
        # Offset time within scene
        off_s = t_s - scene_idx * scene_dur
        off_e = t_e - scene_idx * scene_dur
        scene_word_caps[scene_idx].append((phrase, max(0, off_s), max(0, off_e)))

    # Extract hook text from first sentence — burned onto scene 0 as thumbnail text
    import re as _re2
    hook_text = ""
    if script:
        clean = _re2.sub(r"</?[^>]+>", "", script).strip()
        sentences = _re2.split(r'(?<=[.!?])\s+', clean)
        if sentences:
            hook_text = sentences[0].strip()[:60]

    clip_paths = []
    for i, img in enumerate(image_paths):
        m_a, m_b = scene_motions[i]
        clip = _render_scene_clip(
            job_id, img, scene_dur, i, captions, mood,
            motion_override=m_a, motion_b_override=m_b,
            word_captions=scene_word_caps[i],
            is_last=(i == len(image_paths) - 1),
            end_card_filters=end_card_filters if i == len(image_paths) - 1 else [],
            hook_text=hook_text if i == 0 else "",
        )
        clip_paths.append(clip)

    transitioned = _apply_xfade(job_id, clip_paths, scene_dur, mood)

    for cp in clip_paths:
        try: os.remove(cp)
        except Exception: pass

    # Download mood-matched music track
    music_path = _fetch_music(mood, job_id)

    fade_st = audio_dur - 0.5
    try:
        if music_path and os.path.exists(music_path):
            # Three-stream mux: video + voice + music
            # Music looped to fill duration, mixed at MUSIC_VOLUME under voice
            _run_ffmpeg([
                "ffmpeg", "-y",
                "-i", transitioned,          # 0: video
                "-i", audio_path,            # 1: voice
                "-stream_loop", "-1",
                "-i", music_path,            # 2: music (looped)
                "-filter_complex",
                (f"[1:a]volume=1.0[voice];"
                 f"[2:a]volume={MUSIC_VOLUME}[music];"
                 f"[voice][music]amix=inputs=2:duration=first:dropout_transition=2[audio];"
                 f"[0:v]fade=t=in:st=0:d=0.3,fade=t=out:st={fade_st:.2f}:d=0.5[video]"),
                "-map", "[video]", "-map", "[audio]",
                "-c:v", "libx264", "-preset", ENCODE_PRESET, "-crf", str(CRF),
                "-c:a", "aac", "-b:a", "192k",
                "-t", str(audio_dur),
                "-movflags", "+faststart", video_path
            ], "final-mux-music")
            try: os.remove(music_path)
            except Exception: pass
        else:
            # No music — voice only
            _run_ffmpeg([
                "ffmpeg", "-y",
                "-i", transitioned, "-i", audio_path,
                "-vf", f"fade=t=in:st=0:d=0.3,fade=t=out:st={fade_st:.2f}:d=0.5",
                "-map", "0:v", "-map", "1:a",
                "-c:v", "libx264", "-preset", ENCODE_PRESET, "-crf", str(CRF),
                "-c:a", "aac", "-b:a", "192k",
                "-t", str(audio_dur),
                "-movflags", "+faststart", video_path
            ], "final-mux")
    except Exception as e:
        print(f"  Music mux failed ({e}), falling back to plain mux")
        _run_ffmpeg([
            "ffmpeg", "-y",
            "-i", transitioned, "-i", audio_path,
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", str(CRF),
            "-c:a", "aac", "-b:a", "192k",
            "-t", str(audio_dur),
            "-movflags", "+faststart", video_path
        ], "final-mux-plain")

    try: os.remove(transitioned)
    except Exception: pass

    size = os.path.getsize(video_path)
    if size < 100_000:
        raise Exception(f"Rendered video too small: {size} bytes")
    print(f"  Final: {size // 1024}KB → {video_path}")

    # Return bytes — renderer and pipeline/publisher run in different containers
    with open(video_path, "rb") as f:
        video_bytes = f.read()
    return video_bytes


# ==========================================
# RENDER SILENT (Human Voice mode)
# ==========================================

@app.function(image=image, secrets=secrets, cpu=4.0, memory=4096, timeout=360)
def render_silent(
    job_id: str,
    image_paths: list,
    captions: list,
    mood: str,
    image_bytes_list: list = None,
) -> str:
    """
    Render 3 images into a silent MP4 (no audio track).
    Used in Human Voice mode — audio is added later by mixer.py.
    Returns path to silent video.
    """
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    video_path = f"{TMP_DIR}/{job_id}_silent.mp4"
    scene_dur  = 8.6

    print(f"\n[Render Silent] job={job_id} mood={mood}")

    # Write image bytes to THIS container's /tmp/
    resolved_paths = []
    for i in range(len(image_paths)):
        p = f"{TMP_DIR}/{job_id}_{i}.png"
        img_bytes = (image_bytes_list[i] if image_bytes_list and i < len(image_bytes_list) else None)
        if img_bytes:
            with open(p, "wb") as f: f.write(img_bytes)
            resolved_paths.append(p)
        else:
            black = f"{TMP_DIR}/{job_id}_{i}_black.png"
            subprocess.run(["ffmpeg", "-y", "-f", "lavfi",
                "-i", "color=c=0x0d1117:s=864x1536:d=1",
                "-frames:v", "1", black], capture_output=True, timeout=15)
            resolved_paths.append(black)
    image_paths = resolved_paths

    clip_paths = []
    for i, img in enumerate(image_paths):
        clip = _render_scene_clip(job_id, img, scene_dur, i, captions, mood)
        clip_paths.append(clip)

    transitioned = _apply_xfade(job_id, clip_paths, scene_dur, mood)

    for cp in clip_paths:
        try: os.remove(cp)
        except Exception: pass

    _run_ffmpeg([
        "ffmpeg", "-y", "-i", transitioned,
        "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-an",
        "-movflags", "+faststart", video_path
    ], "silent-render")

    try: os.remove(transitioned)
    except Exception: pass

    size = os.path.getsize(video_path)
    print(f"  Silent: {size // 1024}KB → {video_path}")
    with open(video_path, "rb") as f:
        return f.read()


# ==========================================
# SCENE CLIP RENDERING
# ==========================================

def _render_scene_clip(
    job_id: str, img_path: str, duration: float,
    scene_idx: int, captions: list, mood: str,
    motion_override: str = None,
    motion_b_override: str = None,
    transition_override: str = None,
    word_captions: list = None,
    is_last: bool = False,
    end_card_filters: list = None,
    hook_text: str = "",       # thumbnail text burned into scene 0 only
) -> str:
    """
    Render one image as TWO sub-clips with a hard cut between them.
    Motions are picked from mood pools — no two consecutive clips repeat.
    """
    import random
    clip_path = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
    pre_path  = f"{TMP_DIR}/{job_id}_pre{scene_idx}.jpg"
    cap_y     = int(OUT_HEIGHT * 0.68)   # 68% — above YouTube's channel name overlay at bottom
    cap_size  = 48                        # smaller — prevents long phrases overflowing screen width
    wm        = _escape_dt("@India20Sixty")

    preset = MOOD_PRESETS.get(mood, MOOD_PRESETS.get("hopeful_future"))
    if not preset:
        preset = list(MOOD_PRESETS.values())[0]
    grade = preset["grade"]
    caption_style = preset.get("caption", "plain")

    # Pick motions from pool (or use overrides from render_with_audio)
    motion_a_name = motion_override or _pick_motion(scene_idx, preset, set())
    motion_b_name = motion_b_override or _pick_motion(scene_idx, preset, {motion_a_name})

    motion_a = MOTIONS.get(motion_a_name, MOTIONS["pan_right_slow"])
    motion_b = MOTIONS.get(motion_b_name, MOTIONS["diagonal_bl_tr"])

    print(f"  Clip {scene_idx}: [{mood}] {motion_a_name} → {motion_b_name}")

    hpct  = max(motion_a["hpct"], motion_b["hpct"])
    pan_w = int(OUT_WIDTH  * hpct)
    pan_h = int(OUT_HEIGHT * hpct)
    dx    = pan_w - OUT_WIDTH
    dy    = pan_h - OUT_HEIGHT

    # PASS 1: Pre-process to scaled JPEG
    _run_ffmpeg([
        "ffmpeg", "-y", "-i", img_path,
        "-vf", (f"scale={pan_w}:{pan_h}:force_original_aspect_ratio=increase:flags=lanczos,"
                f"crop={pan_w}:{pan_h}"),
        "-frames:v", "1", "-q:v", "3", "-f", "image2", "-vcodec", "mjpeg", pre_path
    ], f"pre-{scene_idx}", timeout=20)

    # Split: sub-clip A = 42%, sub-clip B = 58%
    dur_a = duration * 0.42
    dur_b = duration * 0.58
    n_a   = int(dur_a * FPS)
    n_b   = int(dur_b * FPS)

    # Motion speed scaling based on motion's own speed tag
    speed_map = {"fast": 1.0, "medium": 0.72, "slow": 0.45, "static": 0.0}
    speed_a = speed_map.get(motion_a.get("speed","medium"), 0.72)
    speed_b = speed_map.get(motion_b.get("speed","medium"), 0.72)

    sdx_a = max(1, min(int(dx * speed_a), dx))
    sdy_a = max(1, min(int(dy * speed_a), dy))
    sdx_b = max(1, min(int(dx * speed_b), dx))
    sdy_b = max(1, min(int(dy * speed_b), dy))

    x_a = motion_a["x"](sdx_a, sdy_a, n_a)
    y_a = motion_a["y"](sdx_a, sdy_a, n_a)
    x_b = motion_b["x"](sdx_b, sdy_b, n_b)
    y_b = motion_b["y"](sdx_b, sdy_b, n_b)

    def make_vf(x_expr, y_expr, sub_offset, sub_dur):
        """
        sub_offset: global time offset this sub-clip starts at (seconds)
        sub_dur: duration of this sub-clip
        """
        sub_end = sub_offset + sub_dur
        parts = [
            f"crop={OUT_WIDTH}:{OUT_HEIGHT}:{x_expr}:{y_expr}",
            grade["ccm"],
            grade["eq"],
            grade["sharp"],
            grade["noise"],
            grade["vignette"],
            "setsar=1",
            # Watermark — top left, clear of YouTube UI
            f"drawtext=text='\u25B6 India20Sixty':fontsize=32:fontcolor=white@0.80"
            f":borderw=2:bordercolor=black@0.85:x=24:y=120",
        ]

        # Hook frame thumbnail text — scene 0 only, top of frame
        if scene_idx == 0 and hook_text:
            escaped_hook = _escape_dt(hook_text[:60])
            parts.append(
                f"drawtext=text='{escaped_hook}':fontsize=44:fontcolor=white"
                f":borderw=12:bordercolor=black@0.88"
                f":x=(w-text_w)/2:y=220"
            )

        # Word-synced captions — only show phrases that fall in this sub-clip's time window
        # Times in word_captions are GLOBAL (0 to audio_dur)
        # ffmpeg t resets to 0 at start of each sub-clip, so offset the enable window
        if word_captions:
            for text, t_s, t_e in word_captions:
                # Only include if this phrase overlaps with this sub-clip's global window
                if t_e <= sub_offset or t_s >= sub_end:
                    continue
                # Convert global time to local sub-clip time
                local_s = max(0.0, t_s - sub_offset)
                local_e = min(sub_dur, t_e - sub_offset)
                if local_e <= local_s:
                    continue
                escaped = _escape_dt(text)
                bw = 14 if caption_style == 'box' else 5
                bc = 'black@0.78' if caption_style == 'box' else 'black@0.85'
                # Cap font size and enforce max width to prevent overflow
                safe_size = min(cap_size, 52)
                parts.append(
                    f"drawtext=text='{escaped}':fontsize={safe_size}:fontcolor=white"
                    f":borderw={bw}:bordercolor={bc}"
                    f":x=(w-text_w)/2:y={cap_y}"
                    f":enable='between(t,{local_s:.3f},{local_e:.3f})'"
                )

        # End card on last scene's last sub-clip
        if is_last and end_card_filters and sub_offset > 0:
            parts.extend(end_card_filters)

        return ",".join(parts)

    scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
    while len(scene_caps) < 3:
        scene_caps.append("")

    sub_a = f"{TMP_DIR}/{job_id}_clip{scene_idx}a.mp4"
    sub_b = f"{TMP_DIR}/{job_id}_clip{scene_idx}b.mp4"
    lst   = f"{TMP_DIR}/{job_id}_list{scene_idx}.txt"

    _run_ffmpeg([
        "ffmpeg", "-y", "-loop", "1", "-r", str(FPS), "-i", pre_path,
        "-vf", make_vf(x_a, y_a, 0.0, dur_a),
        "-t", str(dur_a), "-r", str(FPS),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p", sub_a
    ], f"clip-{scene_idx}a", timeout=180)

    _run_ffmpeg([
        "ffmpeg", "-y", "-loop", "1", "-r", str(FPS), "-i", pre_path,
        "-vf", make_vf(x_b, y_b, dur_a, dur_b),
        "-t", str(dur_b), "-r", str(FPS),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p", sub_b
    ], f"clip-{scene_idx}b", timeout=180)

    with open(lst, "w") as f:
        f.write(f"file '{sub_a}'\nfile '{sub_b}'\n")

    _run_ffmpeg([
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", lst, "-c", "copy", clip_path
    ], f"concat-{scene_idx}", timeout=60)

    for p in [sub_a, sub_b, lst, pre_path]:
        try: os.remove(p)
        except Exception: pass
    try: os.remove(img_path)
    except Exception: pass

    print(f"  Clip {scene_idx}: {os.path.getsize(clip_path) // 1024}KB")
    return clip_path


def _apply_xfade(job_id: str, clip_paths: list, scene_dur: float, mood: str) -> str:
    import random
    if len(clip_paths) == 1:
        return clip_paths[0]

    output_path = f"{TMP_DIR}/{job_id}_xfaded.mp4"
    n           = len(clip_paths)
    inputs      = []
    for cp in clip_paths:
        inputs += ["-i", cp]

    preset = MOOD_PRESETS.get(mood, list(MOOD_PRESETS.values())[0])

    def get_transition(idx: int) -> str:
        # Use new transition_pools if available
        if "transition_pools" in preset:
            pool = preset["transition_pools"].get(idx, ["dissolve","fade"])
            return random.choice(pool)
        # Legacy fallback
        all_t = _all_xfade()
        return random.choice(all_t)

    fc_parts = []
    offset   = scene_dur - XFADE_DUR
    t0       = get_transition(0)
    fc_parts.append(
        f"[0:v][1:v]xfade=transition={t0}:duration={XFADE_DUR}:offset={offset:.3f}[xf0]"
    )
    for i in range(2, n):
        offset += scene_dur - XFADE_DUR
        t_i    = get_transition(i - 1)
        prev   = f"[xf{i-2}]" if i > 2 else "[xf0]"
        fc_parts.append(
            f"{prev}[{i}:v]xfade=transition={t_i}:duration={XFADE_DUR}:offset={offset:.3f}[xf{i-1}]"
        )

    transitions_used = [get_transition(i) for i in range(n - 1)]
    sep = ' \u2192 '
    print(f"  xfade: {sep.join(transitions_used)}")

    try:
        _run_ffmpeg([
            "ffmpeg", "-y", *inputs,
            "-filter_complex", ";".join(fc_parts),
            "-map", f"[xf{n-2}]",
            "-c:v", "libx264", "-preset", "fast", "-crf", "21",
            "-pix_fmt", "yuv420p", output_path
        ], "xfade", timeout=120)
        print(f"  xfaded: {os.path.getsize(output_path) // 1024}KB")
        return output_path
    except Exception as e:
        print(f"  xfade failed ({e}), fallback to concat")
        list_path   = f"{TMP_DIR}/{job_id}_list.txt"
        concat_path = f"{TMP_DIR}/{job_id}_concat.mp4"
        with open(list_path, "w") as f:
            for cp in clip_paths:
                f.write(f"file '{cp}'\n")
        _run_ffmpeg([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", list_path, "-c", "copy", concat_path
        ], "concat", timeout=60)
        try: os.remove(list_path)
        except Exception: pass
        return concat_path


# ==========================================
# UTILITIES
# ==========================================

def _run_ffmpeg(cmd: list, label: str, timeout: int = 300):
    """
    Run an ffmpeg command. Raises with full stderr on failure.
    RULE: Always print full stderr on failure — dashboard truncates to 120 chars.
    """
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print(f"  ffmpeg [{label}] FAILED:")
        print(result.stderr[-600:])  # full stderr — critical for debugging
        raise Exception(f"{label}: {result.stderr[-150:]}")
    return result


def _escape_dt(text: str) -> str:
    """Escape text for ffmpeg drawtext filter."""
    text = text.replace("\\", "\\\\")
    text = text.replace("'", "\u2019")   # unicode apostrophe — NOT backslash-quote
    text = text.replace(":", "\\:")
    text = text.replace("%", "\\%")
    return text


@app.local_entrypoint()
def main():
    print("renderer.py — test render_silent with dummy images")
    import subprocess as sp
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    # Create 3 solid-colour test images
    test_imgs = []
    for i, col in enumerate(["0x1a237e", "0x004d40", "0x311b92"]):
        p = f"{TMP_DIR}/test_{i}.png"
        sp.run([
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", f"color=c={col}:s=864x1536:d=1",
            "-frames:v", "1", p
        ], capture_output=True)
        test_imgs.append(p)
    result = render_silent.remote(
        job_id="test-render-001",
        image_paths=test_imgs,
        captions=["INDIA RISING", "SPACE NATION", "NEW ERA", "ISRO WINS",
                  "FUTURE NOW", "WE DID IT", "PROUD MOMENT", "WORLD WATCHING", "NEXT?"],
        mood="cinematic_epic",
    )
    print("Output:", result)
