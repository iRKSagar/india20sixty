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
FPS        = 25
XFADE_DUR  = 0.3

# ==========================================
# MOTIONS — confirmed safe on Modal debian ffmpeg 5.x
# ==========================================

MOTIONS = {
    "pan_right_slow":  {"hpct": 1.20, "x": lambda dx,dy,n: f"{dx}*n/{n}",             "y": lambda dx,dy,n: f"{dy//2}"},
    "pan_right_fast":  {"hpct": 1.28, "x": lambda dx,dy,n: f"{dx}*n/{n}",             "y": lambda dx,dy,n: f"{dy//3}"},
    "pan_left_slow":   {"hpct": 1.20, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",        "y": lambda dx,dy,n: f"{dy//2}"},
    "pan_left_fast":   {"hpct": 1.28, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",        "y": lambda dx,dy,n: f"{dy//3}"},
    "pan_up":          {"hpct": 1.22, "x": lambda dx,dy,n: f"{dx//2}",                 "y": lambda dx,dy,n: f"{dy}-{dy}*n/{n}"},
    "pan_down":        {"hpct": 1.22, "x": lambda dx,dy,n: f"{dx//2}",                 "y": lambda dx,dy,n: f"{dy}*n/{n}"},
    "diagonal_tl_br":  {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}*n/{n}",              "y": lambda dx,dy,n: f"{dy}*n/{n}"},
    "diagonal_tr_bl":  {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",         "y": lambda dx,dy,n: f"{dy}*n/{n}"},
    "diagonal_bl_tr":  {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}*n/{n}",              "y": lambda dx,dy,n: f"{dy}-{dy}*n/{n}"},
    "diagonal_br_tl":  {"hpct": 1.30, "x": lambda dx,dy,n: f"{dx}-{dx}*n/{n}",         "y": lambda dx,dy,n: f"{dy}-{dy}*n/{n}"},
    "zoom_in_sim":     {"hpct": 1.35, "x": lambda dx,dy,n: f"{dx//2}-{dx//4}*n/{n}",   "y": lambda dx,dy,n: f"{dy//2}-{dy//4}*n/{n}"},
    "pull_back_sim":   {"hpct": 1.35, "x": lambda dx,dy,n: f"{dx//4}+{dx//4}*n/{n}",   "y": lambda dx,dy,n: f"{dy//4}+{dy//4}*n/{n}"},
    "drift_slow":      {"hpct": 1.12, "x": lambda dx,dy,n: f"{dx//3}*n/{n}",            "y": lambda dx,dy,n: f"{dy//4}"},
    "static_hold":     {"hpct": 1.08, "x": lambda dx,dy,n: f"{dx//2}",                  "y": lambda dx,dy,n: f"{dy//2}"},
}

# Confirmed-working xfade transitions on Modal debian ffmpeg 5.x
XFADE_TRANSITIONS = [
    "dissolve", "wipeleft", "wiperight",
    "slideleft", "slideright", "fade", "fadeblack",
]

# Import mood presets from scriptwriter (single source of truth)
# At deploy time Modal resolves this — both files are in the same workspace.
# If running standalone for testing, define a minimal fallback.
# MOOD_PRESETS defined inline — do NOT import from scriptwriter.
# Cross-app imports cause Modal to find two app objects and fail at deploy time.
MOOD_PRESETS = {
    "cinematic_epic":  {"label":"Cinematic Epic",  "grade":{"ccm":"colorchannelmixer=rr=1.05:rg=0.0:rb=-0.05:gr=0.0:gg=0.95:gb=0.05:br=-0.10:bg=0.03:bb=1.07","eq":"eq=contrast=1.38:brightness=-0.03:saturation=0.82","sharp":"unsharp=7:7:1.2:3:3:0.0","noise":"noise=c0s=18:c0f=t+u","vignette":"vignette=angle=0.6"},"scenes":[{"motion_a":"diagonal_bl_tr","motion_b":"zoom_in_sim","transition":"wiperight","energy":"high","caption":"box"},{"motion_a":"pan_right_fast","motion_b":"pan_up","transition":"slideright","energy":"high","caption":"box"},{"motion_a":"diagonal_br_tl","motion_b":"pull_back_sim","transition":"dissolve","energy":"medium","caption":"plain"}]},
    "breaking_news":   {"label":"Breaking News",   "grade":{"ccm":"colorchannelmixer=rr=0.90:rg=0.05:rb=0.05:gr=0.0:gg=0.95:gb=0.05:br=0.05:bg=0.08:bb=0.87","eq":"eq=contrast=1.28:brightness=0.0:saturation=0.68","sharp":"unsharp=5:5:1.1:3:3:0.0","noise":"noise=c0s=10:c0f=t+u","vignette":"vignette=angle=0.45"},"scenes":[{"motion_a":"pan_right_fast","motion_b":"diagonal_tl_br","transition":"slideleft","energy":"high","caption":"box"},{"motion_a":"pan_left_fast","motion_b":"pan_up","transition":"wipeleft","energy":"high","caption":"box"},{"motion_a":"diagonal_tr_bl","motion_b":"static_hold","transition":"fadeblack","energy":"medium","caption":"plain"}]},
    "hopeful_future":  {"label":"Hopeful Future",  "grade":{"ccm":"colorchannelmixer=rr=1.08:rg=0.05:rb=-0.03:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=-0.02:bb=0.97","eq":"eq=contrast=1.12:brightness=0.04:saturation=1.45","sharp":"unsharp=3:3:0.7:3:3:0.0","noise":"noise=c0s=8:c0f=t+u","vignette":"vignette=angle=0.30"},"scenes":[{"motion_a":"pan_right_slow","motion_b":"zoom_in_sim","transition":"dissolve","energy":"medium","caption":"plain"},{"motion_a":"diagonal_bl_tr","motion_b":"pan_up","transition":"fade","energy":"medium","caption":"plain"},{"motion_a":"drift_slow","motion_b":"pull_back_sim","transition":"dissolve","energy":"low","caption":"plain"}]},
    "dark_serious":    {"label":"Dark Serious",    "grade":{"ccm":"colorchannelmixer=rr=0.95:rg=0.0:rb=0.05:gr=0.0:gg=0.88:gb=0.12:br=0.08:bg=0.05:bb=0.87","eq":"eq=contrast=1.45:brightness=-0.06:saturation=0.52","sharp":"unsharp=7:7:1.0:3:3:0.0","noise":"noise=c0s=24:c0f=t+u","vignette":"vignette=angle=0.70"},"scenes":[{"motion_a":"drift_slow","motion_b":"pan_left_slow","transition":"fadeblack","energy":"low","caption":"box"},{"motion_a":"diagonal_tr_bl","motion_b":"static_hold","transition":"dissolve","energy":"low","caption":"box"},{"motion_a":"pan_up","motion_b":"pull_back_sim","transition":"fade","energy":"low","caption":"plain"}]},
    "cold_tech":       {"label":"Cold Tech",       "grade":{"ccm":"colorchannelmixer=rr=0.88:rg=0.05:rb=0.07:gr=-0.03:gg=0.95:gb=0.08:br=0.0:bg=0.05:bb=1.15","eq":"eq=contrast=1.22:brightness=0.0:saturation=0.88","sharp":"unsharp=5:5:1.0:3:3:0.0","noise":"noise=c0s=12:c0f=t+u","vignette":"vignette=angle=0.42"},"scenes":[{"motion_a":"diagonal_tl_br","motion_b":"zoom_in_sim","transition":"slideleft","energy":"medium","caption":"box"},{"motion_a":"pan_right_fast","motion_b":"diagonal_br_tl","transition":"wipeleft","energy":"medium","caption":"box"},{"motion_a":"pull_back_sim","motion_b":"drift_slow","transition":"dissolve","energy":"low","caption":"plain"}]},
    "vibrant_pop":     {"label":"Vibrant Pop",     "grade":{"ccm":"colorchannelmixer=rr=1.05:rg=0.0:rb=0.0:gr=0.05:gg=1.08:gb=0.0:br=0.0:bg=0.0:bb=1.05","eq":"eq=contrast=1.08:brightness=0.06:saturation=1.72","sharp":"unsharp=3:3:0.6:3:3:0.0","noise":"noise=c0s=6:c0f=t+u","vignette":"vignette=angle=0.22"},"scenes":[{"motion_a":"diagonal_tl_br","motion_b":"diagonal_br_tl","transition":"wiperight","energy":"high","caption":"box"},{"motion_a":"pan_right_fast","motion_b":"zoom_in_sim","transition":"slideright","energy":"high","caption":"box"},{"motion_a":"diagonal_bl_tr","motion_b":"pan_up","transition":"dissolve","energy":"medium","caption":"plain"}]},
    "nostalgic_film":  {"label":"Nostalgic Film",  "grade":{"ccm":"colorchannelmixer=rr=1.12:rg=0.05:rb=-0.08:gr=0.05:gg=1.0:gb=-0.05:br=-0.03:bg=0.0:bb=0.93","eq":"eq=contrast=1.18:brightness=0.03:saturation=1.12","sharp":"unsharp=3:3:0.5:3:3:0.0","noise":"noise=c0s=26:c0f=t+u","vignette":"vignette=angle=0.65"},"scenes":[{"motion_a":"pan_right_slow","motion_b":"drift_slow","transition":"dissolve","energy":"low","caption":"plain"},{"motion_a":"diagonal_bl_tr","motion_b":"pan_up","transition":"fade","energy":"medium","caption":"plain"},{"motion_a":"zoom_in_sim","motion_b":"static_hold","transition":"dissolve","energy":"low","caption":"plain"}]},
    "warm_human":      {"label":"Warm Human",      "grade":{"ccm":"colorchannelmixer=rr=1.10:rg=0.05:rb=-0.05:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=0.0:bb=0.95","eq":"eq=contrast=1.10:brightness=0.05:saturation=1.32","sharp":"unsharp=3:3:0.5:3:3:0.0","noise":"noise=c0s=8:c0f=t+u","vignette":"vignette=angle=0.28"},"scenes":[{"motion_a":"pan_right_slow","motion_b":"zoom_in_sim","transition":"dissolve","energy":"low","caption":"plain"},{"motion_a":"drift_slow","motion_b":"pan_up","transition":"fade","energy":"low","caption":"plain"},{"motion_a":"static_hold","motion_b":"pull_back_sim","transition":"dissolve","energy":"low","caption":"plain"}]},
}
CLUSTER_MOOD_DEFAULTS = {"Space":"cinematic_epic","DeepTech":"cold_tech","AI":"cold_tech","Gadgets":"vibrant_pop","GreenTech":"hopeful_future","Startups":"hopeful_future"}


# ==========================================
# RENDER WITH AUDIO (AI Voice mode)
# ==========================================

@app.function(image=image, secrets=secrets, cpu=4.0, memory=4096, timeout=360)
def render_with_audio(
    job_id: str,
    image_paths: list,       # list of local paths OR None per scene
    audio_path: str,         # local path (may not exist in this container)
    audio_dur: float,
    captions: list,
    mood: str,
    image_bytes_list: list = None,   # list of bytes per scene (preferred)
    audio_bytes: bytes = None,       # audio bytes (preferred over path)
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

    clip_paths = []
    for i, img in enumerate(image_paths):
        clip = _render_scene_clip(job_id, img, scene_dur, i, captions, mood)
        clip_paths.append(clip)

    transitioned = _apply_xfade(job_id, clip_paths, scene_dur, mood)

    for cp in clip_paths:
        try: os.remove(cp)
        except Exception: pass

    fade_st = audio_dur - 0.5
    try:
        _run_ffmpeg([
            "ffmpeg", "-y",
            "-i", transitioned, "-i", audio_path,
            "-vf", f"fade=t=out:st={fade_st:.2f}:d=0.5",
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-c:a", "aac", "-b:a", "128k",
            "-t", str(audio_dur),          # NOT -shortest
            "-movflags", "+faststart", video_path
        ], "final-mux")
    except Exception as e:
        print(f"  Fade mux failed ({e}), plain mux")
        _run_ffmpeg([
            "ffmpeg", "-y",
            "-i", transitioned, "-i", audio_path,
            "-map", "0:v", "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-c:a", "aac", "-b:a", "128k",
            "-t", str(audio_dur),
            "-movflags", "+faststart", video_path
        ], "final-mux-plain")

    try: os.remove(transitioned)
    except Exception: pass

    size = os.path.getsize(video_path)
    if size < 100_000:
        raise Exception(f"Rendered video too small: {size} bytes")
    print(f"  Final: {size // 1024}KB → {video_path}")
    return video_path


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
    return video_path


# ==========================================
# SCENE CLIP RENDERING
# ==========================================

def _render_scene_clip(
    job_id: str, img_path: str, duration: float,
    scene_idx: int, captions: list, mood: str
) -> str:
    """
    Render one image as TWO sub-clips with a hard cut between them.
    All color/motion/caption choices come from MOOD_PRESETS[mood].
    """
    clip_path = f"{TMP_DIR}/{job_id}_clip{scene_idx}.mp4"
    pre_path  = f"{TMP_DIR}/{job_id}_pre{scene_idx}.jpg"
    cap_y     = int(OUT_HEIGHT * 0.73)
    cap_size  = 58
    wm        = _escape_dt("@India20Sixty")

    preset     = MOOD_PRESETS.get(mood, MOOD_PRESETS.get("hopeful_future"))
    if not preset:
        preset = list(MOOD_PRESETS.values())[0]
    scene_cfg  = preset["scenes"][scene_idx % len(preset["scenes"])]
    grade      = preset["grade"]

    motion_a      = MOTIONS.get(scene_cfg["motion_a"], MOTIONS["pan_right_fast"])
    motion_b      = MOTIONS.get(scene_cfg["motion_b"], MOTIONS["diagonal_tl_br"])
    energy        = scene_cfg.get("energy", "medium")
    caption_style = scene_cfg.get("caption", "plain")

    hpct  = max(motion_a["hpct"], motion_b["hpct"])
    pan_w = int(OUT_WIDTH  * hpct)
    pan_h = int(OUT_HEIGHT * hpct)
    dx    = pan_w - OUT_WIDTH
    dy    = pan_h - OUT_HEIGHT

    print(f"  Clip {scene_idx}: [{mood}] {scene_cfg['motion_a']}|{scene_cfg['motion_b']} [{energy}]")

    # PASS 1: Pre-process to scaled JPEG (much faster than re-scaling per frame)
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

    speed = {"high": 1.0, "medium": 0.72, "low": 0.45}.get(energy, 0.72)
    sdx   = max(1, min(int(dx * speed), dx))
    sdy   = max(1, min(int(dy * speed), dy))

    x_a = motion_a["x"](sdx, sdy, n_a)
    y_a = motion_a["y"](sdx, sdy, n_a)
    x_b = motion_b["x"](sdx, sdy, n_b)
    y_b = motion_b["y"](sdx, sdy, n_b)

    def make_vf(x_expr, y_expr, caps_for_sub, sub_dur):
        third = sub_dur / 3.0
        parts = [
            f"crop={OUT_WIDTH}:{OUT_HEIGHT}:{x_expr}:{y_expr}",
            grade["ccm"],        # colorchannelmixer — split-tone grade
            grade["eq"],         # eq — contrast/brightness/saturation (no gamma params)
            grade["sharp"],      # unsharp — edge sharpening
            grade["noise"],      # noise=c0f=t+u — animated grain per-frame
            grade["vignette"],   # vignette=angle=X — numeric only (NOT PI/X)
            "setsar=1",
            # Watermark — borderw (box=1 unreliable on some Modal ffmpeg builds)
            f"drawtext=text='{wm}':fontsize=44:fontcolor=white@0.9"
            f":borderw=4:bordercolor=black@0.95:x=28:y=h-88",
        ]
        for ci, cap in enumerate(caps_for_sub):
            if not cap.strip():
                continue
            escaped = _escape_dt(cap)
            t_s, t_e = ci * third, (ci + 1) * third
            if caption_style == "box":
                # Heavy border creates solid box illusion — confirmed safe on Modal
                parts.append(
                    f"drawtext=text='{escaped}':fontsize={cap_size}:fontcolor=white"
                    f":borderw=13:bordercolor=black@0.72"
                    f":x=(w-text_w)/2:y={cap_y}"
                    f":enable='between(t,{t_s:.3f},{t_e:.3f})'"
                )
            else:
                # Plain — standard bordered text
                parts.append(
                    f"drawtext=text='{escaped}':fontsize={cap_size}:fontcolor=white"
                    f":borderw=5:bordercolor=black@0.85"
                    f":x=(w-text_w)/2:y={cap_y}"
                    f":enable='between(t,{t_s:.3f},{t_e:.3f})'"
                )
        return ",".join(parts)

    scene_caps = captions[scene_idx * 3: scene_idx * 3 + 3]
    while len(scene_caps) < 3:
        scene_caps.append("")

    sub_a = f"{TMP_DIR}/{job_id}_clip{scene_idx}a.mp4"
    sub_b = f"{TMP_DIR}/{job_id}_clip{scene_idx}b.mp4"
    lst   = f"{TMP_DIR}/{job_id}_list{scene_idx}.txt"

    _run_ffmpeg([
        "ffmpeg", "-y", "-loop", "1", "-r", str(FPS), "-i", pre_path,
        "-vf", make_vf(x_a, y_a, [scene_caps[0], scene_caps[1], ""], dur_a),
        "-t", str(dur_a), "-r", str(FPS),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p", sub_a
    ], f"clip-{scene_idx}a", timeout=180)

    _run_ffmpeg([
        "ffmpeg", "-y", "-loop", "1", "-r", str(FPS), "-i", pre_path,
        "-vf", make_vf(x_b, y_b, ["", scene_caps[2], ""], dur_b),
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
    if len(clip_paths) == 1:
        return clip_paths[0]

    output_path = f"{TMP_DIR}/{job_id}_xfaded.mp4"
    n           = len(clip_paths)
    inputs      = []
    for cp in clip_paths:
        inputs += ["-i", cp]

    preset = MOOD_PRESETS.get(mood, list(MOOD_PRESETS.values())[0])

    def get_transition(idx: int) -> str:
        scenes = preset["scenes"]
        key    = scenes[idx % len(scenes)].get("transition", "dissolve")
        if key is None or key not in XFADE_TRANSITIONS:
            return "dissolve"
        return key

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
