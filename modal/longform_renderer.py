import modal
import os
import subprocess
import re
import requests
from pathlib import Path

# ==========================================
# MODAL APP — LONGFORM RENDERER
# Renders a complete long-form video from
# multiple segments, each with:
#   - 1-3 images OR 1 video clip
#   - audio track
#   - mood-based visual effects
#   - chapter title card between segments
#
# Key differences from Shorts renderer:
#   - Accepts video clips (not just images)
#   - Adds chapter title cards
#   - Subtitle-style captions (smaller, lower)
#   - Final output: single concatenated MP4
# ==========================================

app = modal.App("india20sixty-longform-renderer")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("ffmpeg", "fonts-liberation", "fonts-dejavu-core", "fonts-noto")
    .pip_install("requests", "fastapi[standard]")
)

secrets = [modal.Secret.from_name("india20sixty-secrets")]

TMP_DIR    = "/tmp/india20sixty-longform"
OUT_WIDTH  = 1080
OUT_HEIGHT = 1920
FPS        = 25

# MOOD_PRESETS — pool-based, same as renderer.py
MOOD_PRESETS = {
    "cinematic_epic":  {"label":"Cinematic Epic","grade":{"ccm":"colorchannelmixer=rr=1.05:rg=0.0:rb=-0.05:gr=0.0:gg=0.95:gb=0.05:br=-0.10:bg=0.03:bb=1.07","eq":"eq=contrast=1.38:brightness=-0.03:saturation=0.82","sharp":"unsharp=7:7:1.2:3:3:0.0","noise":"noise=c0s=18:c0f=t+u","vignette":"vignette=angle=0.6"},"energy":"high","motion_pools":{0:["zoom_in_fast","diagonal_bl_tr_fast","pan_right_fast","pull_back_fast"],1:["pan_left_fast","diagonal_tr_bl","zoom_in_med","pan_up_fast"],2:["pull_back_slow","diagonal_bl_tr","pan_right_med","drift_up_right"]},"transition_pools":{0:["wiperight","slideleft","slideright"],1:["wipeleft","wiperight"],2:["dissolve","fade"]},"caption":"box"},
    "breaking_news":   {"label":"Breaking News","grade":{"ccm":"colorchannelmixer=rr=0.90:rg=0.05:rb=0.05:gr=0.0:gg=0.95:gb=0.05:br=0.05:bg=0.08:bb=0.87","eq":"eq=contrast=1.28:brightness=0.0:saturation=0.68","sharp":"unsharp=5:5:1.1:3:3:0.0","noise":"noise=c0s=10:c0f=t+u","vignette":"vignette=angle=0.45"},"energy":"high","motion_pools":{0:["pan_right_fast","diagonal_tl_br_fast","zoom_in_fast","pan_left_fast"],1:["pan_left_fast","diagonal_tr_bl","pan_up_fast"],2:["static_hold","pan_right_med","zoom_in_med"]},"transition_pools":{0:["slideleft","wipeleft","wiperight"],1:["wipeleft","slideleft"],2:["fadeblack","dissolve"]},"caption":"box"},
    "hopeful_future":  {"label":"Hopeful Future","grade":{"ccm":"colorchannelmixer=rr=1.08:rg=0.05:rb=-0.03:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=-0.02:bb=0.97","eq":"eq=contrast=1.12:brightness=0.04:saturation=1.35","sharp":"unsharp=3:3:0.7:3:3:0.0","noise":"noise=c0s=8:c0f=t+u","vignette":"vignette=angle=0.30"},"energy":"low","motion_pools":{0:["pan_right_slow","drift_slow","zoom_in_slow","pan_up_slow"],1:["drift_up_right","pan_left_slow","diagonal_bl_tr","pull_back_slow"],2:["pull_back_slow","drift_slow","static_breathe","pan_up_slow"]},"transition_pools":{0:["dissolve","fade"],1:["fade","dissolve"],2:["dissolve","fade"]},"caption":"plain"},
    "dark_serious":    {"label":"Dark Serious","grade":{"ccm":"colorchannelmixer=rr=0.95:rg=0.0:rb=0.05:gr=0.0:gg=0.88:gb=0.12:br=0.08:bg=0.05:bb=0.87","eq":"eq=contrast=1.45:brightness=-0.06:saturation=0.52","sharp":"unsharp=7:7:1.0:3:3:0.0","noise":"noise=c0s=24:c0f=t+u","vignette":"vignette=angle=0.70"},"energy":"low","motion_pools":{0:["drift_slow","pan_left_slow","static_hold","drift_down_left"],1:["diagonal_tr_bl","static_hold","pan_up_slow","drift_slow"],2:["pull_back_slow","static_breathe","pan_up_slow"]},"transition_pools":{0:["fadeblack","dissolve"],1:["dissolve","fade"],2:["fadeblack","fade"]},"caption":"box"},
    "cold_tech":       {"label":"Cold Tech","grade":{"ccm":"colorchannelmixer=rr=0.88:rg=0.05:rb=0.07:gr=-0.03:gg=0.95:gb=0.08:br=0.0:bg=0.05:bb=1.15","eq":"eq=contrast=1.22:brightness=0.0:saturation=0.88","sharp":"unsharp=5:5:1.0:3:3:0.0","noise":"noise=c0s=12:c0f=t+u","vignette":"vignette=angle=0.42"},"energy":"medium","motion_pools":{0:["diagonal_tl_br","zoom_in_med","pan_right_med","diagonal_bl_tr"],1:["pan_left_med","diagonal_br_tl","zoom_in_slow","pan_right_fast"],2:["pull_back_med","drift_slow","static_hold","pan_up_slow"]},"transition_pools":{0:["slideleft","wipeleft","dissolve"],1:["wipeleft","slideright","dissolve"],2:["dissolve","fade"]},"caption":"box"},
    "vibrant_pop":     {"label":"Vibrant Pop","grade":{"ccm":"colorchannelmixer=rr=1.05:rg=0.0:rb=0.0:gr=0.05:gg=1.08:gb=0.0:br=0.0:bg=0.0:bb=1.05","eq":"eq=contrast=1.08:brightness=0.06:saturation=1.72","sharp":"unsharp=3:3:0.6:3:3:0.0","noise":"noise=c0s=6:c0f=t+u","vignette":"vignette=angle=0.22"},"energy":"high","motion_pools":{0:["diagonal_tl_br_fast","pan_right_fast","zoom_in_fast","diagonal_bl_tr_fast"],1:["pan_left_fast","zoom_in_med","diagonal_tr_bl","diagonal_br_tl"],2:["diagonal_bl_tr","zoom_in_med","pan_up_fast","pan_right_med"]},"transition_pools":{0:["wiperight","slideright","slideleft"],1:["slideright","wiperight","wipeleft"],2:["dissolve","wiperight"]},"caption":"box"},
    "nostalgic_film":  {"label":"Nostalgic Film","grade":{"ccm":"colorchannelmixer=rr=1.12:rg=0.05:rb=-0.08:gr=0.05:gg=1.0:gb=-0.05:br=-0.03:bg=0.0:bb=0.93","eq":"eq=contrast=1.18:brightness=0.03:saturation=1.12","sharp":"unsharp=3:3:0.5:3:3:0.0","noise":"noise=c0s=26:c0f=t+u","vignette":"vignette=angle=0.65"},"energy":"low","motion_pools":{0:["pan_right_slow","drift_slow","zoom_in_slow","drift_up_right"],1:["diagonal_bl_tr","pan_up_slow","drift_down_left","pan_left_slow"],2:["zoom_in_slow","static_breathe","pull_back_slow","drift_slow"]},"transition_pools":{0:["dissolve","fade"],1:["fade","dissolve"],2:["dissolve","fadeblack"]},"caption":"plain"},
    "warm_human":      {"label":"Warm Human","grade":{"ccm":"colorchannelmixer=rr=1.10:rg=0.05:rb=-0.05:gr=0.03:gg=1.02:gb=-0.05:br=-0.05:bg=0.0:bb=0.95","eq":"eq=contrast=1.10:brightness=0.05:saturation=1.32","sharp":"unsharp=3:3:0.5:3:3:0.0","noise":"noise=c0s=8:c0f=t+u","vignette":"vignette=angle=0.28"},"energy":"low","motion_pools":{0:["pan_right_slow","drift_slow","zoom_in_slow","drift_up_right"],1:["drift_slow","pan_up_slow","drift_down_left","zoom_in_slow"],2:["static_breathe","pull_back_slow","drift_slow","pan_up_slow"]},"transition_pools":{0:["dissolve","fade"],1:["fade","dissolve"],2:["dissolve","fade"]},"caption":"plain"},
}

MOTIONS = {
    "pan_right_slow":      {"hpct":1.18,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy//2}","speed":"slow"},
    "pan_right_med":       {"hpct":1.24,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy//2}","speed":"medium"},
    "pan_right_fast":      {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy//3}","speed":"fast"},
    "pan_left_slow":       {"hpct":1.18,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}","y":lambda dx,dy,n:f"{dy//2}","speed":"slow"},
    "pan_left_med":        {"hpct":1.24,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}","y":lambda dx,dy,n:f"{dy//2}","speed":"medium"},
    "pan_left_fast":       {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}","y":lambda dx,dy,n:f"{dy//3}","speed":"fast"},
    "pan_up_slow":         {"hpct":1.22,"x":lambda dx,dy,n:f"{dx//2}","y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}","speed":"slow"},
    "pan_up_fast":         {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//2}","y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}","speed":"fast"},
    "pan_down_slow":       {"hpct":1.22,"x":lambda dx,dy,n:f"{dx//2}","y":lambda dx,dy,n:f"{dy}*n/{n}","speed":"slow"},
    "diagonal_tl_br":      {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy}*n/{n}","speed":"medium"},
    "diagonal_tr_bl":      {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}","y":lambda dx,dy,n:f"{dy}*n/{n}","speed":"medium"},
    "diagonal_bl_tr":      {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}","speed":"medium"},
    "diagonal_br_tl":      {"hpct":1.30,"x":lambda dx,dy,n:f"{dx}-{dx}*n/{n}","y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}","speed":"medium"},
    "diagonal_tl_br_fast": {"hpct":1.36,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy}*n/{n}","speed":"fast"},
    "diagonal_bl_tr_fast": {"hpct":1.36,"x":lambda dx,dy,n:f"{dx}*n/{n}","y":lambda dx,dy,n:f"{dy}-{dy}*n/{n}","speed":"fast"},
    "zoom_in_slow":        {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//2}-{dx//5}*n/{n}","y":lambda dx,dy,n:f"{dy//2}-{dy//5}*n/{n}","speed":"slow"},
    "zoom_in_med":         {"hpct":1.35,"x":lambda dx,dy,n:f"{dx//2}-{dx//4}*n/{n}","y":lambda dx,dy,n:f"{dy//2}-{dy//4}*n/{n}","speed":"medium"},
    "zoom_in_fast":        {"hpct":1.42,"x":lambda dx,dy,n:f"{dx//2}-{dx//3}*n/{n}","y":lambda dx,dy,n:f"{dy//2}-{dy//3}*n/{n}","speed":"fast"},
    "pull_back_slow":      {"hpct":1.28,"x":lambda dx,dy,n:f"{dx//5}+{dx//5}*n/{n}","y":lambda dx,dy,n:f"{dy//5}+{dy//5}*n/{n}","speed":"slow"},
    "pull_back_med":       {"hpct":1.35,"x":lambda dx,dy,n:f"{dx//4}+{dx//4}*n/{n}","y":lambda dx,dy,n:f"{dy//4}+{dy//4}*n/{n}","speed":"medium"},
    "pull_back_fast":      {"hpct":1.42,"x":lambda dx,dy,n:f"{dx//3}+{dx//3}*n/{n}","y":lambda dx,dy,n:f"{dy//3}+{dy//3}*n/{n}","speed":"fast"},
    "drift_slow":          {"hpct":1.12,"x":lambda dx,dy,n:f"{dx//3}*n/{n}","y":lambda dx,dy,n:f"{dy//5}","speed":"slow"},
    "drift_up_right":      {"hpct":1.15,"x":lambda dx,dy,n:f"{dx//4}*n/{n}","y":lambda dx,dy,n:f"{dy//3}-{dy//4}*n/{n}","speed":"slow"},
    "drift_down_left":     {"hpct":1.15,"x":lambda dx,dy,n:f"{dx//3}-{dx//4}*n/{n}","y":lambda dx,dy,n:f"{dy//4}*n/{n}","speed":"slow"},
    "static_hold":         {"hpct":1.05,"x":lambda dx,dy,n:f"{dx//2}","y":lambda dx,dy,n:f"{dy//2}","speed":"static"},
    "static_breathe":      {"hpct":1.08,"x":lambda dx,dy,n:f"{dx//2}","y":lambda dx,dy,n:f"{dy//2}","speed":"static"},
}

def _pick_motion_lf(pool_key: int, preset: dict, used: set) -> str:
    import random
    pool = preset.get("motion_pools",{}).get(pool_key, list(MOTIONS.keys()))
    available = [m for m in pool if m not in used]
    if not available: available = pool
    choice = random.choice(available)
    used.add(choice)
    return choice


# ==========================================
# RENDER FULL VIDEO
# ==========================================

@app.function(image=image, secrets=secrets, cpu=4.0, memory=8192, timeout=1200)
def render_longform(
    job_id: str,
    segments: list,
    mood: str,
) -> bytes:
    """
    Render all segments into one final MP4.
    Segments contain R2 URLs — renderer downloads media itself.
    Returns video bytes.
    """
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    print(f"\n[Longform Render] job={job_id} mood={mood} segments={len(segments)}")

    # Download all media into this container's /tmp/
    resolved = []
    for seg in segments:
        seg_idx   = seg["segment_idx"]
        voice_url = seg.get("voice_url","")
        if not voice_url:
            print(f"  Seg {seg_idx}: no voice URL — skipping")
            continue

        audio_local = f"{TMP_DIR}/{job_id}_seg{seg_idx}_audio.mp3"
        _download(voice_url, audio_local)
        audio_dur = _get_duration(audio_local)

        media_local = []
        for m in (seg.get("media_urls") or []):
            url = m.get("url","")
            if not url: continue
            ext  = ".mp4" if m.get("type") == "video" else ".png"
            path = f"{TMP_DIR}/{job_id}_seg{seg_idx}_m{m.get('order',0)}{ext}"
            _download(url, path)
            media_local.append({"type": m.get("type","image"), "local_path": path})

        resolved.append({
            **seg,
            "audio_path": audio_local,
            "audio_dur":  audio_dur,
            "media":      media_local,
        })

    if not resolved:
        raise Exception("No segments could be resolved")

    rendered_segments = []
    for seg in resolved:
        seg_path = _render_segment(job_id, seg, mood)
        if seg_path:
            rendered_segments.append((seg, seg_path))
            print(f"  Seg {seg['segment_idx']} [{seg['label']}]: {os.path.getsize(seg_path)//1024}KB")

    if not rendered_segments:
        raise Exception("No segments rendered successfully")

    final_path = _concatenate_segments(job_id, rendered_segments)
    size = os.path.getsize(final_path)
    print(f"\n  Final: {size//1024}KB ({size//1024//1024}MB)")
    if size < 500_000:
        raise Exception(f"Final video too small: {size} bytes")

    with open(final_path, "rb") as f:
        return f.read()


# ==========================================
# RENDER SINGLE SEGMENT
# ==========================================

def _render_segment(job_id: str, seg: dict, mood: str) -> str:
    seg_idx    = seg["segment_idx"]
    label      = seg["label"]
    audio_path = seg["audio_path"]
    audio_dur  = seg["audio_dur"]
    media      = seg.get("media", [])
    cap_style  = seg.get("caption_style", "subtitle")

    print(f"\n  [Seg {seg_idx}: {label}] dur={audio_dur:.1f}s media={len(media)}")

    # No media — can't render
    if not media:
        print(f"  WARNING: no media for segment {seg_idx}, skipping")
        return None

    out_path = f"{TMP_DIR}/{job_id}_seg{seg_idx}.mp4"

    # Video clips pass through directly with audio overlay
    if media[0]["type"] == "video":
        return _render_video_clip_segment(job_id, seg_idx, media[0]["local_path"],
                                          audio_path, audio_dur, out_path)

    # Image-based segments
    return _render_image_segment(job_id, seg_idx, media, audio_path, audio_dur,
                                  mood, cap_style, label, out_path)


def _render_image_segment(job_id, seg_idx, media, audio_path, audio_dur,
                            mood, cap_style, label, out_path):
    """Render image-based segment with pan motion and mood grade."""
    preset      = MOOD_PRESETS.get(mood, list(MOOD_PRESETS.values())[0])
    grade       = preset["grade"]
    wm          = _escape_dt("@India20Sixty")
    n_images    = len(media)
    time_per_img = audio_dur / n_images

    import random
    clip_paths = []
    used_motions = set()
    preset = MOOD_PRESETS.get(mood, list(MOOD_PRESETS.values())[0])

    for i, m in enumerate(media):
        img_path  = m["local_path"]
        clip_path = f"{TMP_DIR}/{job_id}_seg{seg_idx}_img{i}.mp4"
        pre_path  = f"{TMP_DIR}/{job_id}_seg{seg_idx}_pre{i}.jpg"

        # Pick motion from pool — no consecutive repeats
        motion_key = _pick_motion_lf(i % 3, preset, used_motions)
        used_motions.add(motion_key)
        motion   = MOTIONS.get(motion_key, MOTIONS["pan_right_slow"])
        hpct     = motion["hpct"]
        pan_w    = int(OUT_WIDTH * hpct)
        pan_h    = int(OUT_HEIGHT * hpct)
        dx, dy   = pan_w - OUT_WIDTH, pan_h - OUT_HEIGHT
        n_frames = int(time_per_img * FPS)
        print(f"  Seg {seg_idx} img {i}: {motion_key}")

        # Pre-process
        _ffmpeg([
            "ffmpeg","-y","-i",img_path,
            "-vf",f"scale={pan_w}:{pan_h}:force_original_aspect_ratio=increase:flags=lanczos,crop={pan_w}:{pan_h}",
            "-frames:v","1","-q:v","3","-f","image2","-vcodec","mjpeg",pre_path
        ], f"pre-{seg_idx}-{i}", timeout=20)

        speed_map = {"fast": 1.0, "medium": 0.72, "slow": 0.45, "static": 0.0}
        spd = speed_map.get(motion.get("speed","medium"), 0.72)
        sdx = max(1, min(int(dx * spd), dx))
        sdy = max(1, min(int(dy * spd), dy))

        x_expr = motion["x"](sdx, sdy, n_frames)
        y_expr = motion["y"](sdx, sdy, n_frames)

        # Caption — subtitle style for long-form (smaller, positioned lower)
        cap_filter = ""
        if cap_style == "large":
            cap_label = _escape_dt(label.upper())
            cap_filter = (f",drawtext=text='{cap_label}':fontsize=72:fontcolor=white"
                          f":borderw=8:bordercolor=black@0.9:x=(w-text_w)/2:y=h*0.68")
        elif cap_style == "subtitle":
            cap_label = _escape_dt(label)
            cap_filter = (f",drawtext=text='{cap_label}':fontsize=44:fontcolor=white@0.85"
                          f":borderw=5:bordercolor=black@0.7:x=(w-text_w)/2:y=h*0.88"
                          f":enable='between(t,0,3)'")  # show label only first 3s of each image

        vf = (f"crop={OUT_WIDTH}:{OUT_HEIGHT}:{x_expr}:{y_expr},"
              f"{grade['ccm']},{grade['eq']},{grade['sharp']},{grade['noise']},"
              f"{grade['vignette']},setsar=1,"
              f"drawtext=text='{wm}':fontsize=40:fontcolor=white@0.8:borderw=3:bordercolor=black@0.9:x=24:y=h-80"
              f"{cap_filter}")

        _ffmpeg([
            "ffmpeg","-y","-loop","1","-r",str(FPS),"-i",pre_path,
            "-vf",vf,"-t",str(time_per_img),"-r",str(FPS),
            "-c:v","libx264","-preset","fast","-crf","20","-pix_fmt","yuv420p",clip_path
        ], f"img-clip-{seg_idx}-{i}", timeout=180)

        clip_paths.append(clip_path)
        try: os.remove(pre_path)
        except Exception: pass
        try: os.remove(img_path)
        except Exception: pass

    # Concatenate clips then add audio
    if len(clip_paths) == 1:
        video_only = clip_paths[0]
    else:
        list_path  = f"{TMP_DIR}/{job_id}_seg{seg_idx}_imglist.txt"
        concat_path = f"{TMP_DIR}/{job_id}_seg{seg_idx}_concat.mp4"
        with open(list_path, "w") as f:
            for cp in clip_paths: f.write(f"file '{cp}'\n")
        _ffmpeg(["ffmpeg","-y","-f","concat","-safe","0","-i",list_path,"-c","copy",concat_path],
                f"concat-{seg_idx}", timeout=60)
        try: os.remove(list_path)
        except Exception: pass
        for cp in clip_paths:
            try: os.remove(cp)
            except Exception: pass
        video_only = concat_path

    # Add audio
    _ffmpeg([
        "ffmpeg","-y","-i",video_only,"-i",audio_path,
        "-map","0:v","-map","1:a",
        "-c:v","libx264","-preset","fast","-crf","22",
        "-c:a","aac","-b:a","128k",
        "-t",str(audio_dur),"-movflags","+faststart",out_path
    ], f"mux-{seg_idx}", timeout=120)

    try: os.remove(video_only)
    except Exception: pass

    return out_path


def _render_video_clip_segment(job_id, seg_idx, clip_path, audio_path, audio_dur, out_path):
    """
    For video clip segments: overlay audio onto the clip.
    Trims or loops video to match audio duration.
    """
    wm = _escape_dt("@India20Sixty")
    # Scale to portrait 1080x1920 if needed, add watermark, overlay audio
    _ffmpeg([
        "ffmpeg","-y",
        "-stream_loop","-1","-i",clip_path,  # loop if short
        "-i",audio_path,
        "-filter_complex",
        (f"[0:v]scale={OUT_WIDTH}:{OUT_HEIGHT}:force_original_aspect_ratio=increase:flags=lanczos,"
         f"crop={OUT_WIDTH}:{OUT_HEIGHT},"
         f"drawtext=text='{wm}':fontsize=40:fontcolor=white@0.8:borderw=3:bordercolor=black@0.9:x=24:y=h-80[vout]"),
        "-map","[vout]","-map","1:a",
        "-c:v","libx264","-preset","fast","-crf","22",
        "-c:a","aac","-b:a","128k",
        "-t",str(audio_dur),"-movflags","+faststart",out_path
    ], f"videoclip-{seg_idx}", timeout=180)
    return out_path


# ==========================================
# CONCATENATE SEGMENTS WITH CHAPTER CARDS
# ==========================================

def _concatenate_segments(job_id: str, rendered_segments: list) -> str:
    """
    Stitch all segment videos together.
    Adds a 1.5s chapter title card between segments (except after last).
    """
    final_path = f"{TMP_DIR}/{job_id}_final.mp4"
    all_parts  = []

    for i, (seg, seg_path) in enumerate(rendered_segments):
        all_parts.append(seg_path)
        # Add chapter card between segments (not after the last one)
        if i < len(rendered_segments) - 1:
            next_seg = rendered_segments[i + 1][0]
            card_path = _make_chapter_card(job_id, i, next_seg["label"])
            if card_path:
                all_parts.append(card_path)

    list_path = f"{TMP_DIR}/{job_id}_finallist.txt"
    with open(list_path, "w") as f:
        for p in all_parts:
            f.write(f"file '{p}'\n")

    _ffmpeg([
        "ffmpeg","-y","-f","concat","-safe","0","-i",list_path,
        "-c:v","libx264","-preset","fast","-crf","22",
        "-c:a","aac","-b:a","128k",
        "-movflags","+faststart",final_path
    ], "final-concat", timeout=300)

    # Cleanup
    try: os.remove(list_path)
    except Exception: pass
    for p in all_parts:
        try: os.remove(p)
        except Exception: pass

    return final_path


def _make_chapter_card(job_id: str, after_idx: int, next_label: str) -> str:
    """
    1.5 second black card with the next chapter's title.
    Used as a visual break between segments.
    """
    card_path = f"{TMP_DIR}/{job_id}_card{after_idx}.mp4"
    label_esc = _escape_dt(next_label.upper())
    try:
        _ffmpeg([
            "ffmpeg","-y","-f","lavfi",
            "-i",f"color=c=black:s={OUT_WIDTH}x{OUT_HEIGHT}:d=1.5",
            "-vf",(f"drawtext=text='{label_esc}':fontsize=58:fontcolor=white"
                   f":borderw=5:bordercolor=black@0.8:x=(w-text_w)/2:y=(h-text_h)/2,"
                   f"fade=t=in:st=0:d=0.3,fade=t=out:st=1.2:d=0.3"),
            "-c:v","libx264","-preset","fast","-crf","22",
            "-pix_fmt","yuv420p",
            "-af","anullsrc=r=44100:cl=mono","-ar","44100","-ac","1",
            "-t","1.5",card_path
        ], f"chapter-card-{after_idx}", timeout=15)
        return card_path
    except Exception as e:
        print(f"  Chapter card failed (non-fatal): {e}")
        return None


# ==========================================
# UTILITIES
# ==========================================

def _ffmpeg(cmd, label, timeout=300):
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print(f"  ffmpeg [{label}] FAILED:")
        print(result.stderr[-600:])
        raise Exception(f"{label}: {result.stderr[-150:]}")
    return result


def _escape_dt(text: str) -> str:
    text = text.replace("\\", "\\\\")
    text = text.replace("'", "\u2019")
    text = text.replace(":", "\\:")
    text = text.replace("%", "\\%")
    return text


def _download(url: str, path: str):
    import requests as req
    r = req.get(url, timeout=60, stream=True)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192): f.write(chunk)


def _get_duration(path: str) -> float:
    try:
        r = subprocess.run(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=10)
        return float(r.stdout.strip())
    except Exception:
        return 30.0


@app.local_entrypoint()
def main():
    print("longform_renderer.py — use render_longform.remote() from longform_pipeline.py")
