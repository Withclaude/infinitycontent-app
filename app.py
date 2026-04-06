"""
InfinityContent App — Cloud Video Processor (Internal SaaS)
Multi-user • Google Drive I/O • FFmpeg • Streamlit Cloud-ready
Advanced Obfuscation Engine — Ghost Overlay | Frame Jittering | RGB Shifts | Metadata Strip
"""

import io
import os
import re
import json
import shutil
import random
import tempfile
import datetime
import subprocess
from pathlib import Path

import streamlit as st
import imageio_ffmpeg
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="InfinityContent Processor",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]

VIDEO_EXTENSIONS = {
    ".mp4", ".mov", ".avi", ".mkv", ".wmv",
    ".flv", ".webm", ".m4v", ".mpeg", ".mpg",
}

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff",
}

GDRIVE_FOLDER_PATTERN = re.compile(
    r"(?:drive\.google\.com/(?:drive/(?:u/\d+/)?folders/|open\?id=)|id=)([\w-]+)"
)

# ─────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────
def _init_session():
    defaults = {
        "authenticated": False,
        "user_id": None,
        "activity_log": [],
        "processed_files": [],
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

_init_session()

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
def log_activity(user_id: str, filename: str, status: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{user_id}] — {filename} — {status}"
    print(line, flush=True)
    st.session_state.activity_log.append(line)

# ─────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────
def _get_valid_users() -> dict:
    try:
        return dict(st.secrets["users"])
    except Exception:
        return {}

def _authenticate(access_key: str) -> bool:
    users = _get_valid_users()
    if access_key in users:
        st.session_state.authenticated = True
        st.session_state.user_id = users[access_key]
        return True
    return False

def _logout():
    for key in ["authenticated", "user_id", "activity_log", "processed_files"]:
        st.session_state.pop(key, None)
    _init_session()
    st.rerun()

# ─────────────────────────────────────────────────────────────
# GOOGLE DRIVE
# ─────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _get_drive_service():
    # Priority 1: OAuth2 refresh token
    try:
        oauth = st.secrets["gdrive_oauth"]
        creds = Credentials(
            token=None,
            refresh_token=oauth["refresh_token"],
            client_id=oauth["client_id"],
            client_secret=oauth["client_secret"],
            token_uri="https://oauth2.googleapis.com/token",
            scopes=SCOPES,
        )
        creds.refresh(Request())
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception:
        pass

    # Priority 2: Service Account JSON file in project root
    root = Path(__file__).parent
    sa_info = None
    for candidate in [root / "key.json"] + sorted(root.glob("*.json")):
        if not candidate.exists():
            continue
        try:
            data = json.load(open(candidate))
            if data.get("type") == "service_account":
                sa_info = data
                break
        except Exception:
            continue

    # Priority 3: st.secrets fallback
    if sa_info is None:
        sa_info = dict(st.secrets["gcp_service_account"])
        if "private_key" in sa_info:
            sa_info["private_key"] = sa_info["private_key"].replace("\\n", "\n")

    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def parse_folder_id(url_or_id: str) -> str | None:
    url_or_id = url_or_id.strip()
    match = GDRIVE_FOLDER_PATTERN.search(url_or_id)
    if match:
        return match.group(1)
    if re.fullmatch(r"[\w-]{20,50}", url_or_id):
        return url_or_id
    return None


def list_media_files(service, folder_id: str) -> list[dict]:
    """List all video and image files in a Drive folder."""
    results = []
    page_token = None
    query = (
        f"'{folder_id}' in parents "
        f"and (mimeType contains 'video/' or mimeType contains 'image/') "
        f"and trashed = false"
    )
    while True:
        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, size, mimeType)",
            pageToken=page_token,
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def list_video_files_only(service, folder_id: str) -> list[dict]:
    """List only video files (used for ghost overlay folder).
    Only fetches IDs/names — no full download yet."""
    results = []
    page_token = None
    query = (
        f"'{folder_id}' in parents "
        f"and mimeType contains 'video/' "
        f"and trashed = false"
    )
    while True:
        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def download_file(service, file_id: str, dest_path: str, progress_cb=None):
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if progress_cb and status:
                progress_cb(status.progress())


def upload_file(service, local_path: str, folder_id: str, filename: str) -> str:
    meta = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True, chunksize=8 * 1024 * 1024)
    file = service.files().create(
        body=meta,
        media_body=media,
        fields="id, webViewLink",
        supportsAllDrives=True,
    ).execute()
    return file.get("webViewLink", "")

# ─────────────────────────────────────────────────────────────
# FFMPEG / FFPROBE
# ─────────────────────────────────────────────────────────────
def _get_ffmpeg_binary() -> str:
    try:
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return "ffmpeg"


def _get_ffprobe_binary() -> str | None:
    # Known fixed locations (Homebrew on Apple Silicon / Intel)
    candidates = [
        "/opt/homebrew/bin/ffprobe",
        "/usr/local/bin/ffprobe",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c

    # Try system PATH
    try:
        r = subprocess.run(["ffprobe", "-version"], capture_output=True, text=True)
        if r.returncode == 0:
            return "ffprobe"
    except Exception:
        pass

    # Try next to imageio_ffmpeg binary
    try:
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        d = os.path.dirname(ffmpeg_path)
        for name in ["ffprobe.exe", "ffprobe"]:
            p = os.path.join(d, name)
            if os.path.exists(p):
                return p
    except Exception:
        pass
    return None


def get_media_info(path: str, ffprobe_bin: str) -> dict | None:
    try:
        r = subprocess.run(
            [ffprobe_bin, "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", path],
            capture_output=True, text=True,
        )
        return json.loads(r.stdout)
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────
def random_filename(ext: str) -> str:
    prefix = random.choice(["IMG", "MOV", "VID", "DSC", "MVI", "CLIP"])
    num = random.randint(1000, 9999)
    return f"{prefix}_{num}{ext}"

# ─────────────────────────────────────────────────────────────
# ADVANCED VIDEO MODIFICATION
# ─────────────────────────────────────────────────────────────
def modify_video(
    input_path: str,
    output_path: str,
    ffmpeg_bin: str,
    ffprobe_bin: str,
    ghost_videos: list[str],
) -> tuple[bool, str, dict]:
    info = get_media_info(input_path, ffprobe_bin)
    if not info:
        return False, "ffprobe failed to read file", {}

    orig_w = orig_h = None
    duration = float(info.get("format", {}).get("duration", 0))
    for s in info.get("streams", []):
        if s.get("codec_type") == "video":
            orig_w, orig_h = int(s["width"]), int(s["height"])
            break
    if not orig_w or duration == 0:
        return False, "Could not read video dimensions or duration", {}

    p = {
        "brightness":          random.uniform(-0.03, 0.03),
        "contrast":            random.uniform(0.97, 1.03),
        "saturation":          random.uniform(0.97, 1.03),
        "gamma":               random.uniform(0.97, 1.03),
        "hue":                 random.uniform(-5, 5),
        "speed":               random.uniform(0.98, 1.02),
        "crop":                random.uniform(0.97, 0.99),
        "mirror":              random.choice([True, False]),
        "red_shift":           random.uniform(-0.005, 0.005),
        "green_shift":         random.uniform(-0.005, 0.005),
        "blue_shift":          random.uniform(-0.005, 0.005),
        "noise":               random.randint(1, 3),
        "vignette":            random.choice([True, False]),
        "vignette_angle":      random.uniform(0.3, 0.42),
        "sharpen":             random.uniform(0.3, 0.8),
        "blur":                random.uniform(0, 0.2),
        "color_temp":          random.choice(["warm", "cool", "neutral"]),
        "audio_pitch":         random.uniform(0.99, 1.01),
        "audio_noise_vol":     random.uniform(0.001, 0.003),
        "audio_bitrate":       random.choice(["126k", "128k", "130k", "132k"]),
        "video_bitrate":       random.randint(4800, 5200),
        "gop_size":            random.randint(24, 48),
        "frame_drop_interval": random.randint(45, 60),
        "ghost_opacity":       random.uniform(0.005, 0.01),
    }

    # === VIDEO FILTER CHAIN ===
    vf = []

    # Speed
    vf.append(f"setpts={1.0 / p['speed']}*PTS")

    # Frame jittering — remove 1 random frame every N frames
    drop_pos = random.randint(0, p["frame_drop_interval"] - 1)
    vf.append(f"select='not(eq(mod(n\\,{p['frame_drop_interval']})\\,{drop_pos}))'")
    vf.append("setpts=N/FRAME_RATE/TB")

    # Color temperature
    if p["color_temp"] == "warm":
        vf.append("colorbalance=rs=0.03:gs=-0.01:bs=-0.03")
    elif p["color_temp"] == "cool":
        vf.append("colorbalance=rs=-0.03:gs=0.01:bs=0.03")

    # RGB micro-shifts
    vf.append(
        f"colorbalance=rs={p['red_shift']}:gs={p['green_shift']}:bs={p['blue_shift']}"
    )

    # Main color/light adjustments
    vf.append(
        f"eq=brightness={p['brightness']}:contrast={p['contrast']}"
        f":saturation={p['saturation']}:gamma={p['gamma']}"
    )
    vf.append(f"hue=h={p['hue']}")

    # Crop + scale back to original size
    vf.append(f"crop={int(orig_w * p['crop'])}:{int(orig_h * p['crop'])}")
    vf.append(f"scale={orig_w}:{orig_h}:flags=lanczos")

    # Mirror
    if p["mirror"]:
        vf.append("hflip")

    # Noise grain
    vf.append(f"noise=alls={p['noise']}:allf=t")

    # Vignette
    if p["vignette"]:
        vf.append(f"vignette=angle={p['vignette_angle']}")

    # Sharpen or blur
    if p["blur"] > 0.12:
        vf.append(f"gblur=sigma={p['blur']}")
    else:
        vf.append(f"unsharp=5:5:{p['sharpen']}:5:5:0")

    video_filter = ",".join(vf)
    audio_filter = f"atempo={p['speed'] * p['audio_pitch']}"
    noise_video = random.choice(ghost_videos) if ghost_videos else None
    success = False
    last_err = ""

    # === ATTEMPT 1: With ghost overlay ===
    if noise_video:
        cmd = [
            ffmpeg_bin, "-y",
            "-i", input_path,
            "-stream_loop", "-1", "-i", noise_video,
            "-f", "lavfi", "-i", f"anoisesrc=d={duration + 1}:c=pink:a={p['audio_noise_vol']}",
            "-filter_complex",
            f"[1:v]scale={orig_w}:{orig_h},format=yuva420p,"
            f"colorchannelmixer=aa={p['ghost_opacity']}[ghost];"
            f"[0:v]{video_filter}[main];"
            f"[main][ghost]overlay=0:0:shortest=1[vout];"
            f"[0:a]{audio_filter}[amain];"
            f"[amain][2:a]amix=inputs=2:weights=1 {p['audio_noise_vol']}[aout]",
            "-map", "[vout]", "-map", "[aout]",
            "-map_metadata", "-1",
            "-fflags", "+bitexact", "-flags:v", "+bitexact", "-flags:a", "+bitexact",
            "-c:v", "libx264", "-preset", "medium",
            "-b:v", f"{p['video_bitrate']}k",
            "-g", str(p["gop_size"]),
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", p["audio_bitrate"],
            "-movflags", "+faststart",
            "-t", str(duration),
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            success = True
            p["ghost_overlay"] = os.path.basename(noise_video)
        else:
            last_err = result.stderr.strip()

    # === ATTEMPT 2: Without ghost, with audio noise ===
    if not success:
        cmd = [
            ffmpeg_bin, "-y",
            "-i", input_path,
            "-f", "lavfi", "-i", f"anoisesrc=d={duration + 1}:c=pink:a={p['audio_noise_vol']}",
            "-filter_complex",
            f"[0:v]{video_filter}[vout];"
            f"[0:a]{audio_filter}[amain];"
            f"[amain][1:a]amix=inputs=2:weights=1 {p['audio_noise_vol']}[aout]",
            "-map", "[vout]", "-map", "[aout]",
            "-map_metadata", "-1",
            "-fflags", "+bitexact", "-flags:v", "+bitexact", "-flags:a", "+bitexact",
            "-c:v", "libx264", "-preset", "medium",
            "-b:v", f"{p['video_bitrate']}k",
            "-g", str(p["gop_size"]),
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", p["audio_bitrate"],
            "-movflags", "+faststart",
            "-t", str(duration),
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            success = True
            p["ghost_overlay"] = "none"
        else:
            last_err = result.stderr.strip()

    # === ATTEMPT 3: Simple fallback ===
    if not success:
        cmd = [
            ffmpeg_bin, "-y", "-i", input_path,
            "-map_metadata", "-1",
            "-fflags", "+bitexact", "-flags:v", "+bitexact", "-flags:a", "+bitexact",
            "-vf", video_filter,
            "-af", audio_filter,
            "-c:v", "libx264", "-preset", "medium",
            "-b:v", f"{p['video_bitrate']}k",
            "-g", str(p["gop_size"]),
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", p["audio_bitrate"],
            "-movflags", "+faststart",
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            success = True
            p["ghost_overlay"] = "fallback"
        else:
            last_err = result.stderr.strip()

    if success:
        p["size_mb"] = round(os.path.getsize(output_path) / (1024 * 1024), 1)
        return True, "", p
    return False, last_err, {}

# ─────────────────────────────────────────────────────────────
# ADVANCED IMAGE MODIFICATION
# ─────────────────────────────────────────────────────────────
def modify_image(
    input_path: str,
    output_path: str,
    ffmpeg_bin: str,
    ffprobe_bin: str,
) -> tuple[bool, str, dict]:
    info = get_media_info(input_path, ffprobe_bin)
    if not info:
        return False, "ffprobe failed to read file", {}

    orig_w = orig_h = None
    for s in info.get("streams", []):
        if s.get("width"):
            orig_w, orig_h = int(s["width"]), int(s["height"])
            break
    if not orig_w:
        return False, "Could not read image dimensions", {}

    p = {
        "brightness":     random.uniform(-0.03, 0.03),
        "contrast":       random.uniform(0.97, 1.03),
        "saturation":     random.uniform(0.97, 1.03),
        "hue":            random.uniform(-5, 5),
        "crop":           random.uniform(0.97, 0.99),
        "mirror":         random.choice([True, False]),
        "noise":          random.randint(1, 3),
        "vignette":       random.choice([True, False]),
        "vignette_angle": random.uniform(0.3, 0.42),
        "sharpen":        random.uniform(0.3, 0.8),
        "gamma":          random.uniform(0.97, 1.03),
        "color_temp":     random.choice(["warm", "cool", "neutral"]),
        "blur":           random.uniform(0, 0.2),
        "red_shift":      random.uniform(-0.005, 0.005),
        "green_shift":    random.uniform(-0.005, 0.005),
        "blue_shift":     random.uniform(-0.005, 0.005),
    }

    f = []
    if p["color_temp"] == "warm":
        f.append("colorbalance=rs=0.03:gs=-0.01:bs=-0.03")
    elif p["color_temp"] == "cool":
        f.append("colorbalance=rs=-0.03:gs=0.01:bs=0.03")
    f.append(f"colorbalance=rs={p['red_shift']}:gs={p['green_shift']}:bs={p['blue_shift']}")
    f.append(
        f"eq=brightness={p['brightness']}:contrast={p['contrast']}"
        f":saturation={p['saturation']}:gamma={p['gamma']}"
    )
    f.append(f"hue=h={p['hue']}")
    f.append(f"crop={int(orig_w * p['crop'])}:{int(orig_h * p['crop'])}")
    f.append(f"scale={orig_w}:{orig_h}:flags=lanczos")
    if p["mirror"]:
        f.append("hflip")
    f.append(f"noise=alls={p['noise']}:allf=t")
    if p["vignette"]:
        f.append(f"vignette=angle={p['vignette_angle']}")
    if p["blur"] > 0.12:
        f.append(f"gblur=sigma={p['blur']}")
    else:
        f.append(f"unsharp=5:5:{p['sharpen']}:5:5:0")

    ext = os.path.splitext(output_path)[1].lower()
    quality_flags = (
        ["-q:v", "2"] if ext in [".jpg", ".jpeg"]
        else ["-compression_level", "3"] if ext == ".png"
        else []
    )

    cmd = (
        [ffmpeg_bin, "-y", "-i", input_path,
         "-map_metadata", "-1",
         "-fflags", "+bitexact", "-flags:v", "+bitexact",
         "-vf", ",".join(f)]
        + quality_flags
        + [output_path]
    )

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if result.returncode == 0:
        p["size_mb"] = round(os.path.getsize(output_path) / (1024 * 1024), 1)
        return True, "", p
    return False, result.stderr.strip(), {}

# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.image(
            "https://via.placeholder.com/200x60/1a1a2e/ffffff?text=InfinityContent",
            use_container_width=True,
        )
        st.markdown("---")

        if not st.session_state.authenticated:
            st.subheader("🔐 VA Login")
            key_input = st.text_input(
                "Access Key",
                type="password",
                placeholder="Paste your personal access key…",
                key="login_key_input",
            )
            if st.button("Login", use_container_width=True, type="primary"):
                if _authenticate(key_input):
                    st.success(f"Welcome, {st.session_state.user_id}!")
                    st.rerun()
                else:
                    st.error("Invalid access key. Contact your manager.")
        else:
            st.markdown(f"**Logged in as:** `{st.session_state.user_id}`")
            st.markdown("---")
            if st.button("🚪 Logout", use_container_width=True):
                _logout()
            st.markdown("---")
            with st.expander("📋 Activity Log", expanded=False):
                if st.session_state.activity_log:
                    for entry in reversed(st.session_state.activity_log[-50:]):
                        st.code(entry, language=None)
                else:
                    st.info("No activity yet this session.")

# ─────────────────────────────────────────────────────────────
# MAIN UI
# ─────────────────────────────────────────────────────────────
def render_main():
    st.title("🎬 InfinityContent Video Processor")
    st.caption(
        "Advanced obfuscation engine — "
        "Ghost Overlay · Frame Jittering · RGB Micro-Shifts · Deep Metadata Strip"
    )

    if not st.session_state.authenticated:
        st.info("👈 Please log in from the sidebar to access the processor.")
        st.stop()

    # ── Tool detection ─────────────────────────────────────────
    ff = _get_ffmpeg_binary()
    fp = _get_ffprobe_binary()

    parts = ["FFmpeg ✅" if ff else "FFmpeg ❌",
             "FFprobe ✅" if fp else "FFprobe ❌ (install via `brew install ffmpeg`)"]
    st.caption(" | ".join(parts))

    if not fp:
        st.error(
            "**FFprobe not found.** Advanced obfuscation requires FFprobe.\n\n"
            "Install it with: `brew install ffmpeg`  (includes ffprobe)"
        )
        st.stop()

    st.markdown("---")

    # ── Mode selector ──────────────────────────────────────────
    st.subheader("⚙️ Processing Mode")
    mode = st.radio(
        "mode",
        [
            "📁 Batch — 1 variation per file",
            "🔄 Multi Variation — N variations per file",
        ],
        label_visibility="collapsed",
        horizontal=True,
    )
    multi_mode = "Multi Variation" in mode
    n_variations = st.slider(
        "Variations per file", min_value=1, max_value=30, value=5
    ) if multi_mode else 1

    st.markdown("---")

    # ── Drive folders ──────────────────────────────────────────
    st.subheader("📂 Google Drive Folders")
    col_in, col_out = st.columns(2)
    with col_in:
        input_url = st.text_input(
            "Input Folder URL",
            placeholder="https://drive.google.com/drive/folders/…",
            help="Folder with raw videos/images to process.",
        )
    with col_out:
        upload_to_drive = st.toggle("Upload to Drive after processing", value=True)
        output_url = st.text_input(
            "Output Folder URL",
            placeholder="https://drive.google.com/drive/folders/…",
            disabled=not upload_to_drive,
        )

    # ── Ghost overlay folder ───────────────────────────────────
    st.subheader("👻 Ghost Overlay Videos (optional)")
    ghost_url = st.text_input(
        "Ghost Videos Folder URL",
        placeholder="https://drive.google.com/drive/folders/… (leave empty to skip)",
        help=(
            "Videos from this Drive folder will be used as invisible ghost overlays "
            "at 0.5–1% opacity. One random video is picked per output. "
            "Visually undetectable but changes every pixel's hash."
        ),
    )

    st.markdown("---")

    # ── Validate IDs ───────────────────────────────────────────
    input_id  = parse_folder_id(input_url)  if input_url  else None
    output_id = parse_folder_id(output_url) if (output_url and upload_to_drive) else None
    ghost_id  = parse_folder_id(ghost_url)  if ghost_url  else None

    col1, col2, col3 = st.columns(3)
    with col1:
        if input_url:
            st.success(f"Input: `{input_id}`") if input_id else st.error("Invalid Input URL")
    with col2:
        if upload_to_drive:
            if output_url:
                st.success(f"Output: `{output_id}`") if output_id else st.error("Invalid Output URL")
        else:
            st.info("Drive upload disabled.")
    with col3:
        if ghost_url:
            st.success(f"Ghost folder: `{ghost_id}`") if ghost_id else st.error("Invalid Ghost URL")
        else:
            st.info("No ghost folder — overlay skipped.")

    can_run = bool(input_id and (output_id if upload_to_drive else True))
    start = st.button(
        "▶ Start Processing",
        disabled=not can_run,
        type="primary",
        use_container_width=True,
    )

    _render_download_section()

    if not start:
        return

    # ════════════════════════════════════════════════════════════
    # PIPELINE
    # ════════════════════════════════════════════════════════════
    service = _get_drive_service()
    user_id = st.session_state.user_id
    st.session_state.processed_files = []

    # ── Step 0: List ghost videos (no download yet) ────────────
    ghost_files_list = []
    ghost_tmp_dir = None

    if ghost_id:
        with st.status("👻 Scanning ghost overlay folder…", expanded=False) as gs:
            try:
                ghost_files_list = list_video_files_only(service, ghost_id)
                if ghost_files_list:
                    ghost_tmp_dir = tempfile.mkdtemp()
                    gs.update(
                        label=f"✅ Found {len(ghost_files_list)} ghost video(s) — 1 random will be downloaded per output",
                        state="complete",
                    )
                else:
                    gs.update(label="⚠️ No videos found in ghost folder.", state="complete")
            except Exception as exc:
                gs.update(label=f"⚠️ Ghost scan failed: {exc}", state="error")

    # ── Step 1: List input files ───────────────────────────────
    with st.status("🔍 Scanning input folder…", expanded=True) as scan_status:
        try:
            files = list_media_files(service, input_id)
        except Exception as exc:
            st.error(f"Failed to list files: {exc}")
            log_activity(user_id, "N/A", f"ERROR listing folder — {exc}")
            if ghost_tmp_dir:
                shutil.rmtree(ghost_tmp_dir, ignore_errors=True)
            return

        if not files:
            st.warning("No video or image files found in the input folder.")
            scan_status.update(label="No media found.", state="complete")
            if ghost_tmp_dir:
                shutil.rmtree(ghost_tmp_dir, ignore_errors=True)
            return

        nv = sum(1 for f in files if "video/" in f.get("mimeType", ""))
        ni = sum(1 for f in files if "image/" in f.get("mimeType", ""))
        total_jobs = len(files) * n_variations
        st.write(
            f"Found **{len(files)}** file(s) — {nv} video(s), {ni} image(s) → "
            f"**{total_jobs}** output(s) to generate"
        )
        scan_status.update(
            label=f"Found {len(files)} file(s). Starting pipeline…", state="running"
        )

    # ── Step 2: Process ────────────────────────────────────────
    overall_bar = st.progress(0, text="Overall progress")
    all_results = []
    success_count = 0
    fail_count = 0
    job_idx = 0

    with tempfile.TemporaryDirectory() as tmp_dir:
        for file_meta in files:
            fname    = file_meta["name"]
            file_id  = file_meta["id"]
            mime     = file_meta.get("mimeType", "")
            is_video = "video/" in mime
            suffix   = Path(fname).suffix.lower() or (".mp4" if is_video else ".jpg")
            input_path = os.path.join(tmp_dir, f"input_{file_id}{suffix}")

            # Download once per source file
            with st.status(f"⬇ Downloading `{fname}`…", expanded=False) as dl_s:
                dl_bar = st.progress(0)
                try:
                    download_file(
                        service, file_id, input_path,
                        progress_cb=lambda frac: dl_bar.progress(frac),
                    )
                    dl_s.update(label=f"✅ Downloaded `{fname}`", state="complete")
                except Exception as exc:
                    dl_s.update(label=f"❌ Download failed: {fname}", state="error")
                    log_activity(user_id, fname, f"DOWNLOAD ERROR — {exc}")
                    fail_count += n_variations
                    job_idx    += n_variations
                    overall_bar.progress(
                        job_idx / total_jobs,
                        text=f"[{job_idx}/{total_jobs}] Skipped: {fname}",
                    )
                    continue

            # Generate N variations
            for var_i in range(n_variations):
                job_idx += 1
                var_label  = f" (var {var_i + 1}/{n_variations})" if n_variations > 1 else ""
                out_name   = random_filename(suffix)
                output_path = os.path.join(tmp_dir, out_name)

                with st.status(
                    f"⚙ Processing `{fname}`{var_label}…", expanded=False
                ) as proc_s:
                    # Download 1 random ghost video for this output only
                    ghost_local_paths = []
                    if ghost_files_list and ghost_tmp_dir and is_video:
                        chosen = random.choice(ghost_files_list)
                        ghost_path = os.path.join(ghost_tmp_dir, chosen["name"])
                        if not os.path.exists(ghost_path):
                            try:
                                download_file(service, chosen["id"], ghost_path)
                            except Exception:
                                ghost_path = None
                        if ghost_path and os.path.exists(ghost_path):
                            ghost_local_paths = [ghost_path]

                    if is_video:
                        ok, err, params = modify_video(
                            input_path, output_path, ff, fp, ghost_local_paths
                        )
                    else:
                        ok, err, params = modify_image(input_path, output_path, ff, fp)
                        params["ghost_overlay"] = "N/A"

                    if ok:
                        ghost_tag = params.get("ghost_overlay", "none")
                        proc_s.update(
                            label=(
                                f"✅ `{fname}`{var_label} → `{out_name}` "
                                f"[ghost: {ghost_tag}]"
                            ),
                            state="complete",
                        )
                    else:
                        proc_s.update(
                            label=f"❌ Failed: `{fname}`{var_label}", state="error"
                        )
                        st.error(f"`{fname}`{var_label} — {err or '(no output captured)'}")
                        log_activity(user_id, fname, f"PROCESS ERROR{var_label} — {err}")
                        fail_count += 1
                        overall_bar.progress(
                            job_idx / total_jobs,
                            text=f"[{job_idx}/{total_jobs}] Failed: {fname}",
                        )
                        if os.path.exists(output_path):
                            os.remove(output_path)
                        continue

                # Upload
                gdrive_url = ""
                if upload_to_drive and output_id:
                    with st.status(
                        f"⬆ Uploading `{out_name}`…", expanded=False
                    ) as up_s:
                        try:
                            gdrive_url = upload_file(
                                service, output_path, output_id, out_name
                            )
                            up_s.update(
                                label=f"✅ Uploaded `{out_name}`", state="complete"
                            )
                            log_activity(
                                user_id, out_name,
                                f"SUCCESS — uploaded to Drive "
                                f"(ghost: {params.get('ghost_overlay', 'none')})",
                            )
                        except Exception as exc:
                            up_s.update(
                                label=f"⚠ Upload failed: `{out_name}`", state="error"
                            )
                            st.error(
                                f"❌ Upload failed for `{out_name}`: "
                                f"`{str(exc)[:200]}`"
                            )
                            log_activity(user_id, out_name, f"UPLOAD ERROR — {exc}")
                else:
                    log_activity(
                        user_id, out_name,
                        f"SUCCESS — processed (Drive upload skipped)",
                    )

                # Cache for local download
                try:
                    with open(output_path, "rb") as fh:
                        file_bytes = fh.read()
                    st.session_state.processed_files.append({
                        "name": out_name,
                        "bytes": file_bytes,
                        "gdrive_url": gdrive_url,
                    })
                except Exception as exc:
                    log_activity(user_id, out_name, f"READ ERROR — {exc}")

                params["original"] = fname
                params["output"]   = out_name
                all_results.append(params)

                if os.path.exists(output_path):
                    os.remove(output_path)

                success_count += 1
                overall_bar.progress(
                    job_idx / total_jobs,
                    text=f"[{job_idx}/{total_jobs}] Done: {out_name}",
                )

            if os.path.exists(input_path):
                os.remove(input_path)

    if ghost_tmp_dir:
        shutil.rmtree(ghost_tmp_dir, ignore_errors=True)

    # ── Summary ────────────────────────────────────────────────
    st.markdown("---")
    col_ok, col_fail, col_ghost = st.columns(3)
    col_ok.metric("✅ Successful", success_count)
    col_fail.metric("❌ Failed", fail_count)
    ghost_count = sum(
        1 for r in all_results
        if r.get("ghost_overlay") not in ["none", "fallback", "N/A", ""]
    )
    col_ghost.metric("👻 Ghost Overlay Applied", ghost_count)

    if success_count:
        st.balloons()

    _render_download_section()


def _render_download_section():
    files = st.session_state.get("processed_files", [])
    if not files:
        return

    st.markdown("---")
    st.subheader("📥 Download Processed Files")
    st.caption("Files are cached in memory for this session only.")

    for idx, item in enumerate(files):
        col_name, col_drive, col_dl = st.columns([3, 2, 2])
        col_name.write(f"**{item['name']}**")
        if item.get("gdrive_url"):
            col_drive.markdown(f"[☁ View on Drive]({item['gdrive_url']})")
        else:
            col_drive.write("—")
        col_dl.download_button(
            label="⬇ Download",
            data=item["bytes"],
            file_name=item["name"],
            mime="video/mp4",
            key=f"dl_{idx}_{item['name']}",
        )


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
render_sidebar()
render_main()
