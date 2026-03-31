import os
import io
import time
import json
import requests
import google.auth

from datetime import date, datetime
from typing import Dict, Optional, List, Tuple
from urllib.parse import quote

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import storage
from google.oauth2.credentials import Credentials


# -----------------------------
# Config
# -----------------------------
BUCKET_NAME = os.getenv("BUCKET_NAME", "deep-dive-podcast-assets")
VIDEO_OBJECT_PREFIX = (os.getenv("VIDEO_OBJECT_PREFIX", "episodes/").strip("/") + "/")

SHEET_ID = os.getenv("SHEET_ID") or os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Main Schedule")

DATE_OVERRIDE = os.getenv("DATE_OVERRIDE")  # YYYY-MM-DD (optional)

# Facebook Page
META_PAGE_ID = os.getenv("META_PAGE_ID")
META_PAGE_ACCESS_TOKEN = os.getenv("META_PAGE_ACCESS_TOKEN")
FB_MODE = os.getenv("FB_MODE", "native").strip().lower()  # native | link

# YouTube OAuth (user creds w/ refresh token)
YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")
YOUTUBE_PRIVACY_STATUS = os.getenv("YOUTUBE_PRIVACY_STATUS", "public").strip().lower()  # public|unlisted|private


# -----------------------------
# Auth (ADC for Sheets + Storage)
# -----------------------------
def get_adc_creds():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/devstorage.read_only",
    ]
    creds, _ = google.auth.default(scopes=scopes)
    return creds


# -----------------------------
# Sheet helpers
# -----------------------------
def parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def col_to_a1(col_index_0_based: int) -> str:
    n = col_index_0_based
    letters = ""
    while True:
        n, rem = divmod(n, 26)
        letters = chr(ord("A") + rem) + letters
        if n == 0:
            break
        n -= 1
    return letters


def pick_existing_header(col_index: Dict[str, int], *candidates: str) -> str:
    for c in candidates:
        if c in col_index:
            return c
    raise KeyError(f"Missing required column. Tried: {candidates}. Found: {list(col_index.keys())}")


def try_header(col_index: Dict[str, int], *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in col_index:
            return c
    return None


# -----------------------------
# GCS helpers
# -----------------------------
def gcs_public_url(bucket: str, object_name: str) -> str:
    return f"https://storage.googleapis.com/{bucket}/{quote(object_name, safe='/')}"


def derive_video_object(publish_date_iso: str, audio_filename: str) -> str:
    base = audio_filename.rsplit(".", 1)[0]
    video_filename = f"{base}.mp4"
    return f"{VIDEO_OBJECT_PREFIX}{publish_date_iso}/{video_filename}"


# -----------------------------
# YouTube upload
# -----------------------------
def youtube_client():
    if not (YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET and YOUTUBE_REFRESH_TOKEN):
        raise RuntimeError("Missing YouTube OAuth env vars (YOUTUBE_CLIENT_ID/SECRET/REFRESH_TOKEN).")

    creds = Credentials(
        token=None,
        refresh_token=YOUTUBE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YOUTUBE_CLIENT_ID,
        client_secret=YOUTUBE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/youtube.upload"],
    )
    return build("youtube", "v3", credentials=creds)


def upload_to_youtube(yt, video_path: str, title: str, description: str, privacy_status: str) -> str:
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "22",  # People & Blogs
        },
        "status": {
            "privacyStatus": privacy_status,
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True)
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)

    resp = None
    while resp is None:
        _, resp = req.next_chunk()

    video_id = resp["id"]
    return f"https://www.youtube.com/watch?v={video_id}"


# -----------------------------
# Facebook helpers
# -----------------------------
def _fb_log_response(prefix: str, response: requests.Response):
    print(f"{prefix}: status={response.status_code}")
    try:
        print(f"{prefix}: body={response.text}")
    except Exception:
        pass


def _fb_raise_for_status(response: requests.Response, context: str):
    if response.ok:
        return
    _fb_log_response(context, response)
    response.raise_for_status()


# -----------------------------
# Facebook Page native upload
# Resumable/chunked upload
# -----------------------------
def fb_upload_video_native(message: str, video_file_path: str) -> str:
    if not META_PAGE_ID or not META_PAGE_ACCESS_TOKEN:
        raise RuntimeError("Missing META_PAGE_ID / META_PAGE_ACCESS_TOKEN.")

    if not os.path.exists(video_file_path):
        raise RuntimeError(f"Video file not found: {video_file_path}")

    file_size = os.path.getsize(video_file_path)
    print(f"[FB] Preparing native upload: {video_file_path}")
    print(f"[FB] File size: {file_size} bytes ({file_size / (1024 * 1024):.2f} MB)")

    url = f"https://graph-video.facebook.com/v19.0/{META_PAGE_ID}/videos"
    session = requests.Session()

    # Phase 1: start
    start_resp = session.post(
        url,
        data={
            "access_token": META_PAGE_ACCESS_TOKEN,
            "upload_phase": "start",
            "file_size": str(file_size),
        },
        timeout=120,
    )
    _fb_raise_for_status(start_resp, "[FB] start phase failed")

    start_json = start_resp.json()
    upload_session_id = start_json["upload_session_id"]
    video_id = start_json["video_id"]
    start_offset = int(start_json["start_offset"])
    end_offset = int(start_json["end_offset"])

    print(f"[FB] upload_session_id={upload_session_id}")
    print(f"[FB] video_id={video_id}")
    print(f"[FB] initial offsets: start={start_offset}, end={end_offset}")

    # Phase 2: transfer
    retry_budget = 5

    with open(video_file_path, "rb") as f:
        while True:
            if start_offset == end_offset:
                break

            chunk_size = end_offset - start_offset
            f.seek(start_offset)
            chunk = f.read(chunk_size)

            if not chunk:
                raise RuntimeError(
                    f"[FB] Failed to read chunk for upload. start_offset={start_offset}, end_offset={end_offset}"
                )

            transfer_resp = session.post(
                url,
                data={
                    "access_token": META_PAGE_ACCESS_TOKEN,
                    "upload_phase": "transfer",
                    "upload_session_id": upload_session_id,
                    "start_offset": str(start_offset),
                },
                files={
                    "video_file_chunk": (
                        os.path.basename(video_file_path),
                        chunk,
                        "application/octet-stream",
                    )
                },
                timeout=300,
            )

            if transfer_resp.ok:
                transfer_json = transfer_resp.json()
                start_offset = int(transfer_json["start_offset"])
                end_offset = int(transfer_json["end_offset"])
                print(f"[FB] transferred chunk, next offsets: start={start_offset}, end={end_offset}")
                continue

            _fb_log_response("[FB] transfer phase failed", transfer_resp)

            error_json = {}
            try:
                error_json = transfer_resp.json()
            except Exception:
                error_json = {}

            error = error_json.get("error", {}) if isinstance(error_json, dict) else {}
            error_data = error.get("error_data", {}) if isinstance(error, dict) else {}

            corrected_start = error_data.get("start_offset")
            corrected_end = error_data.get("end_offset")
            is_transient = bool(error.get("is_transient"))

            if corrected_start is not None and corrected_end is not None and retry_budget > 0:
                start_offset = int(corrected_start)
                end_offset = int(corrected_end)
                retry_budget -= 1
                print(f"[FB] corrected offsets from API, retrying: start={start_offset}, end={end_offset}")
                continue

            if is_transient and retry_budget > 0:
                retry_budget -= 1
                print("[FB] transient transfer error, sleeping 2 seconds and retrying")
                time.sleep(2)
                continue

            transfer_resp.raise_for_status()

    # Phase 3: finish
    finish_resp = session.post(
        url,
        data={
            "access_token": META_PAGE_ACCESS_TOKEN,
            "upload_phase": "finish",
            "upload_session_id": upload_session_id,
            "description": message,
            "published": "true",
        },
        timeout=120,
    )
    _fb_raise_for_status(finish_resp, "[FB] finish phase failed")

    finish_json = finish_resp.json()
    print(f"[FB] finish response: {finish_json}")

    return finish_json.get("id") or video_id


def fb_post_link(message: str, link: str) -> str:
    if not META_PAGE_ID or not META_PAGE_ACCESS_TOKEN:
        raise RuntimeError("Missing META_PAGE_ID / META_PAGE_ACCESS_TOKEN.")

    url = f"https://graph.facebook.com/v19.0/{META_PAGE_ID}/feed"
    payload = {
        "message": message,
        "link": link,
        "access_token": META_PAGE_ACCESS_TOKEN,
    }
    r = requests.post(url, data=payload, timeout=45)
    if not r.ok:
        print("FB link post failed:", r.status_code)
        try:
            print("FB response text:", r.text)
        except Exception:
            pass
        r.raise_for_status()
    return r.json().get("id", "")


# -----------------------------
# Main
# -----------------------------
def main():
    if not SHEET_ID:
        raise RuntimeError("Missing SHEET_ID / SPREADSHEET_ID.")
    if not META_PAGE_ID or not META_PAGE_ACCESS_TOKEN:
        raise RuntimeError("Missing META_PAGE_ID / META_PAGE_ACCESS_TOKEN.")

    today = parse_date(DATE_OVERRIDE) if DATE_OVERRIDE else date.today()
    today_iso = today.isoformat()

    adc_creds = get_adc_creds()
    sheets = build("sheets", "v4", credentials=adc_creds)
    storage_client = storage.Client(credentials=adc_creds)
    bucket = storage_client.bucket(BUCKET_NAME)

    resp = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_NAME}'!A1:Z"
    ).execute()

    values = resp.get("values", [])
    if not values:
        print("Sheet returned no data.")
        return

    headers = values[0]
    col_index = {h.strip(): i for i, h in enumerate(headers)}

    COL_PUBLISH = pick_existing_header(col_index, "Publish Date", "PublishDate", "publish_date")
    COL_TITLE = pick_existing_header(col_index, "Title", "title")
    COL_DESC = pick_existing_header(col_index, "Description", "description")
    COL_FILE = pick_existing_header(col_index, "File Name", "FileName", "Filename", "file_name")
    COL_PROCESSED = pick_existing_header(col_index, "Processed", "processed", "Status", "status")

    COL_SOCIAL_POST = try_header(col_index, "SocialPost", "Social Post", "social_post")
    COL_VIDEO_URL = try_header(col_index, "VideoURL", "Video Url", "video_url")
    COL_YT_URL = try_header(col_index, "YouTubeURL", "YoutubeURL", "youtube_url", "YouTube Url")
    COL_FB_ID = try_header(col_index, "FacebookPostId", "Facebook Post Id", "facebook_post_id")
    COL_SOCIAL_PUBLISHED = try_header(col_index, "SocialPublished", "Social Published", "social_published")

    candidates: List[Tuple[int, List[str]]] = []
    for r in range(1, len(values)):
        row = values[r] + [""] * (len(headers) - len(values[r]))

        publish_raw = (row[col_index[COL_PUBLISH]] or "").strip()
        if publish_raw != today_iso:
            continue

        processed_val = (row[col_index[COL_PROCESSED]] or "").strip().lower()
        is_processed = processed_val in ("yes", "y", "true", "1", "processed", "done")
        if not is_processed:
            continue

        if COL_SOCIAL_PUBLISHED:
            published_val = (row[col_index[COL_SOCIAL_PUBLISHED]] or "").strip().lower()
            if published_val in ("yes", "y", "true", "1", "published", "done"):
                continue

        candidates.append((r, row))

    if not candidates:
        print(f"No rows to publish for {today_iso}.")
        return

    yt = None
    yt_enabled = all([YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN])
    if yt_enabled:
        yt = youtube_client()
    else:
        print("YouTube vars not set; skipping YouTube upload.")

    os.makedirs("/tmp/dd", exist_ok=True)

    for (row_idx, row) in candidates:
        title = (row[col_index[COL_TITLE]] or "").strip()
        desc = (row[col_index[COL_DESC]] or "").strip()
        audio_filename = (row[col_index[COL_FILE]] or "").strip()

        if not title or not audio_filename:
            print(f"Skipping row {row_idx + 1}: missing title or file name.")
            continue

        video_object = derive_video_object(today_iso, audio_filename)
        blob = bucket.blob(video_object)
        if not blob.exists(storage_client):
            raise RuntimeError(f"Expected video not found: gs://{BUCKET_NAME}/{video_object}")

        local_video = f"/tmp/dd/{row_idx + 1}.mp4"
        blob.download_to_filename(local_video)

        local_size = os.path.getsize(local_video)
        print(f"Downloaded video for row {row_idx + 1}: {local_video} ({local_size / (1024 * 1024):.2f} MB)")

        gcs_video_url = gcs_public_url(BUCKET_NAME, video_object)

        yt_url = ""
        if yt_enabled and yt:
            yt_url = upload_to_youtube(
                yt=yt,
                video_path=local_video,
                title=title,
                description=desc,
                privacy_status=YOUTUBE_PRIVACY_STATUS
            )

        if yt_url:
            social_post = f"{title}\n\nWatch on YouTube: {yt_url}"
        else:
            social_post = f"{title}\n\nNew video is up!"

        if FB_MODE == "native":
            try:
                fb_id = fb_upload_video_native(message=social_post, video_file_path=local_video)
            except Exception as e:
                print(f"[FB] Native upload failed, falling back to link mode: {e}")
                fb_link = yt_url or gcs_video_url
                fb_id = fb_post_link(message=social_post, link=fb_link)
        else:
            fb_link = yt_url or gcs_video_url
            fb_id = fb_post_link(message=social_post, link=fb_link)

        def update_cell(col_name: str, value: str):
            col_letter = col_to_a1(col_index[col_name])
            a1 = f"'{SHEET_NAME}'!{col_letter}{row_idx + 1}"
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=a1,
                valueInputOption="RAW",
                body={"values": [[value]]}
            ).execute()

        if COL_VIDEO_URL:
            update_cell(COL_VIDEO_URL, gcs_video_url)
        if COL_YT_URL and yt_url:
            update_cell(COL_YT_URL, yt_url)
        if COL_SOCIAL_POST and social_post:
            update_cell(COL_SOCIAL_POST, social_post)
        if COL_FB_ID and fb_id:
            update_cell(COL_FB_ID, fb_id)
        if COL_SOCIAL_PUBLISHED:
            update_cell(COL_SOCIAL_PUBLISHED, "yes")

        print(f"Published row {row_idx + 1}: {title}")
        print("  GCS:", gcs_video_url)
        if yt_url:
            print("  YouTube:", yt_url)
        print("  FB id:", fb_id)


if __name__ == "__main__":
    main()
