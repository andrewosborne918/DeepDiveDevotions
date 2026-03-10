import os
import io
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

# Facebook Page (native upload)
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
    # URL-encode the object path (spaces, etc.)
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
# Facebook Page native upload
# -----------------------------
def fb_upload_video_native(message: str, video_file_path: str) -> str:
    if not META_PAGE_ID or not META_PAGE_ACCESS_TOKEN:
        raise RuntimeError("Missing META_PAGE_ID / META_PAGE_ACCESS_TOKEN.")

    url = f"https://graph.facebook.com/v19.0/{META_PAGE_ID}/videos"
    with open(video_file_path, "rb") as f:
        files = {"source": f}
        data = {
            "description": message,
            "published": "true",
            "access_token": META_PAGE_ACCESS_TOKEN,
        }
        r = requests.post(url, data=data, files=files, timeout=300)
        if not r.ok:
            print("FB upload failed:", r.status_code)
            try:
                print("FB response text:", r.text)
            except Exception:
                pass
            r.raise_for_status()
        return r.json().get("id", "")


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
        print("FB upload failed:", r.status_code)
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

    # Read sheet
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

    # Required inputs
    COL_PUBLISH = pick_existing_header(col_index, "Publish Date", "PublishDate", "publish_date")
    COL_TITLE = pick_existing_header(col_index, "Title", "title")
    COL_DESC = pick_existing_header(col_index, "Description", "description")
    COL_FILE = pick_existing_header(col_index, "File Name", "FileName", "Filename", "file_name")
    COL_PROCESSED = pick_existing_header(col_index, "Processed", "processed", "Status", "status")

    # Outputs (recommended columns)
    COL_SOCIAL_POST = try_header(col_index, "SocialPost", "Social Post", "social_post")
    COL_VIDEO_URL = try_header(col_index, "VideoURL", "Video Url", "video_url")
    COL_YT_URL = try_header(col_index, "YouTubeURL", "YoutubeURL", "youtube_url", "YouTube Url")
    COL_FB_ID = try_header(col_index, "FacebookPostId", "Facebook Post Id", "facebook_post_id")
    COL_SOCIAL_PUBLISHED = try_header(col_index, "SocialPublished", "Social Published", "social_published")

    # Find all rows for today that are processed and not social-published yet
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

    # YouTube client (optional: only if vars exist)
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
            print(f"Skipping row {row_idx+1}: missing title or file name.")
            continue

        video_object = derive_video_object(today_iso, audio_filename)
        blob = bucket.blob(video_object)
        if not blob.exists(storage_client):
            raise RuntimeError(f"Expected video not found: gs://{BUCKET_NAME}/{video_object}")

        local_video = f"/tmp/dd/{row_idx+1}.mp4"
        blob.download_to_filename(local_video)

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

        # Social post text (simple; customize later)
        if yt_url:
            social_post = f"{title}\n\nWatch on YouTube: {yt_url}"
        else:
            social_post = f"{title}\n\nNew video is up!"

        # Facebook publish
        if FB_MODE == "native":
            fb_id = fb_upload_video_native(message=social_post, video_file_path=local_video)
        else:
            # link mode posts the YouTube link if available, otherwise the GCS public URL
            fb_link = yt_url or gcs_video_url
            fb_id = fb_post_link(message=social_post, link=fb_link)

        # Update sheet cells (if columns exist)
        def update_cell(col_name: str, value: str):
            col_letter = col_to_a1(col_index[col_name])
            a1 = f"'{SHEET_NAME}'!{col_letter}{row_idx+1}"
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

        print(f"Published row {row_idx+1}: {title}")
        print("  GCS:", gcs_video_url)
        if yt_url:
            print("  YouTube:", yt_url)
        print("  FB id:", fb_id)


if __name__ == "__main__":
    main()
