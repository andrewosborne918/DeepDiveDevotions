import os
import io
import subprocess
import google.auth
from datetime import datetime, date, timezone
from email.utils import format_datetime
import xml.etree.ElementTree as ET

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage


# -----------------------------
# Environment / Config
# -----------------------------
EPISODES_FOLDER_ID = os.getenv("DAILY_FOLDER_ID") or os.getenv("EPISODES_FOLDER_ID")
BOOK_INTRO_FOLDER_ID = os.getenv("BOOK_INTRO_FOLDER_ID")  # optional (not used in this script yet)

BUCKET_NAME = os.getenv("RSS_BUCKET") or os.getenv("BUCKET_NAME", "deep-dive-podcast-assets")
RSS_BLOB_NAME = os.getenv("RSS_OBJECT") or os.getenv("RSS_BLOB_NAME", "rss.xml")
BUCKET_EPISODES_PREFIX = os.getenv("BUCKET_EPISODES_PREFIX", "episodes")

SPREADSHEET_ID = os.getenv("SHEET_ID") or os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Main Schedule")

# Optional: override "today" for testing: set DATE_OVERRIDE=YYYY-MM-DD in Cloud Run env vars
DATE_OVERRIDE = os.getenv("DATE_OVERRIDE")


# -----------------------------
# Auth (ADC via Cloud Run Job service account)
# -----------------------------
def get_creds():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]
    creds, _ = google.auth.default(scopes=scopes)
    return creds


# -----------------------------
# Helpers
# -----------------------------
def parse_publish_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def col_to_a1(col_index_0_based: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA, etc."""
    n = col_index_0_based
    letters = ""
    while True:
        n, rem = divmod(n, 26)
        letters = chr(ord("A") + rem) + letters
        if n == 0:
            break
        n -= 1
    return letters


def drive_find_file_id(drive_svc, parent_folder_id: str, filename: str) -> str:
    # Drive query escaping uses doubled single quotes
    safe_name = filename.replace("'", "''")
    q = f"name = '{safe_name}' and '{parent_folder_id}' in parents and trashed = false"
    res = drive_svc.files().list(q=q, fields="files(id,name)", pageSize=1).execute()
    files = res.get("files", [])
    if not files:
        raise FileNotFoundError(f"Drive file not found in folder {parent_folder_id}: {filename}")
    return files[0]["id"]


def drive_download_file(drive_svc, file_id: str, out_path: str):
    request = drive_svc.files().get_media(fileId=file_id)
    with io.FileIO(out_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def run_ffmpeg(image_path: str, audio_path: str, out_video_path: str):
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", image_path,
        "-i", audio_path,
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-c:a", "aac",
        "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-shortest",
        out_video_path
    ]
    subprocess.run(cmd, check=True)


def pick_existing_header(col_index: dict, *candidates: str) -> str:
    """Return the first candidate header that exists in the header map."""
    for c in candidates:
        if c in col_index:
            return c
    raise KeyError(f"Missing required column. Tried: {candidates}. Found: {list(col_index.keys())}")


def add_item_to_channel_newest_first(channel: ET.Element, item: ET.Element):
    """Insert item before the first existing <item>, otherwise append. Keeps channel metadata intact."""
    items = channel.findall("item")
    if items:
        channel.insert(list(channel).index(items[0]), item)
    else:
        channel.append(item)


# -----------------------------
# Main
# -----------------------------
def main():
    if not EPISODES_FOLDER_ID:
        raise RuntimeError("Missing EPISODES folder id. Set DAILY_FOLDER_ID (or EPISODES_FOLDER_ID).")
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing spreadsheet id. Set SHEET_ID (or SPREADSHEET_ID).")
    if not BUCKET_NAME:
        raise RuntimeError("Missing bucket name. Set RSS_BUCKET (or BUCKET_NAME).")

    if DATE_OVERRIDE:
        today = parse_publish_date(DATE_OVERRIDE)
    else:
        today = date.today()

    creds = get_creds()
    sheets_svc = build("sheets", "v4", credentials=creds)
    drive_svc = build("drive", "v3", credentials=creds)

    storage_client = storage.Client(credentials=creds)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Read sheet
    resp = sheets_svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1:Z"
    ).execute()

    values = resp.get("values", [])
    if not values:
        print("Sheet returned no data.")
        return

    headers = values[0]
    col_index = {h.strip(): i for i, h in enumerate(headers)}

    # Column names (tolerant to variants)
    COL_PUBLISH = pick_existing_header(col_index, "Publish Date", "PublishDate", "publish_date")
    COL_TITLE = pick_existing_header(col_index, "Title", "title")
    COL_DESC = pick_existing_header(col_index, "Description", "description")
    COL_FILE = pick_existing_header(col_index, "File Name", "FileName", "Filename", "file_name")
    COL_PROCESSED = pick_existing_header(col_index, "Processed", "processed", "Status", "status")

    COL_IMAGE_16x9 = pick_existing_header(
        col_index,
        "Image16x9FileId",
        "Image16x9FileID",
        "Image16x9FileIdField",
        "Image16x9FileIdFiled",
        "Image16x9FileIdFieldId",
        "Image16x9FileIdFieldID",
    )

    # Find today's unprocessed row
    target_row = None
    row_data = None

    for r in range(1, len(values)):
        row = values[r] + [""] * (len(headers) - len(values[r]))

        publish_raw = row[col_index[COL_PUBLISH]].strip() if row[col_index[COL_PUBLISH]] else ""
        if not publish_raw:
            continue

        try:
            pub = parse_publish_date(publish_raw)
        except ValueError:
            continue

        processed_val = (row[col_index[COL_PROCESSED]] or "").strip().lower()
        is_processed = processed_val in ("yes", "y", "true", "1", "processed", "done")

        if pub == today and not is_processed:
            target_row = r
            row_data = row
            break

    if target_row is None:
        print(f"No episode scheduled for {today.isoformat()} (or it is already processed).")
        return

    title = (row_data[col_index[COL_TITLE]] or "").strip()
    description = (row_data[col_index[COL_DESC]] or "").strip()
    filename = (row_data[col_index[COL_FILE]] or "").strip()
    image16x9_id = (row_data[col_index[COL_IMAGE_16x9]] or "").strip()

    if not title or not filename:
        raise RuntimeError("Row is missing Title or File Name.")
    if not image16x9_id:
        raise RuntimeError("Row is missing Image16x9FileId (needed to render video).")

    # Local temp files
    os.makedirs("/tmp/dd", exist_ok=True)
    local_audio = "/tmp/dd/audio.m4a"
    local_image = "/tmp/dd/image.png"
    local_video = "/tmp/dd/output.mp4"

    # Download from Drive
    audio_file_id = drive_find_file_id(drive_svc, EPISODES_FOLDER_ID, filename)
    drive_download_file(drive_svc, audio_file_id, local_audio)
    drive_download_file(drive_svc, image16x9_id, local_image)

    # Render video
    run_ffmpeg(local_image, local_audio, local_video)

    # Upload artifacts to bucket
    audio_blob_name = f"{BUCKET_EPISODES_PREFIX}/{today.isoformat()}/{filename}"
    video_filename = filename.rsplit(".", 1)[0] + ".mp4"   # "1 Genesis 1.mp4"
    video_blob_name = f"{BUCKET_EPISODES_PREFIX}/{today.isoformat()}/{video_filename}"

    blob_audio = bucket.blob(audio_blob_name)
    blob_audio.upload_from_filename(local_audio, content_type="audio/x-m4a")
    try:
        blob_audio.make_public()
    except Exception:
        pass

    blob_video = bucket.blob(video_blob_name)
    blob_video.upload_from_filename(local_video, content_type="video/mp4")
    try:
        blob_video.make_public()
    except Exception:
        pass

    # Update RSS
    rss_blob = bucket.blob(RSS_BLOB_NAME)
    rss_xml = rss_blob.download_as_text()

    root = ET.fromstring(rss_xml)
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("rss.xml is missing <channel> element.")

    item = ET.Element("item")
    ET.SubElement(item, "title").text = title
    ET.SubElement(item, "description").text = description
    ET.SubElement(item, "pubDate").text = format_datetime(datetime.now(timezone.utc))

    enclosure = ET.SubElement(item, "enclosure")
    enclosure.set("url", blob_audio.public_url)
    enclosure.set("length", str(os.path.getsize(local_audio)))
    enclosure.set("type", "audio/x-m4a")

    guid = ET.SubElement(item, "guid")
    guid.set("isPermaLink", "false")
    guid.text = f"dddevotion-{today.isoformat()}-{filename}"

    add_item_to_channel_newest_first(channel, item)

    updated_xml = ET.tostring(root, encoding="unicode")
    rss_blob.upload_from_string(updated_xml, content_type="application/xml")
    try:
        rss_blob.make_public()
    except Exception:
        pass

    # Mark only the Processed column as "yes"
    processed_col_letter = col_to_a1(col_index[COL_PROCESSED])
    sheets_svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!{processed_col_letter}{target_row+1}",
        valueInputOption="RAW",
        body={"values": [["yes"]]}
    ).execute()

    print("Success:", title)
    print("Audio URL:", blob_audio.public_url)
    print("Video URL:", blob_video.public_url)
    print("RSS URL:", f"https://storage.googleapis.com/{BUCKET_NAME}/{RSS_BLOB_NAME}")


if __name__ == "__main__":
    main()
