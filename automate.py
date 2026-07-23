import os
import io
import time
import subprocess
import google.auth
from datetime import datetime, date, timezone
from email.utils import format_datetime
import xml.etree.ElementTree as ET

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage


# -----------------------------
# Environment / Config
# -----------------------------
EPISODES_FOLDER_ID = (
    os.getenv("DAILY_FOLDER_ID")
    or os.getenv("EPISODES_FOLDER_ID")
)

BOOK_INTRO_FOLDER_ID = os.getenv("BOOK_INTRO_FOLDER_ID")

THUMBNAILS_FOLDER_ID = os.getenv("THUMBNAILS_FOLDER_ID")
FALLBACK_IMAGE_FILE_ID = os.getenv("FALLBACK_IMAGE_FILE_ID")

BUCKET_NAME = (
    os.getenv("RSS_BUCKET")
    or os.getenv("BUCKET_NAME", "deep-dive-podcast-assets")
)

RSS_BLOB_NAME = (
    os.getenv("RSS_OBJECT")
    or os.getenv("RSS_BLOB_NAME", "rss.xml")
)

BUCKET_EPISODES_PREFIX = os.getenv(
    "BUCKET_EPISODES_PREFIX",
    "episodes",
)

SPREADSHEET_ID = (
    os.getenv("SHEET_ID")
    or os.getenv("SPREADSHEET_ID")
)

SHEET_NAME = os.getenv("SHEET_NAME", "Main Schedule")

# Optional override for backfills:
# DATE_OVERRIDE=YYYY-MM-DD
DATE_OVERRIDE = os.getenv("DATE_OVERRIDE")


# -----------------------------
# Authentication
# -----------------------------
def get_creds():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]

    creds, _ = google.auth.default(scopes=scopes)
    return creds


def build_google_service(service_name: str, version: str, creds):
    """
    Build a Google API client with discovery caching disabled.

    Disabling discovery caching is safer in ephemeral Cloud Run
    containers and avoids unnecessary cache-related behavior.
    """
    return build(
        service_name,
        version,
        credentials=creds,
        cache_discovery=False,
    )


# -----------------------------
# General helpers
# -----------------------------
def parse_publish_date(value: str) -> date:
    return datetime.strptime(
        value.strip(),
        "%Y-%m-%d",
    ).date()


def col_to_a1(col_index_0_based: int) -> str:
    """
    Convert a zero-based column number to a Sheets column name.

    Examples:
    0  -> A
    25 -> Z
    26 -> AA
    """
    n = col_index_0_based
    letters = ""

    while True:
        n, remainder = divmod(n, 26)
        letters = chr(ord("A") + remainder) + letters

        if n == 0:
            break

        n -= 1

    return letters


def pick_existing_header(
    col_index: dict,
    *candidates: str,
) -> str:
    """
    Return the first matching spreadsheet header.
    """
    for candidate in candidates:
        if candidate in col_index:
            return candidate

    raise KeyError(
        "Missing required column. "
        f"Tried: {candidates}. "
        f"Found: {list(col_index.keys())}"
    )


def is_processed_value(value: str) -> bool:
    return value.strip().lower() in (
        "yes",
        "y",
        "true",
        "1",
        "processed",
        "done",
    )


# -----------------------------
# Google Drive helpers
# -----------------------------
def drive_find_file_id(
    drive_svc,
    parent_folder_id: str,
    filename: str,
) -> str:
    safe_name = filename.replace("'", "''")

    query = (
        f"name = '{safe_name}' "
        f"and '{parent_folder_id}' in parents "
        "and trashed = false"
    )

    result = (
        drive_svc.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=1,
        )
        .execute(num_retries=5)
    )

    files = result.get("files", [])

    if not files:
        raise FileNotFoundError(
            "Drive file not found in folder "
            f"{parent_folder_id}: {filename}"
        )

    return files[0]["id"]


def drive_find_file_id_optional(
    drive_svc,
    parent_folder_id: str,
    filename: str,
):
    if not parent_folder_id or not filename:
        return None

    safe_name = filename.replace("'", "''")

    query = (
        f"name = '{safe_name}' "
        f"and '{parent_folder_id}' in parents "
        "and trashed = false"
    )

    result = (
        drive_svc.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=1,
        )
        .execute(num_retries=5)
    )

    files = result.get("files", [])

    if not files:
        return None

    return files[0]["id"]


def drive_download_file(
    drive_svc,
    file_id: str,
    out_path: str,
):
    request = drive_svc.files().get_media(
        fileId=file_id,
    )

    with io.FileIO(out_path, "wb") as output_file:
        downloader = MediaIoBaseDownload(
            output_file,
            request,
        )

        done = False

        while not done:
            status, done = downloader.next_chunk(
                num_retries=5,
            )

            if status:
                percentage = int(
                    status.progress() * 100
                )
                print(
                    f"Drive download progress: "
                    f"{percentage}%"
                )


def resolve_thumbnail_file_id(
    drive_svc,
    thumbnails_folder_id: str,
    thumbnail_filename: str,
    image16x9_id: str,
    fallback_image_file_id: str,
) -> str:
    # First try the exact thumbnail filename.
    if thumbnails_folder_id and thumbnail_filename:
        matched_id = drive_find_file_id_optional(
            drive_svc,
            thumbnails_folder_id,
            thumbnail_filename,
        )

        if matched_id:
            print(
                "Using thumbnail from Drive folder match: "
                f"{thumbnail_filename}"
            )
            return matched_id

        print(
            "No Drive thumbnail match found for: "
            f"{thumbnail_filename}"
        )

    # Then try the image file ID stored in the sheet.
    if image16x9_id:
        print("Using Image16x9FileId from sheet.")
        return image16x9_id

    # Finally use the configured fallback image.
    if fallback_image_file_id:
        print("Using fallback image file ID.")
        return fallback_image_file_id

    raise RuntimeError(
        "No thumbnail source available. "
        "Checked Thumbnail folder match, "
        "Image16x9FileId, and "
        "FALLBACK_IMAGE_FILE_ID."
    )


# -----------------------------
# Video rendering
# -----------------------------
def run_ffmpeg(
    image_path: str,
    audio_path: str,
    out_video_path: str,
):
    command = [
        "ffmpeg",
        "-y",
        "-loop",
        "1",
        "-i",
        image_path,
        "-i",
        audio_path,
        "-vf",
        "pad=ceil(iw/2)*2:ceil(ih/2)*2",
        "-c:v",
        "libx264",
        "-tune",
        "stillimage",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-shortest",
        out_video_path,
    ]

    print("Starting video render.")

    subprocess.run(
        command,
        check=True,
    )

    print(
        "Rendered video to "
        f"{out_video_path}"
    )


# -----------------------------
# RSS helpers
# -----------------------------
def add_item_to_channel_newest_first(
    channel: ET.Element,
    item: ET.Element,
):
    """
    Insert the item before the first existing RSS item while
    preserving the channel metadata above it.
    """
    items = channel.findall("item")

    if items:
        first_item_index = list(channel).index(
            items[0]
        )
        channel.insert(
            first_item_index,
            item,
        )
    else:
        channel.append(item)


def get_item_guid(item: ET.Element):
    guid_element = item.find("guid")

    if guid_element is None:
        return None

    return guid_element.text


def find_items_by_guid(
    channel: ET.Element,
    guid_value: str,
):
    matches = []

    for existing_item in channel.findall("item"):
        existing_guid = get_item_guid(existing_item)

        if existing_guid == guid_value:
            matches.append(existing_item)

    return matches


def remove_duplicate_items_for_guid(
    channel: ET.Element,
    guid_value: str,
):
    """
    Keep the first occurrence of the GUID and remove any additional
    copies. Since new items are inserted at the top, the first match
    should normally be the newest copy.
    """
    matching_items = find_items_by_guid(
        channel,
        guid_value,
    )

    if len(matching_items) <= 1:
        return matching_items

    kept_item = matching_items[0]
    duplicates = matching_items[1:]

    for duplicate in duplicates:
        channel.remove(duplicate)

    print(
        f"Removed {len(duplicates)} duplicate RSS "
        f"item(s) for GUID: {guid_value}"
    )

    return [kept_item]


def build_rss_item(
    title: str,
    description: str,
    audio_url: str,
    audio_size: int,
    guid_value: str,
):
    item = ET.Element("item")

    ET.SubElement(
        item,
        "title",
    ).text = title

    ET.SubElement(
        item,
        "description",
    ).text = description

    ET.SubElement(
        item,
        "pubDate",
    ).text = format_datetime(
        datetime.now(timezone.utc)
    )

    enclosure = ET.SubElement(
        item,
        "enclosure",
    )

    enclosure.set(
        "url",
        audio_url,
    )

    enclosure.set(
        "length",
        str(audio_size),
    )

    enclosure.set(
        "type",
        "audio/x-m4a",
    )

    guid = ET.SubElement(
        item,
        "guid",
    )

    guid.set(
        "isPermaLink",
        "false",
    )

    guid.text = guid_value

    return item


# -----------------------------
# Google Sheets retry helper
# -----------------------------
def mark_processed_with_retry(
    spreadsheet_id: str,
    sheet_name: str,
    processed_col_letter: str,
    sheet_row_number: int,
    max_attempts: int = 6,
):
    """
    Mark the sheet row as processed.

    A brand-new Sheets client is created for each attempt so that
    the script does not reuse the HTTP connection that existed
    during a long video render.
    """
    target_range = (
        f"'{sheet_name}'!"
        f"{processed_col_letter}"
        f"{sheet_row_number}"
    )

    retryable_status_codes = {
        408,
        429,
        500,
        502,
        503,
        504,
    }

    for attempt in range(
        1,
        max_attempts + 1,
    ):
        try:
            print(
                "Updating Processed cell. "
                f"Attempt {attempt} of {max_attempts}. "
                f"Range: {target_range}"
            )

            fresh_creds = get_creds()

            fresh_sheets_svc = build_google_service(
                "sheets",
                "v4",
                fresh_creds,
            )

            (
                fresh_sheets_svc.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=target_range,
                    valueInputOption="RAW",
                    body={
                        "values": [["yes"]],
                    },
                )
                .execute(num_retries=3)
            )

            print(
                "Successfully marked episode "
                "as Processed."
            )

            return

        except HttpError as error:
            status_code = getattr(
                error.resp,
                "status",
                None,
            )

            is_retryable = (
                status_code in retryable_status_codes
            )

            print(
                "Google Sheets HTTP error while "
                "marking episode processed. "
                f"Status: {status_code}. "
                f"Attempt: {attempt}."
            )

            if not is_retryable:
                raise

            if attempt >= max_attempts:
                raise

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
            TimeoutError,
            OSError,
        ) as error:
            print(
                "Temporary connection error while "
                "marking episode processed: "
                f"{type(error).__name__}: {error}. "
                f"Attempt: {attempt}."
            )

            if attempt >= max_attempts:
                raise

        wait_seconds = min(
            2 ** attempt,
            30,
        )

        print(
            f"Waiting {wait_seconds} seconds "
            "before retrying the Sheets update."
        )

        time.sleep(wait_seconds)

    raise RuntimeError(
        "Unable to mark episode processed after "
        f"{max_attempts} attempts."
    )


# -----------------------------
# Main
# -----------------------------
def main():
    if not EPISODES_FOLDER_ID:
        raise RuntimeError(
            "Missing episodes folder ID. "
            "Set DAILY_FOLDER_ID or "
            "EPISODES_FOLDER_ID."
        )

    if not SPREADSHEET_ID:
        raise RuntimeError(
            "Missing spreadsheet ID. "
            "Set SHEET_ID or SPREADSHEET_ID."
        )

    if not BUCKET_NAME:
        raise RuntimeError(
            "Missing bucket name. "
            "Set RSS_BUCKET or BUCKET_NAME."
        )

    if not THUMBNAILS_FOLDER_ID:
        print(
            "THUMBNAILS_FOLDER_ID is not set. "
            "Thumbnail folder matching will be skipped."
        )

    if not FALLBACK_IMAGE_FILE_ID:
        print(
            "FALLBACK_IMAGE_FILE_ID is not set. "
            "No final image fallback is configured."
        )

    if DATE_OVERRIDE:
        today = parse_publish_date(
            DATE_OVERRIDE
        )
        print(
            "Using DATE_OVERRIDE: "
            f"{today.isoformat()}"
        )
    else:
        today = date.today()
        print(
            "Using current date: "
            f"{today.isoformat()}"
        )

    initial_creds = get_creds()

    sheets_svc = build_google_service(
        "sheets",
        "v4",
        initial_creds,
    )

    drive_svc = build_google_service(
        "drive",
        "v3",
        initial_creds,
    )

    storage_client = storage.Client(
        credentials=initial_creds
    )

    bucket = storage_client.bucket(
        BUCKET_NAME
    )

    # -----------------------------
    # Read Main Schedule
    # -----------------------------
    sheet_response = (
        sheets_svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{SHEET_NAME}'!A1:Z",
        )
        .execute(num_retries=5)
    )

    values = sheet_response.get(
        "values",
        [],
    )

    if not values:
        print("Sheet returned no data.")
        return

    headers = values[0]

    col_index = {
        header.strip(): index
        for index, header in enumerate(headers)
    }

    COL_PUBLISH = pick_existing_header(
        col_index,
        "Publish Date",
        "PublishDate",
        "publish_date",
    )

    COL_TITLE = pick_existing_header(
        col_index,
        "Title",
        "title",
    )

    COL_DESC = pick_existing_header(
        col_index,
        "Description",
        "description",
    )

    COL_FILE = pick_existing_header(
        col_index,
        "File Name",
        "FileName",
        "Filename",
        "file_name",
    )

    COL_PROCESSED = pick_existing_header(
        col_index,
        "Processed",
        "processed",
        "Status",
        "status",
    )

    COL_THUMBNAIL = None

    for candidate in (
        "Thumbnail",
        "thumbnail",
    ):
        if candidate in col_index:
            COL_THUMBNAIL = candidate
            break

    COL_IMAGE_16x9 = pick_existing_header(
        col_index,
        "Image16x9FileId",
        "Image16x9FileID",
        "Image16x9FileIdField",
        "Image16x9FileIdFiled",
        "Image16x9FileIdFieldId",
        "Image16x9FileIdFieldID",
    )

    # -----------------------------
    # Find first unprocessed row
    # -----------------------------
    target_row = None
    row_data = None

    for row_index in range(
        1,
        len(values),
    ):
        row = values[row_index] + [""] * (
            len(headers) - len(values[row_index])
        )

        publish_raw = (
            row[col_index[COL_PUBLISH]].strip()
            if row[col_index[COL_PUBLISH]]
            else ""
        )

        if not publish_raw:
            continue

        try:
            publish_date = parse_publish_date(
                publish_raw
            )
        except ValueError:
            print(
                "Skipping row with invalid publish date: "
                f"row {row_index + 1}, "
                f"value {publish_raw!r}"
            )
            continue

        processed_raw = (
            row[col_index[COL_PROCESSED]]
            or ""
        )

        already_processed = is_processed_value(
            processed_raw
        )

        if (
            publish_date == today
            and not already_processed
        ):
            target_row = row_index
            row_data = row
            break

    if target_row is None:
        print(
            "No episode scheduled for "
            f"{today.isoformat()}, or all matching "
            "episodes are already processed."
        )
        return

    sheet_row_number = target_row + 1

    title = (
        row_data[col_index[COL_TITLE]]
        or ""
    ).strip()

    description = (
        row_data[col_index[COL_DESC]]
        or ""
    ).strip()

    filename = (
        row_data[col_index[COL_FILE]]
        or ""
    ).strip()

    thumbnail_filename = ""

    if COL_THUMBNAIL:
        thumbnail_filename = (
            row_data[col_index[COL_THUMBNAIL]]
            or ""
        ).strip()

    image16x9_id = (
        row_data[col_index[COL_IMAGE_16x9]]
        or ""
    ).strip()

    if not title:
        raise RuntimeError(
            f"Row {sheet_row_number} is missing Title."
        )

    if not filename:
        raise RuntimeError(
            f"Row {sheet_row_number} is missing File Name."
        )

    print(
        f"Processing row {sheet_row_number}: {title}"
    )

    print(
        f"Audio filename: {filename}"
    )

    # -----------------------------
    # Prepare local files
    # -----------------------------
    os.makedirs(
        "/tmp/dd",
        exist_ok=True,
    )

    local_audio = "/tmp/dd/audio.m4a"
    local_image = "/tmp/dd/image.png"
    local_video = "/tmp/dd/output.mp4"

    # -----------------------------
    # Download assets from Drive
    # -----------------------------
    audio_file_id = drive_find_file_id(
        drive_svc,
        EPISODES_FOLDER_ID,
        filename,
    )

    drive_download_file(
        drive_svc,
        audio_file_id,
        local_audio,
    )

    selected_image_file_id = (
        resolve_thumbnail_file_id(
            drive_svc=drive_svc,
            thumbnails_folder_id=(
                THUMBNAILS_FOLDER_ID
            ),
            thumbnail_filename=(
                thumbnail_filename
            ),
            image16x9_id=image16x9_id,
            fallback_image_file_id=(
                FALLBACK_IMAGE_FILE_ID
            ),
        )
    )

    drive_download_file(
        drive_svc,
        selected_image_file_id,
        local_image,
    )

    # -----------------------------
    # Render video
    # -----------------------------
    run_ffmpeg(
        local_image,
        local_audio,
        local_video,
    )

    # -----------------------------
    # Upload artifacts
    # -----------------------------
    audio_blob_name = (
        f"{BUCKET_EPISODES_PREFIX}/"
        f"{today.isoformat()}/"
        f"{filename}"
    )

    video_filename = (
        filename.rsplit(".", 1)[0]
        + ".mp4"
    )

    video_blob_name = (
        f"{BUCKET_EPISODES_PREFIX}/"
        f"{today.isoformat()}/"
        f"{video_filename}"
    )

    print(
        "Uploading audio to "
        f"gs://{BUCKET_NAME}/{audio_blob_name}"
    )

    blob_audio = bucket.blob(
        audio_blob_name
    )

    blob_audio.upload_from_filename(
        local_audio,
        content_type="audio/x-m4a",
        timeout=600,
        retry=storage.retry.DEFAULT_RETRY,
    )

    try:
        blob_audio.make_public()
    except Exception as error:
        print(
            "Could not make audio object public. "
            f"Continuing: {error}"
        )

    print(
        "Uploading video to "
        f"gs://{BUCKET_NAME}/{video_blob_name}"
    )

    blob_video = bucket.blob(
        video_blob_name
    )

    blob_video.upload_from_filename(
        local_video,
        content_type="video/mp4",
        timeout=1800,
        retry=storage.retry.DEFAULT_RETRY,
    )

    try:
        blob_video.make_public()
    except Exception as error:
        print(
            "Could not make video object public. "
            f"Continuing: {error}"
        )

    print("Audio and video uploads completed.")

    # -----------------------------
    # Update RSS safely
    # -----------------------------
    rss_blob = bucket.blob(
        RSS_BLOB_NAME
    )

    rss_xml = rss_blob.download_as_text(
        timeout=120,
        retry=storage.retry.DEFAULT_RETRY,
    )

    root = ET.fromstring(
        rss_xml
    )

    channel = root.find("channel")

    if channel is None:
        raise RuntimeError(
            "rss.xml is missing the <channel> element."
        )

    guid_value = (
        f"dddevotion-"
        f"{today.isoformat()}-"
        f"{filename}"
    )

    matching_items = remove_duplicate_items_for_guid(
        channel,
        guid_value,
    )

    if matching_items:
        print(
            "RSS item already exists. "
            "Skipping duplicate insertion for GUID: "
            f"{guid_value}"
        )
    else:
        new_item = build_rss_item(
            title=title,
            description=description,
            audio_url=blob_audio.public_url,
            audio_size=os.path.getsize(
                local_audio
            ),
            guid_value=guid_value,
        )

        add_item_to_channel_newest_first(
            channel,
            new_item,
        )

        print(
            "Added new RSS item for GUID: "
            f"{guid_value}"
        )

    updated_xml = ET.tostring(
        root,
        encoding="unicode",
    )

    rss_blob.upload_from_string(
        updated_xml,
        content_type="application/xml",
        timeout=120,
        retry=storage.retry.DEFAULT_RETRY,
    )

    try:
        rss_blob.make_public()
    except Exception as error:
        print(
            "Could not make RSS object public. "
            f"Continuing: {error}"
        )

    print("RSS update completed.")

    # -----------------------------
    # Mark row processed
    # -----------------------------
    processed_col_letter = col_to_a1(
        col_index[COL_PROCESSED]
    )

    mark_processed_with_retry(
        spreadsheet_id=SPREADSHEET_ID,
        sheet_name=SHEET_NAME,
        processed_col_letter=(
            processed_col_letter
        ),
        sheet_row_number=(
            sheet_row_number
        ),
    )

    print("Success:", title)
    print(
        "Audio URL:",
        blob_audio.public_url,
    )
    print(
        "Video URL:",
        blob_video.public_url,
    )
    print(
        "RSS URL:",
        (
            f"https://storage.googleapis.com/"
            f"{BUCKET_NAME}/"
            f"{RSS_BLOB_NAME}"
        ),
    )


if __name__ == "__main__":
    main()
