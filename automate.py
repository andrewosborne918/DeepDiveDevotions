
import os
import re
import sys
import io
import subprocess
from datetime import datetime, date
from email.utils import format_datetime
import xml.etree.ElementTree as ET

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage

EPISODES_FOLDER_ID = os.getenv("EPISODES_FOLDER_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME", "deep-dive-podcast-assets")
RSS_BLOB_NAME = os.getenv("RSS_BLOB_NAME", "rss.xml")
BUCKET_EPISODES_PREFIX = os.getenv("BUCKET_EPISODES_PREFIX", "episodes")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Main Schedule")

def get_creds():
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]
    return service_account.Credentials.from_service_account_file(key_path, scopes=scopes)

def parse_publish_date(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()

def drive_find_file_id(drive_svc, parent_folder_id, filename):
    q = f"name = '{filename}' and '{parent_folder_id}' in parents and trashed = false"
    res = drive_svc.files().list(q=q, fields="files(id,name)", pageSize=1).execute()
    files = res.get("files", [])
    if not files:
        raise FileNotFoundError(f"Drive file not found: {filename}")
    return files[0]["id"]

def drive_download_file(drive_svc, file_id, out_path):
    request = drive_svc.files().get_media(fileId=file_id)
    fh = io.FileIO(out_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()

def run_ffmpeg(image_path, audio_path, out_video_path):
    cmd = [
        "ffmpeg","-y",
        "-loop","1","-i",image_path,
        "-i",audio_path,
        "-c:v","libx264",
        "-tune","stillimage",
        "-c:a","aac",
        "-b:a","192k",
        "-pix_fmt","yuv420p",
        "-movflags","+faststart",
        "-shortest",
        out_video_path
    ]
    subprocess.run(cmd, check=True)

def main():
    creds = get_creds()
    sheets_svc = build("sheets","v4",credentials=creds)
    drive_svc = build("drive","v3",credentials=creds)
    storage_client = storage.Client(credentials=creds)
    bucket = storage_client.bucket(BUCKET_NAME)

    resp = sheets_svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1:Z"
    ).execute()

    values = resp.get("values", [])
    headers = values[0]
    today = date.today()

    col_index = {h: i for i, h in enumerate(headers)}

    target_row = None
    for r in range(1, len(values)):
        row = values[r] + [""]*(len(headers)-len(values[r]))
        if not row[col_index["Publish Date"]]:
            continue
        pub = parse_publish_date(row[col_index["Publish Date"]])
        processed = row[col_index["Processed"]].strip().lower()
        if pub == today and processed != "yes":
            target_row = r
            break

    if target_row is None:
        print("No episode for today.")
        return

    row = values[target_row] + [""]*(len(headers)-len(values[target_row]))
    title = row[col_index["Title"]]
    description = row[col_index["Description"]]
    filename = row[col_index["File Name"]]
    image16x9_id = row[col_index["Image16x9FileId"]]

    os.makedirs("/tmp/dd", exist_ok=True)
    local_audio = "/tmp/dd/audio.m4a"
    local_image = "/tmp/dd/image.png"
    local_video = "/tmp/dd/output.mp4"

    audio_file_id = drive_find_file_id(drive_svc, EPISODES_FOLDER_ID, filename)
    drive_download_file(drive_svc, audio_file_id, local_audio)
    drive_download_file(drive_svc, image16x9_id, local_image)

    run_ffmpeg(local_image, local_audio, local_video)

    audio_blob = f"{BUCKET_EPISODES_PREFIX}/{today.isoformat()}/{filename}"
    video_blob = f"{BUCKET_EPISODES_PREFIX}/{today.isoformat()}/video.mp4"

    blob_audio = bucket.blob(audio_blob)
    blob_audio.upload_from_filename(local_audio)
    blob_audio.make_public()

    blob_video = bucket.blob(video_blob)
    blob_video.upload_from_filename(local_video)
    blob_video.make_public()

    rss_blob = bucket.blob(RSS_BLOB_NAME)
    rss_xml = rss_blob.download_as_text()
    root = ET.fromstring(rss_xml)
    channel = root.find("channel")

    item = ET.Element("item")
    ET.SubElement(item,"title").text = title
    ET.SubElement(item,"description").text = description
    ET.SubElement(item,"pubDate").text = format_datetime(datetime.utcnow())
    enclosure = ET.SubElement(item,"enclosure")
    enclosure.set("url", blob_audio.public_url)
    enclosure.set("length", str(os.path.getsize(local_audio)))
    enclosure.set("type","audio/mp4")

    channel.insert(0,item)
    rss_blob.upload_from_string(ET.tostring(root,encoding="unicode"), content_type="application/xml")
    rss_blob.make_public()

    sheets_svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!{target_row+1}:{target_row+1}",
        valueInputOption="RAW",
        body={"values":[row]}
    ).execute()

    print("Success:", title)

if __name__ == "__main__":
    main()
