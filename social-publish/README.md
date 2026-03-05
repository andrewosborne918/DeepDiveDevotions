# deep-dive-social-publish

Cloud Run Job that:
- Finds today's processed rows in your Google Sheet
- Resolves each row's video based on **File Name** -> mp4 in GCS (supports multiple videos per day)
- Uploads to YouTube (optional, if YouTube env vars are set)
- Posts **natively** to your Facebook Page (default)

## Expected GCS layout

`gs://<BUCKET_NAME>/<VIDEO_OBJECT_PREFIX>/<YYYY-MM-DD>/<derived>.mp4`

Example:
- `gs://deep-dive-podcast-assets/episodes/2026-03-02/1 Genesis_v2.mp4`

The script derives the mp4 name from the row's `File Name` column:
- `1 Genesis_v2.m4a` -> `1 Genesis_v2.mp4`

## Sheet columns

Required:
- Publish Date (YYYY-MM-DD)
- Title
- Description
- File Name
- Processed (must be yes)

Recommended outputs (script will update if present):
- VideoURL
- YouTubeURL
- SocialPost
- FacebookPostId
- SocialPublished

## Env vars

See `.env.example` for the full list.

## Build & deploy (example)

Build:
```bash
gcloud builds submit --tag us-east5-docker.pkg.dev/$PROJECT_ID/containers/deep-dive-social-publish:latest
```

Create job:
```bash
gcloud run jobs create deep-dive-social-publish \
  --image us-east5-docker.pkg.dev/$PROJECT_ID/containers/deep-dive-social-publish:latest \
  --region us-east5 \
  --set-env-vars BUCKET_NAME=deep-dive-podcast-assets,VIDEO_OBJECT_PREFIX=episodes/,SHEET_ID=YOUR_SHEET_ID,SHEET_NAME="Main Schedule",FB_MODE=native
```

### Permissions

- Cloud Run Job service account needs `roles/storage.objectViewer` on the bucket.
- Share the Google Sheet with the **service account email** as Editor.

### Secrets

Move these to Secret Manager / Cloud Run Secrets:
- META_PAGE_ACCESS_TOKEN
- YOUTUBE_CLIENT_SECRET
- YOUTUBE_REFRESH_TOKEN
