# DeepDiveDevotions

A Python application for managing and publishing daily devotional content. This system integrates with Google Sheets, Google Drive, Google Cloud Storage, and GitHub to automate the process of fetching, processing, and distributing devotional content via RSS feeds.

## Features

- **Google Sheets Integration**: Fetch devotional content from Google Sheets
- **RSS Feed Generation**: Automatically generate RSS feeds for content distribution
- **Google Drive Support**: Access and manage files stored in Google Drive
- **Cloud Storage**: Upload and manage content in Google Cloud Storage
- **GitHub Integration**: Automated updates to GitHub repository
- **Video Processing**: Handle video content and YouTube integration
- **Automated Publishing**: Daily automated runs via GitHub Actions

## Project Structure

```
.
├── feed.xml                    # Generated RSS feed
├── state.json                  # State tracking for processed items
├── README.md                   # Project documentation
├── requirements.txt            # Python dependencies
├── src/
│   ├── main.py                # Main entry point
│   ├── config.py              # Configuration management
│   ├── sheets.py              # Google Sheets integration
│   ├── drive.py               # Google Drive integration
│   ├── gcs.py                 # Google Cloud Storage integration
│   ├── rss.py                 # RSS feed generation
│   ├── github_repo.py         # GitHub repository integration
│   └── video.py               # Video processing functionality
└── .github/
    └── workflows/
        └── publish.yml        # Automated publishing workflow
```

## Setup

### Prerequisites

- Python 3.11 or higher
- Google Cloud service account with appropriate permissions
- GitHub personal access token (for automated updates)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/andrewosborne918/DeepDiveDevotions.git
   cd DeepDiveDevotions
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables (see Configuration section below)

### Configuration

Set the following environment variables:

**Required:**
- `SPREADSHEET_ID`: Google Sheets spreadsheet ID containing devotions

**Optional:**
- `SHEET_NAME`: Name of the sheet (default: "Devotions")
- `DRIVE_FOLDER_ID`: Google Drive folder ID for file storage
- `GCS_BUCKET_NAME`: Google Cloud Storage bucket name
- `GITHUB_TOKEN`: GitHub personal access token for automated updates
- `GITHUB_REPOSITORY`: Repository in format "owner/repo" (default: andrewosborne918/DeepDiveDevotions)
- `FEED_TITLE`: RSS feed title (default: "Deep Dive Devotions")
- `FEED_LINK`: RSS feed link (default: repository URL)
- `FEED_DESCRIPTION`: RSS feed description (default: "Daily devotional content")
- `STATE_FILE`: Path to state file (default: "state.json")
- `FEED_FILE`: Path to feed file (default: "feed.xml")

### Google Cloud Setup

1. Create a service account in Google Cloud Console
2. Enable the following APIs:
   - Google Sheets API
   - Google Drive API
   - Google Cloud Storage API
3. Download the service account credentials JSON file
4. Store the credentials as a GitHub secret named `GOOGLE_CREDENTIALS`

## Usage

### Manual Execution

Run the devotions processor manually:

```bash
cd src
python main.py
```

### Automated Publishing

The GitHub Actions workflow runs automatically:
- Daily at 6:00 AM UTC
- Can be triggered manually from the Actions tab

The workflow will:
1. Fetch new devotions from Google Sheets
2. Generate an updated RSS feed
3. Commit and push changes to the repository

## Development

### Code Structure

- **config.py**: Manages configuration from environment variables
- **sheets.py**: Provides `SheetsClient` for Google Sheets operations
- **drive.py**: Provides `DriveClient` for Google Drive operations
- **gcs.py**: Provides `GCSClient` for Google Cloud Storage operations
- **rss.py**: RSS feed generation using the `feedgen` library
- **github_repo.py**: GitHub API integration for repository updates
- **video.py**: Video processing and YouTube URL handling
- **main.py**: Orchestrates the entire processing pipeline

### State Management

The application tracks processing state in `state.json`:
- `last_processed_row`: Index of the last processed devotion
- `last_updated`: Timestamp of the last update
- `published_items`: List of published item IDs

## License

This project is open source and available under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.