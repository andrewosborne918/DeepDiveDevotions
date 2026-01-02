"""Configuration management for DeepDiveDevotions."""
import os
import json
from typing import Optional


class Config:
    """Configuration class for managing application settings."""
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        # Google Sheets configuration
        self.spreadsheet_id = os.getenv('SPREADSHEET_ID')
        self.sheet_name = os.getenv('SHEET_NAME', 'Devotions')
        
        # Google Drive configuration
        self.drive_folder_id = os.getenv('DRIVE_FOLDER_ID')
        
        # Google Cloud Storage configuration
        self.gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
        
        # GitHub configuration
        self.github_token = os.getenv('GITHUB_TOKEN')
        self.github_repo = os.getenv('GITHUB_REPOSITORY', 'andrewosborne918/DeepDiveDevotions')
        
        # RSS configuration
        self.feed_title = os.getenv('FEED_TITLE', 'Deep Dive Devotions')
        self.feed_link = os.getenv('FEED_LINK', 'https://github.com/andrewosborne918/DeepDiveDevotions')
        self.feed_description = os.getenv('FEED_DESCRIPTION', 'Daily devotional content')
        
        # State file path
        self.state_file = os.getenv('STATE_FILE', 'state.json')
        self.feed_file = os.getenv('FEED_FILE', 'feed.xml')
        
    def validate(self) -> bool:
        """Validate that required configuration is present."""
        required = ['spreadsheet_id']
        for field in required:
            if not getattr(self, field):
                print(f"Missing required configuration: {field}")
                return False
        return True


def load_config() -> Config:
    """Load and return configuration."""
    config = Config()
    return config
