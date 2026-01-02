"""Google Drive integration for DeepDiveDevotions."""
from typing import Optional, List
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
import io


class DriveClient:
    """Client for interacting with Google Drive."""
    
    def __init__(self, credentials_path: Optional[str] = None):
        """Initialize the Drive client.
        
        Args:
            credentials_path: Path to service account credentials JSON file
        """
        self.credentials = None
        if credentials_path:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
        self.service = None
        
    def connect(self):
        """Establish connection to Google Drive API."""
        if self.credentials:
            self.service = build('drive', 'v3', credentials=self.credentials)
        else:
            self.service = build('drive', 'v3')
    
    def list_files(self, folder_id: Optional[str] = None, mime_type: Optional[str] = None) -> List[dict]:
        """List files in Google Drive.
        
        Args:
            folder_id: Optional folder ID to list files from
            mime_type: Optional MIME type filter
            
        Returns:
            List of file metadata dictionaries
        """
        if not self.service:
            self.connect()
        
        try:
            query_parts = []
            if folder_id:
                query_parts.append(f"'{folder_id}' in parents")
            if mime_type:
                query_parts.append(f"mimeType='{mime_type}'")
            
            query = ' and '.join(query_parts) if query_parts else None
            
            results = self.service.files().list(
                q=query,
                pageSize=100,
                fields="nextPageToken, files(id, name, mimeType, webViewLink)"
            ).execute()
            
            files = results.get('files', [])
            return files
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []
    
    def download_file(self, file_id: str) -> Optional[bytes]:
        """Download a file from Google Drive.
        
        Args:
            file_id: The ID of the file to download
            
        Returns:
            File content as bytes, or None if error
        """
        if not self.service:
            self.connect()
        
        try:
            request = self.service.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            return file_buffer.getvalue()
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None
    
    def get_file_metadata(self, file_id: str) -> Optional[dict]:
        """Get metadata for a file.
        
        Args:
            file_id: The ID of the file
            
        Returns:
            File metadata dictionary
        """
        if not self.service:
            self.connect()
        
        try:
            file = self.service.files().get(
                fileId=file_id,
                fields="id, name, mimeType, webViewLink, createdTime, modifiedTime"
            ).execute()
            return file
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None
