"""Google Cloud Storage integration for DeepDiveDevotions."""
from typing import Optional, List
from google.cloud import storage
from google.oauth2 import service_account


class GCSClient:
    """Client for interacting with Google Cloud Storage."""
    
    def __init__(self, credentials_path: Optional[str] = None):
        """Initialize the GCS client.
        
        Args:
            credentials_path: Path to service account credentials JSON file
        """
        self.credentials = None
        if credentials_path:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
        self.client = None
        
    def connect(self):
        """Establish connection to Google Cloud Storage."""
        if self.credentials:
            self.client = storage.Client(credentials=self.credentials)
        else:
            self.client = storage.Client()
    
    def upload_file(self, bucket_name: str, source_file_path: str, destination_blob_name: str) -> bool:
        """Upload a file to GCS bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            source_file_path: Local path to the file
            destination_blob_name: Destination path in the bucket
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            self.connect()
        
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_path)
            print(f"File {source_file_path} uploaded to {destination_blob_name}")
            return True
        except Exception as error:
            print(f"An error occurred: {error}")
            return False
    
    def upload_string(self, bucket_name: str, content: str, destination_blob_name: str) -> bool:
        """Upload string content to GCS bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            content: String content to upload
            destination_blob_name: Destination path in the bucket
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            self.connect()
        
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(content)
            print(f"Content uploaded to {destination_blob_name}")
            return True
        except Exception as error:
            print(f"An error occurred: {error}")
            return False
    
    def download_file(self, bucket_name: str, source_blob_name: str, destination_file_path: str) -> bool:
        """Download a file from GCS bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            source_blob_name: Path to the blob in the bucket
            destination_file_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            self.connect()
        
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_path)
            print(f"Blob {source_blob_name} downloaded to {destination_file_path}")
            return True
        except Exception as error:
            print(f"An error occurred: {error}")
            return False
    
    def list_blobs(self, bucket_name: str, prefix: Optional[str] = None) -> List[str]:
        """List blobs in a GCS bucket.
        
        Args:
            bucket_name: Name of the GCS bucket
            prefix: Optional prefix to filter blobs
            
        Returns:
            List of blob names
        """
        if not self.client:
            self.connect()
        
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as error:
            print(f"An error occurred: {error}")
            return []
