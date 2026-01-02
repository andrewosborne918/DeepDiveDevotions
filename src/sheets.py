"""Google Sheets integration for DeepDiveDevotions."""
from typing import List, Dict, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class SheetsClient:
    """Client for interacting with Google Sheets."""
    
    def __init__(self, credentials_path: Optional[str] = None):
        """Initialize the Sheets client.
        
        Args:
            credentials_path: Path to service account credentials JSON file
        """
        self.credentials = None
        if credentials_path:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
        self.service = None
        
    def connect(self):
        """Establish connection to Google Sheets API."""
        if self.credentials:
            self.service = build('sheets', 'v4', credentials=self.credentials)
        else:
            self.service = build('sheets', 'v4')
    
    def read_sheet(self, spreadsheet_id: str, range_name: str) -> List[List[str]]:
        """Read data from a Google Sheet.
        
        Args:
            spreadsheet_id: The ID of the spreadsheet
            range_name: The A1 notation of the range to read
            
        Returns:
            List of rows, where each row is a list of cell values
        """
        if not self.service:
            self.connect()
            
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            return values
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []
    
    def get_devotions(self, spreadsheet_id: str, sheet_name: str = 'Devotions') -> List[Dict]:
        """Get devotional entries from the sheet.
        
        Args:
            spreadsheet_id: The ID of the spreadsheet
            sheet_name: The name of the sheet
            
        Returns:
            List of devotion dictionaries
        """
        rows = self.read_sheet(spreadsheet_id, f"{sheet_name}!A:Z")
        
        if not rows:
            return []
        
        # Assume first row is headers
        headers = rows[0]
        devotions = []
        
        for row in rows[1:]:
            # Pad row to match headers length
            while len(row) < len(headers):
                row.append('')
            
            devotion = {headers[i]: row[i] for i in range(len(headers))}
            devotions.append(devotion)
        
        return devotions
