"""GitHub repository integration for DeepDiveDevotions."""
import os
import json
from typing import Optional, Dict, Any
import requests


class GitHubRepo:
    """Client for interacting with GitHub repository."""
    
    def __init__(self, token: Optional[str] = None, repo: Optional[str] = None):
        """Initialize the GitHub client.
        
        Args:
            token: GitHub personal access token
            repo: Repository in format 'owner/repo'
        """
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.repo = repo or os.getenv('GITHUB_REPOSITORY')
        self.base_url = 'https://api.github.com'
        
        self.headers = {
            'Accept': 'application/vnd.github.v3+json'
        }
        if self.token:
            self.headers['Authorization'] = f'token {self.token}'
    
    def get_file_content(self, path: str, ref: str = 'main') -> Optional[str]:
        """Get content of a file from the repository.
        
        Args:
            path: Path to the file in the repository
            ref: Git reference (branch, tag, or commit)
            
        Returns:
            File content as string, or None if not found
        """
        url = f"{self.base_url}/repos/{self.repo}/contents/{path}"
        params = {'ref': ref}
        
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            import base64
            content = response.json().get('content', '')
            return base64.b64decode(content).decode('utf-8')
        else:
            print(f"Failed to get file: {response.status_code}")
            return None
    
    def update_file(self, path: str, content: str, message: str, 
                    branch: str = 'main', sha: Optional[str] = None) -> bool:
        """Update or create a file in the repository.
        
        Args:
            path: Path to the file in the repository
            content: New file content
            message: Commit message
            branch: Branch to commit to
            sha: SHA of the file being replaced (required for updates)
            
        Returns:
            True if successful, False otherwise
        """
        import base64
        
        url = f"{self.base_url}/repos/{self.repo}/contents/{path}"
        
        # Get current file SHA if not provided
        if not sha:
            response = requests.get(url, headers=self.headers, params={'ref': branch})
            if response.status_code == 200:
                sha = response.json().get('sha')
        
        data = {
            'message': message,
            'content': base64.b64encode(content.encode('utf-8')).decode('utf-8'),
            'branch': branch
        }
        
        if sha:
            data['sha'] = sha
        
        response = requests.put(url, headers=self.headers, json=data)
        
        if response.status_code in [200, 201]:
            print(f"Successfully updated {path}")
            return True
        else:
            print(f"Failed to update file: {response.status_code} - {response.text}")
            return False
    
    def create_issue(self, title: str, body: str, labels: Optional[list] = None) -> Optional[int]:
        """Create an issue in the repository.
        
        Args:
            title: Issue title
            body: Issue body
            labels: Optional list of label names
            
        Returns:
            Issue number if successful, None otherwise
        """
        url = f"{self.base_url}/repos/{self.repo}/issues"
        
        data = {
            'title': title,
            'body': body
        }
        
        if labels:
            data['labels'] = labels
        
        response = requests.post(url, headers=self.headers, json=data)
        
        if response.status_code == 201:
            issue_number = response.json().get('number')
            print(f"Created issue #{issue_number}")
            return issue_number
        else:
            print(f"Failed to create issue: {response.status_code}")
            return None
