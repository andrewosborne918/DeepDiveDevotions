"""Video processing functionality for DeepDiveDevotions."""
from typing import Optional, Dict, Any
import os
import requests


class VideoProcessor:
    """Processor for handling video content."""
    
    def __init__(self):
        """Initialize the video processor."""
        self.supported_formats = ['.mp4', '.mov', '.avi', '.mkv', '.webm']
    
    def is_video_file(self, filename: str) -> bool:
        """Check if a file is a video file.
        
        Args:
            filename: Name of the file
            
        Returns:
            True if video file, False otherwise
        """
        _, ext = os.path.splitext(filename.lower())
        return ext in self.supported_formats
    
    def get_video_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Get metadata from a video file.
        
        Args:
            file_path: Path to the video file
            
        Returns:
            Dictionary of metadata
        """
        if not os.path.exists(file_path):
            print(f"Video file not found: {file_path}")
            return None
        
        # Basic metadata
        metadata = {
            'filename': os.path.basename(file_path),
            'size': os.path.getsize(file_path),
            'format': os.path.splitext(file_path)[1]
        }
        
        return metadata
    
    def get_youtube_video_info(self, video_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a YouTube video.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Dictionary with video information
        """
        # This is a placeholder - in production you'd use YouTube Data API
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        
        return {
            'video_id': video_id,
            'url': video_url,
            'embed_url': f"https://www.youtube.com/embed/{video_id}"
        }
    
    def generate_embed_code(self, video_id: str, width: int = 560, height: int = 315) -> str:
        """Generate HTML embed code for a YouTube video.
        
        Args:
            video_id: YouTube video ID
            width: Video player width
            height: Video player height
            
        Returns:
            HTML embed code
        """
        embed_code = (
            f'<iframe width="{width}" height="{height}" '
            f'src="https://www.youtube.com/embed/{video_id}" '
            f'frameborder="0" '
            f'allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" '
            f'allowfullscreen></iframe>'
        )
        
        return embed_code
    
    def extract_video_id_from_url(self, url: str) -> Optional[str]:
        """Extract YouTube video ID from URL.
        
        Args:
            url: YouTube URL
            
        Returns:
            Video ID if found, None otherwise
        """
        # Handle different YouTube URL formats
        patterns = [
            'youtube.com/watch?v=',
            'youtu.be/',
            'youtube.com/embed/'
        ]
        
        for pattern in patterns:
            if pattern in url:
                if pattern == 'youtube.com/watch?v=':
                    video_id = url.split('v=')[1].split('&')[0]
                else:
                    video_id = url.split(pattern)[1].split('?')[0].split('&')[0]
                return video_id
        
        return None
