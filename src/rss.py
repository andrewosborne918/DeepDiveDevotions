"""RSS feed generation for DeepDiveDevotions."""
from typing import List, Dict
from datetime import datetime
from feedgen.feed import FeedGenerator
import xml.etree.ElementTree as ET


class RSSFeedGenerator:
    """Generator for RSS feeds."""
    
    def __init__(self, title: str, link: str, description: str):
        """Initialize the RSS feed generator.
        
        Args:
            title: Feed title
            link: Feed link
            description: Feed description
        """
        self.fg = FeedGenerator()
        self.fg.title(title)
        self.fg.link(href=link, rel='alternate')
        self.fg.description(description)
        self.fg.language('en')
        
    def add_entry(self, title: str, link: str, description: str, 
                  pub_date: datetime, guid: str, **kwargs):
        """Add an entry to the feed.
        
        Args:
            title: Entry title
            link: Entry link
            description: Entry description
            pub_date: Publication date
            guid: Unique identifier
            **kwargs: Additional entry fields
        """
        fe = self.fg.add_entry()
        fe.title(title)
        fe.link(href=link)
        fe.description(description)
        fe.pubDate(pub_date)
        fe.guid(guid)
        
        # Add optional fields
        if 'author' in kwargs:
            fe.author(name=kwargs['author'])
        if 'content' in kwargs:
            fe.content(kwargs['content'])
    
    def generate_rss_string(self) -> str:
        """Generate RSS feed as string.
        
        Returns:
            RSS feed XML as string
        """
        return self.fg.rss_str(pretty=True).decode('utf-8')
    
    def save_rss_file(self, filepath: str):
        """Save RSS feed to file.
        
        Args:
            filepath: Path to save the RSS file
        """
        self.fg.rss_file(filepath, pretty=True)


def create_feed_from_devotions(devotions: List[Dict], config) -> str:
    """Create RSS feed from devotion entries.
    
    Args:
        devotions: List of devotion dictionaries
        config: Configuration object
        
    Returns:
        RSS feed as string
    """
    feed = RSSFeedGenerator(
        title=config.feed_title,
        link=config.feed_link,
        description=config.feed_description
    )
    
    for devotion in devotions:
        # Extract fields from devotion
        title = devotion.get('title', 'Untitled')
        description = devotion.get('description', '')
        content = devotion.get('content', description)
        link = devotion.get('link', config.feed_link)
        
        # Parse date or use current date
        date_str = devotion.get('date', '')
        try:
            pub_date = datetime.strptime(date_str, '%Y-%m-%d')
        except (ValueError, TypeError):
            pub_date = datetime.now()
        
        # Create unique guid
        guid = devotion.get('id', f"{title}-{date_str}")
        
        feed.add_entry(
            title=title,
            link=link,
            description=description,
            pub_date=pub_date,
            guid=guid,
            content=content
        )
    
    return feed.generate_rss_string()
