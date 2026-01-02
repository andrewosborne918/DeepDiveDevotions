"""Main entry point for DeepDiveDevotions."""
import os
import json
import sys
import traceback
from datetime import datetime
from typing import List, Dict

from config import load_config
from sheets import SheetsClient
from drive import DriveClient
from gcs import GCSClient
from rss import create_feed_from_devotions
from github_repo import GitHubRepo


def load_state(state_file: str) -> Dict:
    """Load state from file.
    
    Args:
        state_file: Path to state file
        
    Returns:
        State dictionary
    """
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)
    return {
        'last_processed_row': 0,
        'last_updated': None,
        'published_items': []
    }


def save_state(state_file: str, state: Dict):
    """Save state to file.
    
    Args:
        state_file: Path to state file
        state: State dictionary
    """
    with open(state_file, 'w') as f:
        json.dump(state, indent=2, fp=f)


def process_devotions(config):
    """Main processing function.
    
    Args:
        config: Configuration object
    """
    print("Starting DeepDiveDevotions processing...")
    
    # Load state
    state = load_state(config.state_file)
    print(f"Last processed row: {state['last_processed_row']}")
    
    # Initialize clients
    sheets_client = SheetsClient()
    
    # Get devotions from Google Sheets
    print(f"Fetching devotions from spreadsheet: {config.spreadsheet_id}")
    devotions = sheets_client.get_devotions(config.spreadsheet_id, config.sheet_name)
    
    if not devotions:
        print("No devotions found")
        return
    
    print(f"Found {len(devotions)} devotions")
    
    # Filter new devotions
    new_devotions = devotions[state['last_processed_row']:]
    
    if not new_devotions:
        print("No new devotions to process")
        return
    
    print(f"Processing {len(new_devotions)} new devotions")
    
    # Generate RSS feed
    feed_content = create_feed_from_devotions(devotions, config)
    
    # Save RSS feed
    with open(config.feed_file, 'w') as f:
        f.write(feed_content)
    print(f"RSS feed saved to {config.feed_file}")
    
    # Update state
    state['last_processed_row'] = len(devotions)
    state['last_updated'] = datetime.now().isoformat()
    state['published_items'].extend([d.get('id', f"item-{i}") for i, d in enumerate(new_devotions)])
    
    save_state(config.state_file, state)
    print("State updated")
    
    # Optionally push to GitHub
    if config.github_token:
        print("Updating GitHub repository...")
        github = GitHubRepo(config.github_token, config.github_repo)
        
        # Update feed.xml
        github.update_file(
            path='feed.xml',
            content=feed_content,
            message=f'Update RSS feed - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        )
        
        # Update state.json
        with open(config.state_file, 'r') as f:
            state_content = f.read()
        
        github.update_file(
            path='state.json',
            content=state_content,
            message=f'Update state - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        )
    
    print("Processing complete!")


def main():
    """Main function."""
    # Load configuration
    config = load_config()
    
    # Validate configuration
    if not config.validate():
        print("Configuration validation failed")
        sys.exit(1)
    
    # Process devotions
    try:
        process_devotions(config)
    except Exception as e:
        print(f"Error processing devotions: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
