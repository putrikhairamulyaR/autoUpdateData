import subprocess
import pandas as pd
import yaml
import os

def fetch_with_harvest(config):
    filename = config.get('harvest_output', 'tweets_harvest.csv')
    # Tweet-harvest saves to tweets-data folder
    actual_filename = f"tweets-data/{filename}"
    search_keyword = config['twitter_query']
    limit = config['max_tweets']
    token = config['twitter_bearer_token']

    # Add date filter for recent tweets (last 2 hours)
    from datetime import datetime, timedelta
    now = datetime.now()
    two_hours_ago = now - timedelta(hours=2)
    
    # Format dates for Twitter search
    current_time = now.strftime('%Y-%m-%d_%H:%M:%S')
    two_hours_ago_str = two_hours_ago.strftime('%Y-%m-%d_%H:%M:%S')
    
    # Update search query to include date filter for last 2 hours
    search_keyword_with_date = f"{search_keyword} since:{two_hours_ago_str} until:{current_time}"
    
    cmd = [
        'npx', '-y', 'tweet-harvest@2.6.1',
        '-o', filename,
        '-s', search_keyword_with_date,
        '--tab', 'LATEST',
        '-l', str(limit),
        '--token', token
    ]
    print("Running:", " ".join(cmd))
    print(f"Searching for tweets with hashtags: #indihome, #telkomIndonesia, #telkom, #gangguanTelkom")
    print(f"Time range: {two_hours_ago_str} to {current_time}")
    print(f"Target: {limit} tweets every 2 hours")
    
    subprocess.run(" ".join(cmd), check=True, shell=True)

    # Check if file exists in tweets-data folder
    if os.path.exists(actual_filename):
        df = pd.read_csv(actual_filename)
        print(f"Loaded {len(df)} tweets from {actual_filename}")
    else:
        print(f"Warning: File {actual_filename} not found")
        df = pd.DataFrame()
    
    return df

if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path) as f:
        config = yaml.safe_load(f)
    fetch_with_harvest(config)