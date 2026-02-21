import requests
import gzip
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def download_daily_id_export():
    # TMDB provides a daily JSON file of ALL valid IDs
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%m_%d_%Y')
    url = f"http://files.tmdb.org/p/exports/movie_ids_{yesterday}.json.gz"
    
    print(f"Attempting to download: {url}")
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        with open("movie_ids.json.gz", "wb") as f:
            f.write(response.content)
        
        movie_ids = []
        with gzip.open("movie_ids.json.gz", "rb") as f:
            for line in f:
                movie_ids.append(json.loads(line))
                if len(movie_ids) >= 5000:
                    break
        return movie_ids
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        return []

ids = download_daily_id_export()
if ids:
    print(f"✅ Successfully retrieved {len(ids)} movie IDs.")