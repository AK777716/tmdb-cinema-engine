import requests
import pandas as pd
import os
import time
import gzip
import json
from dotenv import load_dotenv
from google.cloud import bigquery

# Load credentials from .env
load_dotenv()

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_NAME = os.getenv("BQ_TABLE")
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
API_KEY = os.getenv("TMDB_API_KEY")

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

def fetch_movie_details(movie_id, retries=3):
    """Fetches details with retry logic. IMDB_ID has been removed."""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=15) 
            if response.status_code == 200:
                data = response.json()
                return {
                    "id": data.get("id"),
                    "title": data.get("title", "Unknown"),
                    "release_date": data.get("release_date"),
                    "budget": data.get("budget", 0) or 0,
                    "revenue": data.get("revenue", 0) or 0,
                    "runtime": data.get("runtime", 0),
                    "popularity": data.get("popularity", 0.0),
                    "vote_average": data.get("vote_average", 0.0),
                    "vote_count": data.get("vote_count", 0),
                    "status": data.get("status"),
                    "tagline": data.get("tagline", ""),
                    "overview": data.get("overview", ""),
                    "original_language": data.get("original_language", ""),
                    "adult": data.get("adult", False),
                    "genres": ", ".join([g['name'] for g in data.get("genres", [])]) if data.get("genres") else "None",
                    "production_companies": ", ".join([c['name'] for c in data.get("production_companies", [])]) if data.get("production_companies") else "None",
                    "production_countries": ", ".join([n['name'] for n in data.get("production_countries", [])]) if data.get("production_countries") else "None",
                    "ingested_at": str(pd.Timestamp.now())
                }
            elif response.status_code == 429:
                time.sleep(5)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            time.sleep(2) 
            
    return None

def run_pipeline(limit=5000):
    print(f"🚀 Starting High-Scale Engine for Project: {PROJECT_ID}")
    
    all_movie_data = []
    processed_count = 0
    
    try:
        with gzip.open("movie_ids.json.gz", "rb") as f:
            for line in f:
                if processed_count >= limit:
                    break
                    
                movie_data = json.loads(line)
                movie_id = movie_data["id"]
                popularity = movie_data.get("popularity", 0)

                # Filter: Only fetch if popularity > 5 to ensure better data quality
                if popularity < 5: 
                    continue

                details = fetch_movie_details(movie_id)
                
                if details:
                    all_movie_data.append(details)
                    processed_count += 1
                    if processed_count % 50 == 0:
                        print(f"✅ Progress: {processed_count}/{limit} movies fetched...")
                
                time.sleep(0.05) 
    except FileNotFoundError:
        print("❌ Error: movie_ids.json.gz not found. Run ingest_ids.py first!")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_movie_data)

    # BigQuery Upload Configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", # Deletes old rows and replaces with this new 5k batch
        autodetect=True,
    )

    print(f"📤 Uploading {len(df)} High-Quality movies to BigQuery...")
    try:
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result() 
        print("🏆 SUCCESS! 5,000 High-Quality movies are now in BigQuery.")
    except Exception as e:
        print(f"❌ Upload Failed! Error: {e}")

if __name__ == "__main__":
    run_pipeline(limit=5000)