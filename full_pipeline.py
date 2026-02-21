import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

# Load credentials
load_dotenv()

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_NAME = os.getenv("BQ_TABLE")
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
API_KEY = os.getenv("TMDB_API_KEY")

# Initialize BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

def fetch_movie_details(movie_id):
    """Fetches full details for a specific movie ID."""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}"
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
    except Exception as e:
        print(f"⚠️ Error fetching details for {movie_id}: {e}")
    return None

def run_daily_pipeline():
    # 1. Calculate Target Date (Yesterday)
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"📅 TARGET DATE: {target_date} (Fetching for IST Early Morning Update)")

    all_movie_data = []
    seen_ids = set() # To prevent duplicates between loops
    
    # 2. Priority Indian Languages (hi=Hindi, ml=Malayalam, ta=Tamil, te=Telugu, kn=Kannada)
    indian_langs = ['hi', 'ml', 'ta', 'te', 'kn']
    
    for lang in indian_langs:
        print(f"🇮🇳 Fetching new {lang} language releases...")
        # Discover movies for specific language on target date
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&primary_release_date.gte={target_date}&primary_release_date.lte={target_date}&with_original_language={lang}"
        
        try:
            res = requests.get(url).json()
            for movie in res.get('results', []):
                m_id = movie['id']
                if m_id not in seen_ids:
                    details = fetch_movie_details(m_id)
                    if details:
                        all_movie_data.append(details)
                        seen_ids.add(m_id)
            time.sleep(0.1) # Respect TMDB rate limits
        except Exception as e:
            print(f"⚠️ Language loop error for {lang}: {e}")

    # 3. Global Discovery (To catch Hollywood and other international films)
    print(f"🌍 Fetching remaining global releases...")
    global_url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&primary_release_date.gte={target_date}&primary_release_date.lte={target_date}"
    
    try:
        res = requests.get(global_url).json()
        for movie in res.get('results', []):
            m_id = movie['id']
            if m_id not in seen_ids: # Ensure we don't double-count Indian movies
                details = fetch_movie_details(m_id)
                if details:
                    all_movie_data.append(details)
                    seen_ids.add(m_id)
    except Exception as e:
        print(f"❌ Global discovery failed: {e}")

    if not all_movie_data:
        print(f"ℹ️ No new releases found globally or regionally for {target_date}.")
        return

    # 4. BigQuery Upload (Incremental Load)
    df = pd.DataFrame(all_movie_data)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Keeps the original 5,000 rows intact
        autodetect=True,
    )

    print(f"📤 Appending {len(df)} new records to {TABLE_NAME}...")
    try:
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result() 
        print(f"🏆 SUCCESS! {len(df)} movies added. Total project scale increased.")
    except Exception as e:
        print(f"❌ Upload Failed: {e}")

if __name__ == "__main__":
    run_daily_pipeline()