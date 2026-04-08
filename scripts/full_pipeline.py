import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
import sys

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

def fetch_movie_details(movie_id, retries=3):
    """Fetches details with exponential backoff for network flickers."""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}"
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=25)
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
                time.sleep(10)
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            continue
    return None

def update_excel_landing_zone():
    print("\n🔄 Starting Excel Landing Zone update (Cleaned View + References)...")
    KEY_PATH = 'automation_keys/excel_automation_key.json'
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    
    try:
        creds = Credentials.from_service_account_file(KEY_PATH, scopes=scopes)
        gc = gspread.authorize(creds)
        
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_movies_cleaned`"
        df = client.query(query).to_dataframe()
        
        df['release_date'] = pd.to_datetime(df['release_date']).dt.date
        df['release_year'] = pd.to_datetime(df['release_date']).dt.year.astype(int)
        df['release_month'] = pd.to_datetime(df['release_date']).dt.strftime('%B')
        
        def get_tier(budget):
            if budget >= 150000000: return 'Blockbuster (>$150M)'
            if budget >= 50000000: return 'High ($50M-$150M)'
            if budget >= 10000000: return 'Mid ($10M-$50M)'
            return 'Low (<$10M)'
        df['budget_tier'] = df['budget'].apply(get_tier)

        sh_main = gc.open("TMDB_Cleaned_Data").sheet1
        sh_main.clear()
        set_with_dataframe(sh_main, df)
        print("✅ Main Sheet Updated.")

        genres = df['genres'].str.split(', ').explode().unique()
        genre_df = pd.DataFrame(sorted([g for g in genres if g]), columns=['genre_name'])
        genre_sheet = gc.open("TMDB_Genre_Reference").sheet1
        genre_sheet.clear()
        set_with_dataframe(genre_sheet, genre_df, include_column_header=True)

        countries = df['production_countries'].str.split(', ').explode().unique()
        country_df = pd.DataFrame(sorted([c for c in countries if c]), columns=['country_name'])
        country_sheet = gc.open("TMDB_Country_Reference").sheet1
        country_sheet.clear()
        set_with_dataframe(country_sheet, country_df, include_column_header=True)
        print("✅ Reference Sheets Updated.")
        
    except Exception as e:
        print(f"❌ Automation Error: {e}")

def refresh_recent_movies():
    print("\n🔄 Starting 30-day data refresh...")
    refresh_query = f"""
        SELECT id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}` 
        WHERE SAFE.PARSE_DATE('%Y-%m-%d', release_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    ids_to_refresh = [row.id for row in client.query(refresh_query)]
    total = len(ids_to_refresh)
    print(f"🔎 Found {total} movies to refresh.")

    refreshed_data = []
    for i, m_id in enumerate(ids_to_refresh, 1):
        details = fetch_movie_details(m_id)
        if details:
            refreshed_data.append(details)
        
        percent = (i / total) * 100
        sys.stdout.write(f"\r📊 Refresh Progress: {percent:.1f}% ({i}/{total})")
        sys.stdout.flush()
        time.sleep(0.3)

    if refreshed_data:
        print("\n📤 Uploading refreshed data to BigQuery...")
        df_refreshed = pd.DataFrame(refreshed_data)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(df_refreshed, TABLE_ID, job_config=job_config).result()
        print(f"✅ Refresh complete. {len(refreshed_data)} records updated.")

def run_daily_pipeline():
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"\n📅 TARGET DATE: {target_date}")

    all_discovery_ids = []
    indian_langs = ['hi', 'ml', 'ta', 'te', 'kn']
    search_targets = indian_langs + ['global']
    
    print(f"🇮🇳 Discovering releases...")
    for target in search_targets:
        for attempt in range(3): # Try each language 3 times
            try:
                if target == 'global':
                    url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&primary_release_date.gte={target_date}&primary_release_date.lte={target_date}"
                else:
                    url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&primary_release_date.gte={target_date}&primary_release_date.lte={target_date}&with_original_language={target}"
                
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    ids = [m['id'] for m in response.json().get('results', [])]
                    all_discovery_ids.extend(ids)
                    break 
                time.sleep(1)
            except Exception:
                print(f"⚠️ Flicker detected for {target}. Retrying ({attempt+1}/3)...")
                time.sleep(3)

    unique_ids = list(set(all_discovery_ids))
    total_new = len(unique_ids)
    
    if total_new == 0:
        print("❌ No movies found. Check network connection.")
        return

    print(f"🔎 Found {total_new} unique movies to ingest.")

    all_movie_data = []
    for i, m_id in enumerate(unique_ids, 1):
        details = fetch_movie_details(m_id)
        if details:
            all_movie_data.append(details)
        
        percent = (i / total_new) * 100
        sys.stdout.write(f"\r📊 Ingestion Progress: {percent:.1f}% ({i}/{total_new})")
        sys.stdout.flush()
        time.sleep(0.3)

    if all_movie_data:
        print("\n📤 Sending to BigQuery...")
        df_new = pd.DataFrame(all_movie_data)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(df_new, TABLE_ID, job_config=job_config).result()
        print(f"🏆 SUCCESS! {len(all_movie_data)} movies added.")
    
    refresh_recent_movies()
    update_excel_landing_zone()

if __name__ == "__main__":
    run_daily_pipeline()