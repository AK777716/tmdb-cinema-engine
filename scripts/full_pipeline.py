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
    return None

def update_excel_landing_zone():
    print("\n🔄 Starting Excel Landing Zone update...")
    KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "automation_keys/excel_automation_key.json")
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    
    try:
        creds = Credentials.from_service_account_file(KEY_PATH, scopes=scopes)
        gc = gspread.authorize(creds)
        
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_movies_cleaned`"
        df = client.query(query).to_dataframe()
        
        # Data formatting for Excel
        df['release_date'] = pd.to_datetime(df['release_date']).dt.date
        df['release_year'] = pd.to_datetime(df['release_date']).dt.year.fillna(0).astype(int)
        df['release_month'] = pd.to_datetime(df['release_date']).dt.strftime('%B')
        
        sh_main = gc.open("TMDB_Cleaned_Data").sheet1
        sh_main.clear()
        set_with_dataframe(sh_main, df)
        print("✅ Excel Landing Zone Updated.")
    except Exception as e:
        print(f"❌ Excel Automation Error: {e}")

def refresh_recent_movies():
    print("\n🔄 Starting 30-day data refresh (Background Task)...")
    refresh_query = f"""
        SELECT id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}` 
        WHERE SAFE.PARSE_DATE('%Y-%m-%d', release_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    ids_to_refresh = [row.id for row in client.query(refresh_query)]
    
    refreshed_data = []
    for i, m_id in enumerate(ids_to_refresh, 1):
        details = fetch_movie_details(m_id)
        if details:
            refreshed_data.append(details)
        sys.stdout.write(f"\r📊 Refresh Progress: {(i/len(ids_to_refresh))*100:.1f}%")
        sys.stdout.flush()
        time.sleep(0.2)

    if refreshed_data:
        df_refreshed = pd.DataFrame(refreshed_data)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df_refreshed, TABLE_ID, job_config=job_config).result()
        print(f"\n✅ Refresh complete. {len(refreshed_data)} records refreshed.")

def run_daily_pipeline():
    # 1. SCAN RANGE (3-day window for stability)
    today = datetime.now()
    start_date = (today - timedelta(days=3)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    print(f"\n📅 SCANNING RANGE: {start_date} to {end_date}")

    all_discovery_ids = []
    search_targets = ['hi', 'ml', 'ta', 'te', 'kn', 'global']
    
    for target in search_targets:
        try:
            url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&primary_release_date.gte={start_date}&primary_release_date.lte={end_date}"
            if target != 'global':
                url += f"&with_original_language={target}"
            
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                all_discovery_ids.extend([m['id'] for m in response.json().get('results', [])])
        except Exception as e:
            print(f"⚠️ Flicker on {target}: {e}")

    unique_ids = list(set(all_discovery_ids))
    
    # 2. INGEST NEW MOVIES
    if unique_ids:
        print(f"🔎 Found {len(unique_ids)} unique movies. Ingesting...")
        all_movie_data = [fetch_movie_details(m_id) for m_id in unique_ids]
        all_movie_data = [d for d in all_movie_data if d]
        
        if all_movie_data:
            pd.DataFrame(all_movie_data).to_gbq(TABLE_ID, project_id=PROJECT_ID, if_exists='append')
            print(f"📤 {len(all_movie_data)} records added.")

    # 3. AUTO-DEDUPLICATE (Fixes the duplicate issue)
    print("🧹 Cleaning Duplicates in BigQuery...")
    dedup_sql = f"""
        CREATE OR REPLACE TABLE `{TABLE_ID}` AS
        SELECT * EXCEPT(row_num) FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) as row_num
            FROM `{TABLE_ID}`
        ) WHERE row_num = 1
    """
    client.query(dedup_sql).result()

    # 4. HIGH PRIORITY: Update Dashboard FIRST
    update_excel_landing_zone()
    print("🚀 DASHBOARD SYNCED!")

    # 5. LOW PRIORITY: Slow background refresh
    refresh_recent_movies()

# --- THE ENTRY POINT (DO NOT FORGET THIS) ---
if __name__ == "__main__":
    run_daily_pipeline()