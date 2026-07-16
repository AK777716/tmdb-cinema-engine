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

# Load local environment variables if present
load_dotenv()

# Configuration Variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
TABLE_NAME = os.getenv("BQ_TABLE")
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
API_KEY = os.getenv("TMDB_API_KEY")

# Global placeholder initialized safely within the runtime execution context
client = None

def get_gcp_credentials():
    """
    Dynamically identifies and returns the operational service account credential path
    for both remote virtual execution environments and local workstations.
    """
    env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    # 1. Target the path populated by GitHub Actions runner first
    if env_path and os.path.exists(env_path):
        return env_path
        
    # 2. Check root execution workspace directory alternative
    if os.path.exists("gcp_key.json"):
        return "gcp_key.json"
        
    # 3. Workstation local development fallback path
    fallback_path = "automation_keys/excel_automation_key.json"
    if os.path.exists(fallback_path):
        return fallback_path
        
    raise FileNotFoundError("🔴 Critical System Error: Unable to locate a valid GCP Service Account JSON key.")

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
    print("\n🔄 Starting Full Excel Sync (Main + 3 Reference Files)...")
    
    # Locate operating credential asset file path
    KEY_PATH = get_gcp_credentials()
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    sync_stats = [] 

    try:
        creds = Credentials.from_service_account_file(KEY_PATH, scopes=scopes)
        gc = gspread.authorize(creds)

        # --- 1. MAIN DATA ---
        print("📊 Syncing Main Data File...")
        query_main = client.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.dim_movies_cleaned` ")
        df_main = query_main.to_dataframe()
        df_main['release_date'] = pd.to_datetime(df_main['release_date']).dt.date
        df_main['release_year'] = pd.to_datetime(df_main['release_date']).dt.year.fillna(0).astype(int)
        df_main['release_month'] = pd.to_datetime(df_main['release_date']).dt.strftime('%B')
        
        sh_main = gc.open("TMDB_Cleaned_Data").sheet1 
        sh_main.clear()
        set_with_dataframe(sh_main, df_main, resize=True)
        sync_stats.append({"File": "Main Data", "Rows": len(df_main)})
        print(f"✅ 1/4: Main Sheet Updated.")

        # --- 2. GENRE REFERENCE ---
        print("📊 Syncing Genre Reference...")
        df_gen = client.query(f"""
            SELECT DISTINCT genre as genre_name 
            FROM `{PROJECT_ID}.{DATASET_ID}.dim_movies_cleaned`, 
            UNNEST(SPLIT(genres, ', ')) as genre 
            WHERE genre IS NOT NULL AND genre != 'None' AND genre != '' 
            ORDER BY genre_name
        """).to_dataframe()
        sh_gen = gc.open("TMDB_Genre_Reference").sheet1
        sh_gen.clear()
        set_with_dataframe(sh_gen, df_gen, resize=True)
        sync_stats.append({"File": "Genre Ref", "Rows": len(df_gen)})
        print(f"✅ 2/4: Genre File Updated.")

        # --- 3. COUNTRY REFERENCE ---
        print("📊 Syncing Country Reference...")
        df_coun = client.query(f"""
            SELECT DISTINCT country as country_name 
            FROM `{PROJECT_ID}.{DATASET_ID}.dim_movies_cleaned`, 
            UNNEST(SPLIT(production_countries, ', ')) as country 
            WHERE country IS NOT NULL AND country != 'None' AND country != '' 
            ORDER BY country_name
        """).to_dataframe()
        sh_coun = gc.open("TMDB_Country_Reference").sheet1
        sh_coun.clear()
        set_with_dataframe(sh_coun, df_coun, resize=True)
        sync_stats.append({"File": "Country Ref", "Rows": len(df_coun)})
        print(f"✅ 3/4: Country File Updated.")

        # --- 4. COMPANY REFERENCE ---
        print("📊 Processing Consolidated Company Mapping...")
        df_comp_raw = client.query(f"""
            SELECT company as company_name, COUNT(*) as movie_count
            FROM `{PROJECT_ID}.{DATASET_ID}.dim_movies_cleaned`, 
            UNNEST(SPLIT(production_companies, ', ')) as company 
            WHERE company IS NOT NULL AND company NOT IN ('None', '', 'Unknown', 'unknown') 
            GROUP BY company_name
        """).to_dataframe()

        mapping = {
            "20th Century Fox": "20th Century Studios", "20th Century Fox Animation": "20th Century Studios",
            "Fox Searchlight Pictures": "Searchlight Pictures", "Fox 2000 Pictures": "20th Century Studios",
            "BBC Film": "BBC", "BBC Studios": "BBC", "BBC Storyville": "BBC",
            "Amazon MGM Studios": "Amazon Studios", "Apple Studios": "Apple", "Apple Corps": "Apple",
            "Lions Gate Films": "Lionsgate", "Lions Gate Films (US)": "Lionsgate", "Lionsgate Home Entertainment": "Lionsgate",
            "HBO Films": "HBO", "HBO Documentary Films": "HBO",
            "Columbia Pictures Television": "Columbia Pictures", "Columbia TriStar Television": "Columbia Pictures",
            "Columbia TriStar Filmes do Brasil": "Columbia Pictures",
            "Canal+ España": "Canal+", "Canal+ Polska": "Canal+", "Canal+ Droits Audiovisuels": "Canal+",
            "Balaji Telefilms": "Balaji Motion Pictures", "Anil Kapoor Film & Communication Network": "AKFCN",
            "Universal Pictures": "Universal", "Focus Features": "Universal",
            "Warner Bros. Pictures": "Warner Bros", "Warner Bros. Animation": "Warner Bros",
            "Paramount Players": "Paramount Pictures", "Walt Disney Pictures": "Disney", 
            "Pixar Animation Studios": "Disney", "Marvel Studios": "Disney", "Lucasfilm Ltd.": "Disney"
        }

        df_comp_raw['company_name'] = df_comp_raw['company_name'].replace(mapping)
        df_comp_final = df_comp_raw.groupby('company_name')['movie_count'].sum().reset_index()
        df_comp_final = df_comp_final.sort_values(by='movie_count', ascending=False)

        sh_comp = gc.open("TMDB_Company_Reference").sheet1
        sh_comp.clear()
        set_with_dataframe(sh_comp, df_comp_final, resize=True)
        sync_stats.append({"File": "Company Ref", "Rows": len(df_comp_final)})
        print(f"✅ 4/4: Company File Updated.")

        print("\n" + "="*40)
        print("📁 FINAL EXCEL SYNC SUCCESS REPORT")
        print("="*40)
        for item in sync_stats:
            print(f"🔹 {item['File']}: {item['Rows']} rows synced.")
        print("="*40)
        print("✅ STATUS: ALL FILES SYNCHRONIZED SUCCESSFULLY!")
        print("="*40 + "\n")

    except Exception as e:
        print(f"❌ Excel Automation Error Detail: {str(e)}")
        raise e

def refresh_recent_movies():
    print("\n🔄 Starting 30-day data refresh (Background Task)...")
    refresh_query = f"""
        SELECT id FROM `{TABLE_ID}` 
        WHERE SAFE_CAST(release_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
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
    global client
    
    # Secure runtime environment verification
    credential_file = get_gcp_credentials()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_file
    
    # Initialize BigQuery engine safely under confirmed credentials context
    client = bigquery.Client(project=PROJECT_ID)
    
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
    
    if unique_ids:
        print(f"🔎 Found {len(unique_ids)} unique movies. Ingesting...")
        all_movie_data = [fetch_movie_details(m_id) for m_id in unique_ids]
        all_movie_data = [d for d in all_movie_data if d]
        
        if all_movie_data:
            pd.DataFrame(all_movie_data).to_gbq(TABLE_ID, project_id=PROJECT_ID, if_exists='append')
            print(f"📤 {len(all_movie_data)} records added.")

    print("🧹 Cleaning Duplicates in BigQuery Data Warehouse...")
    dedup_sql = f"""
        CREATE OR REPLACE TABLE `{TABLE_ID}` AS
        SELECT * EXCEPT(row_num) FROM (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY id 
                ORDER BY (budget + revenue) DESC, ingested_at DESC
            ) as row_num
            FROM `{TABLE_ID}`
        ) WHERE row_num = 1
    """
    client.query(dedup_sql).result()

    update_excel_landing_zone()
    print("🚀 DASHBOARD SYNCED!")
    refresh_recent_movies()

if __name__ == "__main__":
    run_daily_pipeline()