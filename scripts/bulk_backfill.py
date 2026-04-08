import requests
import pandas as pd
from google.cloud import bigquery
import time
from datetime import datetime
import urllib3

# This line hides the "InsecureRequestWarning" caused by verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
API_KEY = 'd242a33525f5415d604aa35b475c1033' 
PROJECT_ID = 'cinema-intelligence-engine'
DATASET_ID = 'cinema_intelligence'
TABLE_ID = 'movies_raw'

client = bigquery.Client(project=PROJECT_ID)

def fetch_movies_by_criteria(year, language=None, region=None, pages=5):
    all_data = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    base_url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&primary_release_year={year}&sort_by=popularity.desc"
    if language:
        base_url += f"&with_original_language={language}"
    if region:
        base_url += f"&region={region}"

    for page in range(1, pages + 1):
        try:
            # timeout=20 and verify=False to prevent hangs and SSL blocks
            response = requests.get(f"{base_url}&page={page}", headers=headers, timeout=20, verify=False)
            
            if response.status_code == 200:
                results = response.json().get('results', [])
                all_data.extend(results)
                print(".", end="", flush=True) 
            
            time.sleep(0.4) # Polite delay to avoid 10054 errors
        except Exception:
            # Skip bad pages to keep the scrape moving
            continue
            
    return all_data

# --- MAIN EXECUTION ---
start_time = time.time()
master_list = []

# 🔥 BATCH A: 2000 to 2012
#years_to_track = range(2000, 2013) 
#BATCH B: 2013 to 2026
years_to_track = range(2013, 2027) 

print(f"🚀 STARTING GLOBAL + REGIONAL DEEP SCRAPE (BATCH A: 2000-2012)")
print(f"--------------------------------------------------------------")

for year in years_to_track:
    print(f"📅 Processing Year: {year}", end=" ")
    
    # 1. GLOBAL NET (All languages: English, Korean, Spanish, etc.)
    # 40 pages = 800 most popular movies globally
    master_list.extend(fetch_movies_by_criteria(year, pages=40))
    
    # 2. HINDI (BOLLYWOOD) DEEP DIVE
    # 15 pages = 300 movies
    master_list.extend(fetch_movies_by_criteria(year, language='hi', region='IN', pages=15))
    
    # 3. MALAYALAM DEEP DIVE
    # 10 pages = 200 movies
    master_list.extend(fetch_movies_by_criteria(year, language='ml', region='IN', pages=10))
    
    # 4. TAMIL & TELUGU
    # 5 pages each = 200 movies combined
    master_list.extend(fetch_movies_by_criteria(year, language='ta', region='IN', pages=5))
    master_list.extend(fetch_movies_by_criteria(year, language='te', region='IN', pages=5))
    
    current_count = len(master_list)
    print(f" ✅ Year Done. Session Total: {current_count}")

# --- DATA CLEANING & UPLOAD ---
if not master_list:
    print("\n❌ ERROR: No data was collected. Check your internet or API key.")
    exit()

print(f"\nFinalizing data for BigQuery...")
df = pd.DataFrame(master_list)

# 1. Deduplicate by movie ID
df = df.drop_duplicates(subset=['id'])

# 2. Match your BigQuery schema exactly
# Note: Ensure these columns exist in your 'movies_raw' table
required_columns = ['id', 'title', 'release_date', 'original_language', 'popularity', 'vote_average', 'vote_count']

# Filter only for columns that exist in the dataframe to avoid errors
existing_cols = [col for col in required_columns if col in df.columns]
df = df[existing_cols]

# 3. Add string-based timestamp
df['ingested_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(f"📤 Uploading {len(df)} UNIQUE movies to BigQuery...")

# 4. Final Push
try:
    df.to_gbq(f"{DATASET_ID}.{TABLE_ID}", project_id=PROJECT_ID, if_exists='append')
    print(f"🏆 SUCCESS! Batch A stored in BigQuery.")
except Exception as e:
    print(f"❌ BigQuery Upload Failed: {e}")

total_duration = round((time.time() - start_time) / 60, 2)
print(f"⏱️ Total Duration for Batch A: {total_duration} minutes.")