# 🆔 Movie ID Discovery & Daily Automation — Explained Simply

This document explains, in plain English, what `scripts/ingest_ids.py` does and how the GitHub Actions workflow runs it automatically every day.

---

## 1. What Problem Is This Solving?

The main pipeline (`full_pipeline.py`) needs a list of **movie IDs** to know which movies to fetch details for. Instead of guessing IDs or scanning every possible number, TMDB publishes a **daily export file** containing every valid movie ID that exists on their platform. This script's only job is to **download that file and pull out a batch of IDs** to work with.

---

## 2. What `download_daily_id_export()` Does, Step by Step

```python
def download_daily_id_export():
```
This is one function that does everything — download the file, unzip it, and hand back a list of movie IDs.

### Step 1 — Figure out yesterday's date
```python
yesterday = (datetime.now() - timedelta(days=1)).strftime('%m_%d_%Y')
```
TMDB names its export files by date, like `movie_ids_07_19_2026.json.gz`. Today's file usually isn't ready yet when the job runs early in the morning, so the script asks for **yesterday's** file — the most recent one guaranteed to exist.

### Step 2 — Build the download link
```python
url = f"http://files.tmdb.org/p/exports/movie_ids_{yesterday}.json.gz"
```
This plugs yesterday's date into TMDB's fixed export URL pattern. `.json.gz` means it's a JSON file that's been compressed (zipped) to save space — TMDB's full ID list has hundreds of thousands of entries, so zipping it keeps the download small.

### Step 3 — Download the file
```python
response = requests.get(url, stream=True)
```
`stream=True` means it downloads the file in chunks rather than all at once — safer for a large file.

### Step 4 — Check if the download worked
```python
if response.status_code == 200:
```
`200` is the standard "success" code for a web request. If TMDB hasn't published today's file yet, or the URL is wrong, this will fail with a different status code — handled by the `else` block, which prints an error and returns an empty list instead of crashing.

### Step 5 — Save the zipped file to disk
```python
with open("movie_ids.json.gz", "wb") as f:
    f.write(response.content)
```
The downloaded data is saved locally as `movie_ids.json.gz` so it can be unzipped and read.

### Step 6 — Unzip and read the file, line by line
```python
with gzip.open("movie_ids.json.gz", "rb") as f:
    for line in f:
        movie_ids.append(json.loads(line))
        if len(movie_ids) >= 5000:
            break
```
Here's the important part: this isn't a normal JSON file with one big list — it's **newline-delimited JSON**, meaning every single line in the file is its own tiny JSON object (one movie ID + a bit of metadata). The script reads it line by line and converts each line into a Python object with `json.loads()`.

The `if len(movie_ids) >= 5000: break` is a **safety cap** — TMDB's full export can contain 900,000+ IDs, and this script only wants the first 5,000 for this run rather than processing the entire dataset every single day.

### Step 7 — Return the list
```python
return movie_ids
```
This is the list of movie ID objects the rest of the pipeline uses to go fetch full movie details.

---

## 3. Plain-English Summary of the Script

> "Every morning, go to TMDB's public dump of all movie IDs from yesterday, download the compressed file, unzip it, grab the first 5,000 IDs, and hand them back so the rest of the pipeline can look up full details for each one."

---

## 4. The GitHub Actions Workflow — What It Automates

This is the `.yml` file that makes the whole thing run **without anyone manually clicking a button**. It lives in `.github/workflows/` and is triggered on a schedule.

### When does it run?
```yaml
on:
  schedule:
    - cron: '0 2 * * *'
  workflow_dispatch:
```
- `cron: '0 2 * * *'` — this is a cron schedule meaning "run at 02:00 UTC, every day." GitHub spins up a fresh virtual machine at that time automatically.
- `workflow_dispatch` — this adds a manual "Run workflow" button in GitHub, so you can also trigger it by hand for testing, outside the schedule.

### What machine does it run on?
```yaml
runs-on: ubuntu-latest
```
GitHub gives the job a temporary, disposable Ubuntu Linux computer that exists only for the duration of this run, then disappears.

### Step-by-step what happens on that machine:

1. **Checkout code**
   ```yaml
   - uses: actions/checkout@v3
   ```
   Copies your GitHub repository's code onto that temporary machine, so it has access to `scripts/full_pipeline.py` and everything else.

2. **Set up Python**
   ```yaml
   - uses: actions/setup-python@v4
     with:
       python-version: '3.11'
   ```
   Installs Python 3.11 on the machine, since it doesn't come with Python pre-installed.

3. **Install dependencies**
   ```yaml
   run: |
     pip install pandas google-cloud-bigquery google-cloud-bigquery-storage pyarrow requests pandas-gbq python-dotenv gspread gspread-dataframe google-auth
   ```
   Installs every Python library the pipeline needs — BigQuery client libraries, Google Sheets libraries (`gspread`), HTTP requests, etc. — since this is a brand-new machine with nothing pre-installed.

4. **Run the pipeline — with secrets injected as environment variables**
   ```yaml
   env:
     TMDB_API_KEY: ${{ secrets.TMDB_API_KEY }}
     GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
     BQ_DATASET: ${{ secrets.BQ_DATASET }}
     BQ_TABLE: ${{ secrets.BQ_TABLE }}
     GOOGLE_APPLICATION_CREDENTIALS: ${{ github.workspace }}/gcp_key.json
   run: |
     echo '${{ secrets.GCP_SA_KEY }}' > gcp_key.json
     python scripts/full_pipeline.py
   ```
   This is where the actual work happens:
   - `secrets.*` values are stored securely in **GitHub repo settings** (Settings → Secrets and variables → Actions) — never hardcoded in the code, so API keys and credentials aren't exposed in the repository.
   - `echo '${{ secrets.GCP_SA_KEY }}' > gcp_key.json` writes your Google Cloud service account key (stored as a secret) out to an actual file called `gcp_key.json` on the temporary machine, because BigQuery's Python client needs a real file path to authenticate — not just a variable.
   - Finally, it runs `python scripts/full_pipeline.py`, which is the main pipeline that fetches new movies, cleans duplicates, and syncs everything to Google Sheets and Excel.

### Plain-English Summary of the Workflow

> "Every night at 2 AM UTC, spin up a temporary computer, load the project code onto it, install everything needed, securely load in the API keys and Google Cloud credentials, then run the full pipeline end-to-end — no human involvement required."

---

## 5. Why This Matters (Big Picture)

Together, `ingest_ids.py` and this workflow form the **fully automated front door** of the whole system:

```
GitHub Actions (2 AM UTC, daily)
      │
      ▼
ingest_ids.py  ──►  grabs fresh movie IDs from TMDB's daily export
      │
      ▼
full_pipeline.py  ──►  fetches full details for those IDs, dedupes, loads to BigQuery
      │
      ▼
Google Sheets + Excel  ──►  refreshed automatically, ready for the next morning
```

No one has to remember to run anything manually — the dashboard is simply "up to date" every time someone opens it.
