# Last run: Tue Jul  7 14:01:18 UTC 2026
# Import Packages
from google.cloud import bigquery
from serpapi import GoogleSearch
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import requests
from io import StringIO
import urllib3
import os
import time

now = datetime.now()
year = now.year
month = now.strftime("%b").lower()
table_suffix = f"{year}_{month}"

# Initialize BigQuery client
client = bigquery.Client(project='data-storage-485106')

# Define Locations
locations = ['Kenya']

# Collect Job Data
records = []

for location in locations:
    params = {
        "engine": "google_jobs",
        "q": "Data Analyst",
        "location": str(location),
        "google_domain": "google.com",
        "hl": "en",
        "gl": "ke",
        "api_key": os.getenv("SERPAPI_KEY")
    }

    search = GoogleSearch(params)
    results = search.get_dict()

    if "error" in results:
        print(f"SerpAPI search failed for {location}: {results['error']}, retrying once...")
        time.sleep(10)
        results = search.get_dict()

    if "error" in results:
        print(f"SerpAPI search failed again for {location}: {results['error']}")
        print(results)
        continue

    job_results = results.get("jobs_results", [])

    if not job_results:
        print(f"No jobs returned for {location}.")
        print(results)
        continue

    for job in job_results:
        records.append({
            "title": job.get("title"),
            "company_name": job.get("company_name"),
            "location": job.get("location"),
            "via": job.get("via"),
            "description": job.get("description"),
            "job_highlights": json.dumps(job.get("job_highlights")) if job.get("job_highlights") else None,
            "extensions": json.dumps(job.get("extensions")) if job.get("extensions") else None,
            "detected_extensions": json.dumps(job.get("detected_extensions")) if job.get("detected_extensions") else None,
            "apply_options": json.dumps(job.get("apply_options")) if job.get("apply_options") else None,
            "job_id": job.get("job_id"),
            "scraped_at": now
        })

# Assign DataFrame
bigdata = pd.DataFrame(records)

if bigdata.empty:
    print("No job listings collected this run — nothing to load. Exiting cleanly.")
    exit(0)

# Define Table ID
table_id = f"data-storage-485106.jobs.gsearch_jobs_ke_{table_suffix}"

if now.day == 1 or now.day == 2:

    try:
        check_sql = f"""
                    SELECT COUNT(*) AS cnt
                    FROM `{table_id}`
                    WHERE EXTRACT(MONTH FROM CAST(scraped_at AS DATETIME)) = {now.month}
                      AND EXTRACT(YEAR FROM CAST(scraped_at AS DATETIME)) = {now.year}
                    """
        check_df = client.query(check_sql).to_dataframe()
        has_current_month_data = check_df.loc[0, "cnt"] > 0
    except NotFound:
        has_current_month_data = False

    if not has_current_month_data:
        try:
            prev_month_date = now.replace(day=1) - timedelta(days=1)
            prev_table_suffix = f"{prev_month_date.year}_{prev_month_date.strftime('%b').lower()}"
            prev_table_id = f"data-storage-485106.jobs.gsearch_jobs_ke_{prev_table_suffix}"

            try:
                prev_data = client.query(
                    f"SELECT * FROM `{prev_table_id}` ORDER BY scraped_at DESC"
                ).to_dataframe()
                bigdata = pd.concat([prev_data, bigdata], ignore_index=True)
                print(f"Appended {len(prev_data)} rows from previous month table.")
            except NotFound:
                print("No previous month table found, skipping append.")

            job_bq = client.load_table_from_dataframe(
                bigdata,
                table_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            )
            job_bq.result()
            print(f"All data loaded into {table_id}, total rows: {len(bigdata)}")

        except Exception as e:
            print(f"Error during 1st-of-month load: {e}")
    else:
        job_bq = client.load_table_from_dataframe(
            bigdata,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job_bq.result()
        print(f"Normal load completed into {table_id}, rows: {len(bigdata)}")
else:
    job_bq = client.load_table_from_dataframe(
        bigdata,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job_bq.result()
    print(f"Normal load completed into {table_id}, rows: {len(bigdata)}")

# Retrieve from BigQuery
sql = f"""
        SELECT *
        FROM `{table_id}`
        ORDER BY scraped_at DESC;
      """

data = client.query(sql).to_dataframe()
print(f"Shape of dataset from BigQuery: {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Deduplicate
data.drop_duplicates(subset=['title', 'description', 'company_name', 'via'], keep='first', inplace=True)

# Define Schema
dataset_id = 'jobs'
table_id = f'gsearch_jobs_ke_{table_suffix}'

schema = [
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("via", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("job_highlights", "STRING"),
    bigquery.SchemaField("extensions", "STRING"),
    bigquery.SchemaField("detected_extensions", "STRING"),
    bigquery.SchemaField("apply_options", "STRING"),
    bigquery.SchemaField("job_id", "STRING"),
    bigquery.SchemaField("scraped_at", "TIMESTAMP")
]

table_ref = client.dataset(dataset_id).table(table_id)
table = bigquery.Table(table_ref, schema=schema)

try:
    table = client.create_table(table)
    print(f"Table {table.table_id} created successfully.")
except Exception as e:
    print(f"Table {table.table_id} failed: {e}")

# Load Deduplicated Data
table_id = f'data-storage-485106.jobs.gsearch_jobs_ke_{table_suffix}'

job_bq = client.load_table_from_dataframe(data, table_id)

while job_bq.state != 'DONE':
    time.sleep(2)
    job_bq.reload()
    print(job_bq.state)

print(f"Jobs data of shape {data.shape} successfully loaded into BigQuery.")
