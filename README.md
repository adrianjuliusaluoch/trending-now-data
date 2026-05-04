# Kenya Google Trends — dbt Project

Transforms raw Google Trends data scraped via SerpAPI and stored in BigQuery
into content creator recommendation models served through Looker Studio.

## Architecture

```
GitHub Actions (every 4 hours)
    │
    ├── search.py
    │     └── Scrapes SerpAPI → deduplicates → loads to BigQuery
    │               google.trending_now_2026_may   ← cumulative, all history
    │
    └── dbt run
          └── Rebuilds these views in BigQuery (Looker Studio connects here)
                │
                ├── staging.stg_trending_now          ← stable source union
                ├── google.mart_trending_topics        ← daily recommendations
                ├── google.mart_category_pulse         ← category trend over time
                └── google.mart_keyword_recurrence     ← content series ideas
```

## Looker Studio Pages → dbt Model Mapping

| Dashboard Page | dbt Model | Key Fields |
|---|---|---|
| Today's Recommendations | `mart_trending_topics` | recommendation_score, urgency_tier, content_window |
| Category Trends | `mart_category_pulse` | daily_category_rank, keyword_count_vs_7d_avg, peak_start_hour_eat |
| Content Series Ideas | `mart_keyword_recurrence` | recurrence_label, scheduling_tip, times_trended |

## Dynamic Monthly Table Logic

Your Python script creates monthly cumulative tables:
```
google.trending_now_2026_jan  → deleted after 60 days
google.trending_now_2026_feb  → deleted after 60 days
...
google.trending_now_2026_may  → current (contains ALL data back to Jan)
```

GitHub Actions computes the table name at runtime:
```bash
SUFFIX=$(date +'%Y_%b' | tr '[:upper:]' '[:lower:]')
# → 2026_may
echo "DBT_TRENDS_TABLE=data-storage-485106.google.trending_now_${SUFFIX}" >> $GITHUB_ENV
```

dbt reads this via `env_var('DBT_TRENDS_TABLE')` in `stg_trending_now.sql`.
On June 1st it automatically becomes `trending_now_2026_jun`. No manual changes needed.

## Project Structure

```
dbt/
├── dbt_project.yml              # Project config
├── profiles.yml                 # BigQuery connection (service account)
├── models/
│   ├── staging/
│   │   ├── schema.yml           # Column docs + data tests
│   │   └── stg_trending_now.sql # Stable source view
│   └── marts/
│       ├── mart_trending_topics.sql
│       ├── mart_category_pulse.sql
│       └── mart_keyword_recurrence.sql
└── macros/
    └── current_trends_table.sql # (unused — replaced by env_var directly)
```

## Running Locally

```bash
# Install
pip install dbt-bigquery

# Set env variable (replace with current month)
export DBT_TRENDS_TABLE="data-storage-485106.google.trending_now_2026_may"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"

# Run all models
dbt run --profiles-dir . --project-dir .

# Run tests
dbt test --profiles-dir . --project-dir .

# Generate and serve docs
dbt docs generate --profiles-dir . --project-dir .
dbt docs serve
```

## Recommendation Score Formula

```
score = (search_volume × 0.5)
      + (increase_percentage × 8)
      + 4000  if active
      + 3000  if trend_duration_hrs ≤ 6   (early mover bonus)
      + 1500  if trend_duration_hrs 2–12  (still time to publish)
```

## Adding a New Month (Zero Manual Work)

Nothing to do. GitHub Actions computes the month at runtime.
dbt automatically reads from `trending_now_2026_jun` on June 1st.
Looker Studio sees fresh data because it points at the view, not the raw table.
