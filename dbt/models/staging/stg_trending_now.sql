-- models/staging/stg_trending_now.sql
--
-- Stable entry point for all downstream mart models.
-- Always reads from the current month's cumulative BigQuery table
-- which contains all history back to January 2026.
--
-- The table name is injected at runtime via DBT_TRENDS_TABLE env variable
-- set in GitHub Actions — no manual changes needed when a new month starts.
-- Looker Studio connects to the marts above this, never to raw tables directly.

{{
  config(materialized='view')
}}

SELECT
    -- Core fields (exact column names from BigQuery schema)
    query,
    start_date,
    end_date,
    active,
    search_volume,
    increase_percentage,
    categories,
    trend_breakdown,

    -- Derived: how long the keyword trended in hours
    TIMESTAMP_DIFF(
        COALESCE(end_date, CURRENT_TIMESTAMP()),
        start_date,
        HOUR
    )                                                                           AS trend_duration_hrs,

    -- Derived: Kenya time fields (UTC+3)
    DATE(DATETIME(start_date, 'Africa/Nairobi'))                                AS trend_date,
    EXTRACT(HOUR FROM DATETIME(start_date, 'Africa/Nairobi'))                   AS start_hour_eat,
    FORMAT_DATE('%A', DATE(DATETIME(start_date, 'Africa/Nairobi')))             AS day_of_week,
    FORMAT_DATE('%B', DATE(DATETIME(start_date, 'Africa/Nairobi')))             AS month_name,
    FORMAT_DATE('%W', DATE(DATETIME(start_date, 'Africa/Nairobi')))             AS week_number

FROM `{{ env_var('DBT_TRENDS_TABLE') }}`
