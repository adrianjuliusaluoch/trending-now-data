-- models/marts/mart_keyword_recurrence.sql
--
-- Tracks keywords that trend repeatedly over time.
-- A keyword appearing multiple times = reliable recurring topic =
-- content creator can build a series or scheduled post around it.
--
-- Examples from the actual data:
--   "weather"     → 10+ appearances, always Morning, Climate
--   "flashscore"  → 5+ appearances, always Night, Sports
--   "nba"         → appears every week, early morning
--   "prediction"  → Sports, every match weekend
--
-- One row = one unique keyword with its full recurrence profile.
--
-- Looker Studio: "Content Series Ideas" page
--   - Table of recurring keywords with scheduling guidance
--   - "Post this every Friday evening" type insights

{{
  config(materialized='view')
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_trending_now') }}
),

recurrence AS (
    SELECT
        query,
        categories,

        COUNT(*)                                                        AS times_trended,

        MIN(trend_date)                                                 AS first_seen_date,
        MAX(trend_date)                                                 AS last_seen_date,
        DATE_DIFF(MAX(trend_date), MIN(trend_date), DAY)                AS active_span_days,

        ROUND(AVG(search_volume), 0)                                    AS avg_search_volume,
        MAX(search_volume)                                              AS peak_search_volume,
        ROUND(AVG(increase_percentage), 0)                              AS avg_increase_pct,
        ROUND(AVG(trend_duration_hrs), 1)                               AS avg_trend_duration_hrs,

        -- Most common time of day this keyword trends
        APPROX_TOP_COUNT(
            CASE
                WHEN start_hour_eat BETWEEN 5  AND 11 THEN 'Morning'
                WHEN start_hour_eat BETWEEN 12 AND 16 THEN 'Afternoon'
                WHEN start_hour_eat BETWEEN 17 AND 20 THEN 'Evening'
                ELSE 'Night'
            END, 1
        )[OFFSET(0)].value                                              AS most_common_time_of_day,

        -- Most common day of week this keyword trends
        APPROX_TOP_COUNT(day_of_week, 1)[OFFSET(0)].value              AS most_common_day,

        -- Is it currently active in the latest run
        LOGICAL_OR(active)                                              AS currently_active,
        MAX(start_date)                                                 AS last_trend_start,
        DATE_DIFF(CURRENT_DATE(), MAX(trend_date), DAY)                 AS days_since_last_trend

    FROM base
    GROUP BY query, categories
)

SELECT
    *,

    -- Content series classification
    CASE
        WHEN times_trended >= 10 AND active_span_days >= 30
            THEN 'Series Material — Trends Regularly'
        WHEN times_trended >= 5
            THEN 'Recurring — Worth Scheduling'
        WHEN times_trended >= 2
            THEN 'Appeared Twice — Keep Watching'
        ELSE
            'One-Off — Reactive Content Only'
    END                                                                 AS recurrence_label,

    -- Scheduling tip for the creator
    CONCAT(
        'Best time: ', most_common_time_of_day, ' on ', most_common_day
    )                                                                   AS scheduling_tip

FROM recurrence
ORDER BY times_trended DESC, avg_search_volume DESC
