-- models/marts/mart_category_pulse.sql
--
-- Daily category-level aggregations for trend over time analysis.
-- One row = one category on one day.
--
-- Answers:
--   - Which niches are growing week over week?
--   - What time of day is each category most active? (best time to post)
--   - Which category dominated today?
--
-- Looker Studio: "Category Trends" page
--   - Line charts of keyword volume per category over time
--   - Heatmap: category × hour of day
--   - Daily category rank table

{{
  config(materialized='view')
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_trending_now') }}
),

daily_category AS (
    SELECT
        categories,
        trend_date,
        day_of_week,
        week_number,
        month_name,
        start_hour_eat,
        search_volume,
        increase_percentage,
        active,
        trend_duration_hrs,
        query

    FROM base
),

aggregated AS (
    SELECT
        categories,
        trend_date,
        day_of_week,
        week_number,
        month_name,

        -- Volume metrics
        COUNT(*)                                                AS total_keywords,
        SUM(search_volume)                                      AS total_search_volume,
        ROUND(AVG(search_volume), 0)                            AS avg_search_volume,
        MAX(search_volume)                                      AS peak_search_volume,

        -- Momentum metrics
        ROUND(AVG(increase_percentage), 0)                      AS avg_increase_pct,
        MAX(increase_percentage)                                AS max_increase_pct,

        -- Activity metrics
        COUNTIF(active)                                         AS active_keyword_count,
        ROUND(AVG(trend_duration_hrs), 1)                       AS avg_trend_duration_hrs,

        -- Best hour to post in this category (most common trending start hour)
        APPROX_TOP_COUNT(start_hour_eat, 1)[OFFSET(0)].value   AS peak_start_hour_eat,

        -- Top keyword of the day per category by search volume
        ARRAY_AGG(query ORDER BY search_volume DESC LIMIT 1)[OFFSET(0)]
                                                                AS top_keyword

    FROM daily_category
    GROUP BY
        categories,
        trend_date,
        day_of_week,
        week_number,
        month_name
)

SELECT
    *,

    -- How this category's keyword count compares to its own 7-day average
    -- Positive = above average activity, negative = quieter than usual
    ROUND(
        total_keywords - AVG(total_keywords) OVER (
            PARTITION BY categories
            ORDER BY trend_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        )
    , 1)                                                        AS keyword_count_vs_7d_avg,

    -- Which category was most active on each day (1 = top category that day)
    RANK() OVER (
        PARTITION BY trend_date
        ORDER BY total_search_volume DESC
    )                                                           AS daily_category_rank

FROM aggregated
ORDER BY trend_date DESC, total_search_volume DESC
