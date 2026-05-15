-- models/marts/mart_gap_analysis.sql

{{
  config(materialized='view')
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_gap_videos') }}
),

aggregated AS (
    SELECT
        keyword,
        COUNT(video_id)                                     AS video_count,
        SUM(view_count)                                     AS total_views,
        ROUND(AVG(view_count), 0)                           AS avg_views,
        APPROX_QUANTILES(view_count, 2)[OFFSET(1)]          AS median_views,
        MAX(view_count)                                     AS max_views,
        SUM(like_count)                                     AS total_likes,
        SUM(comment_count)                                  AS total_comments

    FROM base
    GROUP BY keyword
),

gap AS (
    SELECT
        a.*,
        t.search_volume,
        ROUND(t.search_volume / (a.median_views + 1), 2)   AS gap_score,

        CASE
            WHEN ROUND(t.search_volume / (a.median_views + 1), 2) > 50
                THEN 'Strong Gap'
            WHEN ROUND(t.search_volume / (a.median_views + 1), 2) > 10
                THEN 'Good Opportunity'
            WHEN ROUND(t.search_volume / (a.median_views + 1), 2) > 1
                THEN 'Competitive'
            ELSE 'Saturated'
        END                                                 AS opportunity,

        CURRENT_TIMESTAMP()                                 AS collected_at

    FROM aggregated a
    LEFT JOIN `{{ env_var('DBT_TRENDS_TABLE') }}` t
        ON LOWER(a.keyword) = LOWER(t.query)
)

SELECT * FROM gap
ORDER BY gap_score DESC
