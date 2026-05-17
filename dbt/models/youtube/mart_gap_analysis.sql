-- models/youtube/mart_gap_analysis.sql

{{
  config(
    materialized='view',
    schema='youtube'
  )
}}

{% set suffix = modules.datetime.datetime.now().strftime('%Y_%b').lower() %}

WITH raw AS (
    SELECT *
    FROM `{{ env_var('DBT_YOUTUBE_TABLE', 'data-storage-485106.youtube.trending_l24h_' ~ suffix) }}`
    WHERE duration_secs >= 60
      AND view_count > 0
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

    FROM raw
    GROUP BY keyword
),

trends AS (
    SELECT
        query,
        MAX(search_volume) AS search_volume
    FROM `{{ env_var('DBT_TRENDS_TABLE') }}`
    GROUP BY query
),

gap AS (
    SELECT
        a.keyword,
        t.search_volume,
        a.video_count,
        a.median_views,
        a.avg_views,
        a.max_views,
        a.total_likes,
        a.total_comments,
        ROUND(t.search_volume / (a.median_views + 1), 2)   AS gap_score,

        CASE
            WHEN ROUND(t.search_volume / (a.median_views + 1), 2) > 50
                THEN 'Strong Gap'
            WHEN ROUND(t.search_volume / (a.median_views + 1), 2) > 10
                THEN 'Good Opportunity'
            WHEN ROUND(t.search_volume / (a.median_views + 1), 2) > 1
                THEN 'Competitive'
            ELSE
                'Saturated'
        END                                                 AS opportunity,

        CURRENT_TIMESTAMP()                                 AS collected_at

    FROM aggregated a
    INNER JOIN trends t
        ON LOWER(a.keyword) = LOWER(t.query)
)

SELECT * FROM gap
ORDER BY gap_score DESC
