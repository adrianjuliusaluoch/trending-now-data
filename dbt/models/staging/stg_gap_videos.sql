-- models/staging/stg_gap_videos.sql

{{
  config(materialized='view')
}}

SELECT
    keyword,
    video_id,
    title,
    channel_title,
    category_name,
    CAST(published_at AS TIMESTAMP)     AS published_at,
    duration,
    duration_secs,
    tags,
    view_count,
    like_count,
    comment_count,
    region_code

FROM `{{ env_var('DBT_YOUTUBE_TABLE') }}`
