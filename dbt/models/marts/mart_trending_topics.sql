-- models/marts/mart_trending_topics.sql
--
-- Core recommendation model for content creators.
-- Each row is one trending keyword scored and labeled so creators know:
--   - What to post about right now
--   - How urgent it is
--   - How much time they have before the trend dies
--
-- Looker Studio: "Today's Recommendations" page
--   - Leaderboard of hottest keywords
--   - Topic cards filtered by category
--   - Urgency badges (Viral / Hot / Rising / Active / Ended)

{{
  config(materialized='view')
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_trending_now') }}
),

scored AS (
    SELECT
        query,
        start_date,
        end_date,
        active,
        search_volume,
        increase_percentage,
        categories,
        trend_breakdown,
        trend_duration_hrs,
        trend_date,
        start_hour_eat,
        day_of_week,
        month_name,

        -- ── Recommendation Score ──────────────────────────────────────
        -- search_volume     → base popularity (how many people care)
        -- increase_percentage → momentum (how fast it is rising)
        -- active bonus      → still trending = act now
        -- recency bonus     → started within 6hrs = early mover advantage
        -- duration bonus    → 2–12hr sweet spot = still time to publish
        -- ─────────────────────────────────────────────────────────────
        ROUND(
            (search_volume * 0.5)
            + (increase_percentage * 8)
            + (CASE WHEN active                               THEN 4000 ELSE 0 END)
            + (CASE WHEN trend_duration_hrs <= 6              THEN 3000 ELSE 0 END)
            + (CASE WHEN trend_duration_hrs BETWEEN 2 AND 12  THEN 1500 ELSE 0 END)
        , 0)                                                            AS recommendation_score,

        -- ── Content Window ────────────────────────────────────────────
        CASE
            WHEN active AND trend_duration_hrs <= 3  THEN 'Act Now — Just Started'
            WHEN active AND trend_duration_hrs <= 12 THEN 'Still Hot — Publish Today'
            WHEN active AND trend_duration_hrs <= 24 THEN 'Fading — Quick Content Only'
            WHEN active                              THEN 'Long Tail — Deep Content OK'
            ELSE                                          'Ended — Historical Reference'
        END                                                             AS content_window,

        -- ── Urgency Tier ─────────────────────────────────────────────
        CASE
            WHEN search_volume >= 10000 AND active THEN 'Viral'
            WHEN search_volume >= 2000  AND active THEN 'Hot'
            WHEN search_volume >= 500   AND active THEN 'Rising'
            WHEN active                            THEN 'Active'
            ELSE                                        'Ended'
        END                                                             AS urgency_tier,

        -- ── Time of day ───────────────────────────────────────────────
        CASE
            WHEN start_hour_eat BETWEEN 5  AND 11 THEN 'Morning'
            WHEN start_hour_eat BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN start_hour_eat BETWEEN 17 AND 20 THEN 'Evening'
            ELSE                                       'Night'
        END                                                             AS time_of_day

    FROM base
)

SELECT
    *,
    -- Rank within category — Looker uses this for "Top 5 per niche" cards
    ROW_NUMBER() OVER (
        PARTITION BY categories
        ORDER BY recommendation_score DESC
    )                                                                   AS rank_in_category

FROM scored
ORDER BY recommendation_score DESC
