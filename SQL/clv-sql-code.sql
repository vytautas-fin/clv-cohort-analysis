WITH
-- 'ETL' prep on raw_events table.
modified_raw_events AS (
    SELECT 
      DATE(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date))) AS event_date
      ,event_timestamp
      ,TIMESTAMP(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', TIMESTAMP_MICROS(event_timestamp))) AS event_datetime
      ,event_name
      ,user_pseudo_id
      ,category
      ,country
      ,purchase_revenue_in_usd
      ,campaign
    FROM `tc-da-1.turing_data_analytics.raw_events`
    WHERE 1=1
      AND DATE(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date))) < '2021-01-31' -- Exclude final day from dataset as it starts a new cohort (2021-01-31 Sunday)
),

-- Create date parameters
date_param AS (
    SELECT
      DATE_TRUNC(MAX(event_date), WEEK) AS max_week -- 2021-01-24
    FROM modified_raw_events
),

-- Registration (earliest event) week
registration AS (
    SELECT
      MIN(DATE_TRUNC(event_date, WEEK)) OVER (PARTITION BY user_pseudo_id) AS registration_week
      ,modified_raw_events.*
    FROM modified_raw_events
),

-- Average amount spent per user by weekly cohorts
avg_spent_cohort AS (
    SELECT registration_week,
      COUNT(DISTINCT user_pseudo_id) AS new_users,
      SUM(IF(DATE_TRUNC(event_date, WEEK) = registration_week, purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) AS week_0,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) = 0 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 1 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_1,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 1 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 2 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_2,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 2 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 3 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_3,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 3 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 4 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_4,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 4 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 5 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_5,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 5 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 6 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_6,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 6 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 7 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_7,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 7 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 8 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_8,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 8 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 9 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_9,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 9 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 10 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_10,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 10 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 11 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_11,
      CASE
        WHEN DATE_DIFF(max_week, registration_week, WEEK) <= 11 THEN NULL
        ELSE SUM(IF(DATE_TRUNC(event_date, WEEK) = DATE_ADD(registration_week, INTERVAL 12 WEEK), purchase_revenue_in_usd, 0)) / COUNT(DISTINCT user_pseudo_id) END AS week_12
    FROM registration reg, date_param
    GROUP BY registration_week, max_week
    ORDER BY registration_week ASC
)

SELECT * FROM avg_spent_cohort