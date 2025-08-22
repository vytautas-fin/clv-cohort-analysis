WITH
-- 'ETL' prep on raw_events table.
modified_raw_events AS (
    SELECT
        DATE(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date))) AS event_date,
        user_pseudo_id,
        purchase_revenue_in_usd
    FROM `tc-da-1.turing_data_analytics.raw_events`
    WHERE
        DATE(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date))) < '2021-01-31'
),

-- Find the registration week for each user
user_registration AS (
    SELECT
        user_pseudo_id,
        MIN(DATE_TRUNC(event_date, WEEK)) AS registration_week
    FROM modified_raw_events
    GROUP BY 1
),

-- Calculate the week number for each event relative to the user's registration
event_with_week_number AS (
    SELECT
        reg.registration_week,
        events.user_pseudo_id,
        events.purchase_revenue_in_usd,
        DATE_DIFF(DATE_TRUNC(events.event_date, WEEK), reg.registration_week, WEEK) AS week_number
    FROM modified_raw_events AS events
    JOIN user_registration AS reg
      ON events.user_pseudo_id = reg.user_pseudo_id
),

-- Calculate total revenue per cohort per week
weekly_cohort_revenue AS (
    SELECT
        registration_week,
        week_number,
        SUM(purchase_revenue_in_usd) AS total_revenue
    FROM event_with_week_number
    GROUP BY 1, 2
),

-- Count the number of new users in each cohort
cohort_size AS (
    SELECT
        registration_week,
        COUNT(DISTINCT user_pseudo_id) AS new_users
    FROM user_registration
    GROUP BY 1
),

-- Final calculation: average sales per user for each cohort and week (long format)
final_long_data AS (
    SELECT
        rev.registration_week,
        size.new_users,
        rev.week_number,
        rev.total_revenue / size.new_users AS avg_sales_per_user
    FROM weekly_cohort_revenue AS rev
    JOIN cohort_size AS size
      ON rev.registration_week = size.registration_week
)

-- Final step: Pivot the long data into a wide cohort table
SELECT
    registration_week,
    new_users,
    MAX(IF(week_number = 0, avg_sales_per_user, NULL)) AS week_0,
    MAX(IF(week_number = 1, avg_sales_per_user, NULL)) AS week_1,
    MAX(IF(week_number = 2, avg_sales_per_user, NULL)) AS week_2,
    MAX(IF(week_number = 3, avg_sales_per_user, NULL)) AS week_3,
    MAX(IF(week_number = 4, avg_sales_per_user, NULL)) AS week_4,
    MAX(IF(week_number = 5, avg_sales_per_user, NULL)) AS week_5,
    MAX(IF(week_number = 6, avg_sales_per_user, NULL)) AS week_6,
    MAX(IF(week_number = 7, avg_sales_per_user, NULL)) AS week_7,
    MAX(IF(week_number = 8, avg_sales_per_user, NULL)) AS week_8,
    MAX(IF(week_number = 9, avg_sales_per_user, NULL)) AS week_9,
    MAX(IF(week_number = 10, avg_sales_per_user, NULL)) AS week_10,
    MAX(IF(week_number = 11, avg_sales_per_user, NULL)) AS week_11,
    MAX(IF(week_number = 12, avg_sales_per_user, NULL)) AS week_12
FROM final_long_data
GROUP BY
    registration_week,
    new_users
ORDER BY
    registration_week;