WITH
  non_duplicates AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS n_event
  FROM
    `tc-da-1.turing_data_analytics.raw_events` ),

unique_events AS (
  SELECT
    user_pseudo_id,
    event_name,
    country,
    COUNT(*) AS event_count
  FROM
    non_duplicates
  GROUP BY
    user_pseudo_id,
    event_name,
    country)

SELECT COUNT(event_name)
FROM unique_events
WHERE event_name = 'purchase' AND country = 'India'