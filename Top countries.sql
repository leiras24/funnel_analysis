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
  WHERE
    n_event = 1 AND event_name IN ('add_to_cart',
      'add_shipping_info',
      'purchase',
      'view_item',
      'add_payment_info')
  GROUP BY
    user_pseudo_id,
    event_name,
    country),

top_countries AS (
  SELECT
    country,
    COUNT(event_name) AS total_event_count,
    DENSE_RANK() OVER (ORDER BY COUNT(event_name) DESC) AS country_rank
  FROM
    unique_events
  GROUP BY
    country
  ORDER BY
    country_rank
  LIMIT
    3)

SELECT *
FROM top_countries