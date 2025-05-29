WITH
  non_duplicates AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS n_event
    FROM
      `tc-da-1.turing_data_analytics.raw_events`
  ),
-- NOTE: YOU CAN USE THE MIN FUNCTION AT TIMESTAMP TO SEE THE LAST EVENT THAT HAPPEN FROM EVENT_NAME --
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
      country
  ), -- to ensure non-duplicates and with the events I want to analyse  -- 

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
      3
  ), -- Selects the top 3 countries per nº of events --

  total_events AS (
    SELECT
      unique.country,
      unique.event_name,
      SUM(unique.event_count) AS total_event_count
    FROM
      unique_events AS unique
    GROUP BY
      unique.country,
      unique.event_name
  ) -- the total number of occurrences for each event type per country -- 

SELECT 
    CASE
    WHEN total.event_name = 'view_item' THEN 1
    WHEN total.event_name = 'add_to_cart' THEN 2
    WHEN total.event_name = 'add_shipping_info' THEN 3
    WHEN total.event_name = 'add_payment_info' THEN 4
    WHEN total.event_name = 'purchase' THEN 5
    ELSE NULL
  END AS event_order,
  total.event_name,
  SUM(CASE 
        WHEN top_countries.country_rank = 1 THEN total.total_event_count 
        ELSE 0 
      END) AS first_country_events,
  SUM(CASE 
        WHEN top_countries.country_rank = 2 THEN total.total_event_count 
        ELSE 0 
      END) AS second_country_events,
  SUM(CASE 
        WHEN top_countries.country_rank = 3 THEN total.total_event_count 
        ELSE 0 
      END) AS third_country_events
FROM 
  total_events AS total
JOIN 
  top_countries
ON 
  total.country = top_countries.country
GROUP BY 
  total.event_name
ORDER BY 
  event_order;
  -- Select the events order, events name and also the SUM of events for country rank, and with the GROUP BY clause, it puts that SUM divided by event -- 