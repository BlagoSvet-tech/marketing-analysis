--Дополнительный скрипт создан для расчета показателей среднего времени на сайте, конверсии в покупку
WITH session_events AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    event_date,
    event_timestamp,
    IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source'), '(direct)') AS source,
    IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium'), '(none)') AS medium,
    IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign'), '(not set)') AS campaign,
    -- Флаг для логики First Non-Direct
    CASE WHEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') = '(none)' 
         AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') = '(direct)' THEN 1 ELSE 0 END AS is_direct,
    CASE WHEN event_name = 'purchase' AND ecommerce.purchase_revenue > 0 THEN 1 ELSE 0 END AS has_purchase
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _table_suffix BETWEEN '20201101' AND '20210131'
),
session_aggregated AS (
  SELECT
    user_pseudo_id,   --важно сохранить поле на этом этапе, так как у 1 сессии могут быть 2 user_pseudo_id
    session_id,
    event_date,
    -- Источник сессии по First Non-Direct (для соблюдения общей логики)
    ARRAY_AGG(source ORDER BY is_direct, event_timestamp LIMIT 1)[OFFSET(0)] AS source,
    ARRAY_AGG(medium ORDER BY is_direct, event_timestamp LIMIT 1)[OFFSET(0)] AS medium,
    ARRAY_AGG(campaign ORDER BY is_direct, event_timestamp LIMIT 1)[OFFSET(0)] AS campaign,
    MIN(event_timestamp) AS start_ts,
    MAX(event_timestamp) AS end_ts,
    MAX(has_purchase) AS is_purchase_session  
  FROM session_events
  WHERE session_id IS NOT NULL
  GROUP BY 1, 2, 3
)
SELECT
  event_date,
  source,
  medium,
  campaign,
  COUNT(*) AS total_sessions,
  SUM(is_purchase_session) AS purchases_count,
  SUM((end_ts - start_ts) / 1000000) AS total_duration_sec --данные в event_timestamp в микросекундах, переводим в секунды
FROM session_aggregated
GROUP BY 1, 2, 3, 4
