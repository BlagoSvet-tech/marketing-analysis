--Основной запрос
WITH all_events AS (
  SELECT 
    user_pseudo_id, 
    event_date,
    datetime(timestamp_micros(event_timestamp)) as event_time, 
    event_name, 
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id,
    ifnull((select value.string_value from unnest(event_params) where key = 'medium'), '(none)') as event_medium,
    ifnull((select value.string_value from unnest(event_params) where key = 'source'), '(direct)') as event_source,
    ifnull((select value.string_value from unnest(event_params) where key = 'campaign'), '(not set)') as event_campaign,
    geo.country,           
    geo.city,              
    device.category as device_category, 
    ecommerce.purchase_revenue
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _table_suffix BETWEEN '20201101' AND '20210131'  --данные за 3 месяца
), 
events_flags AS (
  SELECT *,
    CASE WHEN event_medium = '(none)' AND event_source = '(direct)' THEN 1 ELSE 0 END AS is_direct, --Прямой или нет
    CASE WHEN regexp_contains(lower(event_medium), r'cpc|ppc|cpm|cpv|cpa|affiliate|paid|ads') THEN 1 ELSE 0 END AS is_paid --Платный или нет
  FROM all_events
),
purchases AS (
  SELECT
    user_pseudo_id,
    event_date AS purchase_date,
    event_time AS purchase_time,
    country,           
    city,              
    device_category,   
    purchase_revenue
  FROM events_flags
  WHERE event_name = 'purchase' AND purchase_revenue IS NOT NULL
),
-------------------------------------------------------------------------
-- Модель First Click Non-Direct
-------------------------------------------------------------------------
m1_prep AS (
  SELECT 
    p.user_pseudo_id, 
    p.purchase_date, 
    p.purchase_revenue,
    p.country, 
    p.city, 
    p.device_category,
    e.event_time,
    e.event_source as source, e.event_medium as medium, e.event_campaign as campaign,
    ROW_NUMBER() OVER(PARTITION BY p.user_pseudo_id, p.purchase_time ORDER BY e.is_direct, e.event_time) as rn
  FROM purchases p
  LEFT JOIN events_flags e ON p.user_pseudo_id = e.user_pseudo_id AND e.event_time < p.purchase_time
),
m1_agg AS (
  SELECT user_pseudo_id, purchase_date, 
  country, city, device_category,
  source, medium, campaign, SUM(purchase_revenue) as m1_revenue
  FROM m1_prep WHERE rn = 1
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
-- модель First Paid / First Non-Direct 
-------------------------------------------------------------------------
m2_prep AS (
  SELECT 
    p.user_pseudo_id, p.purchase_date, p.purchase_revenue,
    p.country, p.city, p.device_category,
    e.event_source as source, e.event_medium as medium, e.event_campaign as campaign,
    ROW_NUMBER() OVER(PARTITION BY p.user_pseudo_id, p.purchase_time ORDER BY e.is_paid DESC, e.is_direct, e.event_time) as rn
  FROM purchases p
  LEFT JOIN events_flags e ON p.user_pseudo_id = e.user_pseudo_id AND e.event_time < p.purchase_time
),
m2_agg AS (
  SELECT user_pseudo_id, purchase_date, 
  country, city, device_category,
  source, medium, campaign, SUM(purchase_revenue) as m2_revenue
  FROM m2_prep WHERE rn = 1
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
-- модель Time Decay 
-------------------------------------------------------------------------
-- Определяем источник для каждой сессии (по модели First Click)
m3_sessions AS (
  SELECT user_pseudo_id, session_id, MIN(event_time) AS session_start,
    COALESCE(ARRAY_AGG(event_source ORDER BY is_direct, event_time LIMIT 1)[OFFSET(0)], '(direct)') AS source,
    COALESCE(ARRAY_AGG(event_medium ORDER BY is_direct, event_time LIMIT 1)[OFFSET(0)], '(none)') AS medium,
    COALESCE(ARRAY_AGG(event_campaign ORDER BY is_direct, event_time LIMIT 1)[OFFSET(0)], '(not set)') AS campaign
  FROM events_flags GROUP BY 1, 2
),
-- Соединяем покупки с сессиями и считаем сырой вес
m3_prep AS (
  SELECT 
    p.user_pseudo_id, p.purchase_date, p.purchase_time, p.purchase_revenue,
    p.country, p.city, p.device_category,
    s.source, s.medium, s.campaign,
    POWER(2, -(DATETIME_DIFF(p.purchase_time, s.session_start, SECOND) / 86400.0) / 7.0) as raw_weight
  FROM purchases p
  LEFT JOIN m3_sessions s ON p.user_pseudo_id = s.user_pseudo_id AND s.session_start <= p.purchase_time
),
-- Нормируем вес
m3_norm AS (
  SELECT *,
    SUM(raw_weight) OVER(PARTITION BY user_pseudo_id, purchase_time) as total_weight
  FROM m3_prep
),
m3_agg AS (
  -- Группируем и умножаем покупку на готовую долю
  SELECT 
    user_pseudo_id, purchase_date,  
    country, city, device_category,
    source, medium, campaign,
    SUM(purchase_revenue * (raw_weight / NULLIF(total_weight, 0))) as m3_revenue
  FROM m3_norm
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
m1_m2_join AS (
  SELECT 
    user_pseudo_id, purchase_date, 
    country, city, device_category,
    source, medium, campaign,
    IFNULL(m1_revenue, 0) AS first_click_revenue,
    IFNULL(m2_revenue, 0) AS first_paid_revenue
  FROM m1_agg
  FULL OUTER JOIN m2_agg USING(user_pseudo_id, purchase_date, country, city, device_category, source, medium, campaign)
)
SELECT 
  user_pseudo_id,
  purchase_date,
  country, city, device_category,
  source,
  medium,
  campaign,
  IFNULL(first_click_revenue, 0) AS first_click_revenue,
  IFNULL(first_paid_revenue, 0) AS first_paid_revenue,
  ROUND(IFNULL(m3_revenue, 0), 2) AS time_decay_revenue
FROM m1_m2_join
FULL OUTER JOIN m3_agg USING(user_pseudo_id, purchase_date, country, city, device_category, source, medium, campaign)


/* Комментарий пополю source: 
В данных в поле source часто попадаются полные URL или домены (например, *.safeframe.googlesyndication.com). В рамках тестового задания я вывожу сырые значения source. Но в реальном рабочем проекте я бы добавила шаг маппинга, чтобы объединить такие домены в общую категорию (например, Google Display). */

/* Комментарии к определению прямых/не прямых источников:
Для определения прямого источника учитывала оба поля medium и source, так как в данных обнаружила строки, где в source null, а в medium referral или organic. Предположила, что возможно не корректные данные в источнике, не подтянулся source. Поэтому не стала их включать в прямой трафик.*/

/*Обоснование определения платных каналов:
Для определения платных каналов взяты метки:
- cpc, ppc, cpa, cpm, cpv — стандартные модели (оплата за клик, действие, показы, просмотры)
- affiliate — партнерские сети
- paid, ads — реклама

Каналы organic (поиск), referral (переход по ссылке с сайта), email (рассылка по базе) бесплатные или собственные каналы.
Также в данных встречаются (data deleted) и <Other>. Предполагаю, что это могут быть какие-то технические заглушки или нераспознанные источники
*/

/* Обоснование выбора функции для модели Time Decay:
В качестве математической функции затухания выбрана экспоненциальная модель (Exponential Decay)  с периодом полураспада 7 дней. Формула сырого веса: 2^(-delta_days / 7).
Причины:
1. Соответствие психологии потребления: модель наиболее точно отражает кривую забывания. Самая высокая ценность у недавних касаний, затем она постепенно нелинейно снижается, но не достигает нуля. Например, вчерашняя реклама влияет на пользователя сильнее всего, реклама недельной давности — в два раза слабее, при этом реклама месячной давности сохраняют минимальный вес.

2. Отказ от альтернатив: 
Линейная модель: снижает ценность равномерно с каждым прошедшим днем. Однако эта модель не учитывает специфику памяти,так как влияние более близких к событию касаний выше тех, которые произошли месяц назад. 
Обратная модель: дает экстремально высокий приоритет касаниям, произошедшим непосредственно перед событием. При этом ценность прошлых касаний слишком резко снижается. Это делает её неподходящей, например, для продукта с длинным циклом принятия решения.

*Примечание: Вычисленные сырые веса нормируются (делятся на сумму весов всей цепочки), чтобы распределенная сумма дохода строго равнялась фактической сумме транзакции.
*/