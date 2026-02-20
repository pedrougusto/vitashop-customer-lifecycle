-- ============================================================
-- PROJECT   : VitaShop – Customer Lifecycle Analytics
-- TABLE     : tb_dimensions_customer_orders
-- PURPOSE   : Enriched order-level table joining web attribution
--             (GA4), order metadata, and CRM interactions.
--             Feeds the temporal classification pipeline.
-- AUTHOR    : [Your Name]
-- ENGINE    : BigQuery (partitioned by order_date)
-- ============================================================

CREATE OR REPLACE TABLE `prj-analytics.vitashop.tb_dimensions_customer_orders`
PARTITION BY order_date AS (

WITH

-- -------------------------------------------------------
-- 1. GA4 WEB ATTRIBUTION
--    Last-touch per user per day (session-level dedup)
-- -------------------------------------------------------
ga4_attribution AS (
  SELECT
    event_date                  AS attribution_date,
    user_id,
    channel_group,
    campaign_non_direct,
    'APP'                       AS booking_origin_type
  FROM `prj-analytics.ds_ga4.tb_ga4_customer_attribution`
  WHERE
    user_id IS NOT NULL
    AND event_date >= '2022-01-01'
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY user_id, event_date
      ORDER BY ga_session_id DESC
    ) = 1
),

-- -------------------------------------------------------
-- 2. ORDERS
--    Enriched with GA4 channel attribution and
--    booking origin classification (digital vs. manual)
-- -------------------------------------------------------
orders_enriched AS (
  SELECT
    DATE(o.created_at)                                        AS order_date,
    o.customer_id,
    o.order_id,
    ga.campaign_non_direct,
    line.product_order_id,

    -- Channel origin classification
    CASE
      WHEN UPPER(o.booking_channel) = 'DIGITAL' THEN 'APP'
      ELSE 'CALL_CENTER'
    END                                                       AS booking_origin_type,

    -- Marketing channel (only for digital orders)
    CASE
      WHEN UPPER(o.booking_channel) = 'DIGITAL' AND ga.channel_group IS NOT NULL
        THEN ga.channel_group
      WHEN UPPER(o.booking_channel) = 'DIGITAL' AND ga.channel_group IS NULL
        THEN 'Unidentified'
      ELSE NULL
    END                                                       AS channel_group,

    -- Broad origin category
    CASE
      WHEN UPPER(o.booking_channel) = 'DIGITAL'    THEN 'DIGITAL'
      WHEN UPPER(o.booking_channel) = 'CALL_CENTER' THEN 'CALL_CENTER'
      ELSE 'IN_STORE'
    END                                                       AS order_origin

  FROM `prj-analytics.ds_orders.fct_orders_vitashop` o
  -- Unnest multi-product orders into individual lines
  , UNNEST(SPLIT(
      COALESCE(o.fulfilled_product_ids, o.ordered_product_ids),
      ';'
    )) AS product_order_id
  LEFT JOIN ga4_attribution ga
    ON  ga.user_id        = o.customer_id
    AND ga.attribution_date = DATE(o.created_at)
  WHERE
    DATE(o.created_at) >= '2022-01-01'
    AND o.order_status  <> 'CANCELLED'
    AND o.booking_channel NOT IN ('PARTNER_API', 'WHOLESALE')
  GROUP BY ALL
),

-- -------------------------------------------------------
-- 3. ORDER LINE ITEMS (product / service level)
--    Core fact table with revenue and volume metrics
-- -------------------------------------------------------
order_lines AS (
  SELECT
    -- Time dimensions
    DATE_TRUNC(DATE(ol.order_date), MONTH)           AS order_month,
    DATE_TRUNC(DATE(ol.order_date), WEEK(MONDAY))    AS order_week,
    ol.order_date,

    -- Keys
    ol.order_line_id,
    ol.customer_id,
    ol.customer_cpf,

    -- Customer demographics
    ol.customer_gender                               AS gender,
    ol.customer_birthdate,

    -- Geography / store
    ol.brand_name                                    AS brand,
    ol.region_name                                   AS region,
    ol.store_name                                    AS store,

    -- Product dimensions
    ol.product_category,
    ol.payment_method,

    -- Payment method normalization
    CASE
      WHEN LOWER(ol.payment_method) = 'credit_card' THEN 'CREDIT_CARD'
      WHEN LOWER(ol.payment_method) = 'vitashop_wallet' THEN 'VITASHOP_WALLET'
      WHEN LOWER(ol.payment_method) = 'health_plan' THEN ol.health_plan_name
      ELSE 'error'
    END                                              AS payment_detail,

    -- Product type classification
    CASE
      WHEN LOWER(ol.product_category) LIKE '%supplement%' THEN 'SUPPLEMENT'
      WHEN LOWER(ol.product_category) LIKE '%device%'     THEN 'DEVICE'
      WHEN LOWER(ol.product_category) LIKE '%service%'    THEN 'SERVICE'
      ELSE 'OTHER'
    END                                              AS product_type,

    -- Fulfillment modality
    CASE
      WHEN LOWER(ol.fulfillment_type) LIKE '%delivery%'
        OR LOWER(ol.cluster_name) LIKE '%home%'
        THEN 'HOME_DELIVERY'
      ELSE 'IN_STORE_PICKUP'
    END                                              AS fulfillment_modality,

    -- Age bracket at time of purchase
    CASE
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 0  AND 4  THEN '00–04'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 5  AND 9  THEN '05–09'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 10 AND 16 THEN '10–16'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 17 AND 21 THEN '17–21'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 22 AND 28 THEN '22–28'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 29 AND 35 THEN '29–35'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 36 AND 45 THEN '36–45'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 46 AND 59 THEN '46–59'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) BETWEEN 60 AND 89 THEN '60–89'
      WHEN DATE_DIFF(ol.order_date, ol.customer_birthdate, YEAR) >= 90             THEN '90+'
      ELSE 'error'
    END                                              AS age_bracket,

    -- Metrics
    ROUND(SUM(ol.revenue_amount), 2)                 AS revenue,
    SUM(ol.quantity)                                 AS unit_volume

  FROM `prj-analytics.ds_orders.fct_order_lines_vitashop` ol
  WHERE
    ol.order_date >= '2022-01-01'
    AND ol.market_segment IN ('B2C', 'PREMIUM')
    AND ol.is_excluded     = FALSE
    AND ol.customer_cpf    IS NOT NULL
    AND ol.customer_cpf    NOT IN ('-2', '-1', '0')
  GROUP BY ALL
)

-- -------------------------------------------------------
-- 4. FINAL JOIN — orders + attribution + CRM
-- -------------------------------------------------------
SELECT
  ol.*,
  ord.channel_group,
  ord.campaign_non_direct,
  COALESCE(ord.order_origin, 'IN_STORE')        AS order_origin,
  crm.journey                                   AS crm_journey,
  crm.channel                                   AS crm_channel,
  crm.interaction_group                         AS crm_interaction_group
FROM order_lines ol
LEFT JOIN (
  -- Dedup: keep most recent order record per product_order_id
  SELECT * FROM orders_enriched
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_order_id
    ORDER BY order_date DESC
  ) = 1
) ord
  ON  ord.product_order_id = ol.order_line_id
LEFT JOIN `prj-analytics.vitashop.tb_crm_customer_interactions` crm
  ON  crm.customer_cpf = ol.customer_cpf
  AND crm.order_line_id = ol.order_line_id
)
