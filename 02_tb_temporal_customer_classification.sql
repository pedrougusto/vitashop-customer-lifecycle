-- ============================================================
-- PROJECT   : VitaShop – Customer Lifecycle Analytics
-- TABLE     : tb_temporal_customer_classification
-- PURPOSE   : Iterates month-by-month from the start date to
--             today, computing a 24-month rolling window of
--             purchase counts per customer × dimension and
--             applying a 5-state lifecycle classification
--             (new / recurring / inactive / churned / recovered).
--
--             Classifications are computed independently for:
--               • Overall (cross-brand)
--               • Brand
--               • Payment method
--               • Product type
--               • Fulfillment modality
--
-- AUTHOR    : [Your Name]
-- ENGINE    : BigQuery (procedural SQL, WHILE loop backfill)
-- ============================================================

BEGIN

  -- ── Control variables ──────────────────────────────────────
  DECLARE data_start   DATE DEFAULT '2022-01-01';
  DECLARE data_end     DATE DEFAULT CURRENT_DATE();
  DECLARE current_month DATE;

  -- Ensure we start at the 1st of the month
  SET current_month = DATE_TRUNC(data_start, MONTH);

  -- ── Main loop: one INSERT per calendar month ───────────────
  WHILE current_month <= DATE_TRUNC(data_end, MONTH) DO

    INSERT INTO `prj-analytics.vitashop.tb_temporal_customer_classification`

    WITH

    -- ──────────────────────────────────────────────────────────
    -- STEP 1 — Calendar anchors for the analysis month
    -- ──────────────────────────────────────────────────────────
    calendar AS (
      SELECT
        current_month                                           AS month_start,
        LAST_DAY(current_month)                                 AS month_end,
        -- Lower bound of the 24-month lookback window
        DATE_TRUNC(
          DATE_SUB(current_month, INTERVAL 23 MONTH),
          MONTH
        )                                                       AS window_start_24m
    ),

    -- ──────────────────────────────────────────────────────────
    -- STEP 2 — Slice the dimension table to the 24-month window
    -- ──────────────────────────────────────────────────────────
    base_orders AS (
      SELECT *
      FROM `prj-analytics.vitashop.tb_dimensions_customer_orders`
      WHERE
        order_date >= DATE_SUB(current_month, INTERVAL 24 MONTH)
        AND order_date <= LAST_DAY(current_month)
    ),

    -- Pre-aggregate first/last purchase per customer
    -- (used to skip months before the customer's first order)
    customer_dates AS (
      SELECT
        customer_cpf,
        order_line_id,
        order_date,
        brand,
        payment_method,
        product_type,
        fulfillment_modality,
        MIN(order_date) OVER (PARTITION BY customer_cpf) AS first_order_date,
        MAX(order_date) OVER (PARTITION BY customer_cpf) AS last_order_date
      FROM base_orders
    ),

    -- ──────────────────────────────────────────────────────────
    -- STEP 3 — Rolling window metrics per customer × dimension
    --
    --  Periods:
    --    • hist  → before the 24-month window (older history)
    --    • roll  → last 23 complete months (window_start_24m to month_start)
    --    • curr  → the analysis month itself (month_start to month_end)
    -- ──────────────────────────────────────────────────────────
    rolling_metrics AS (
      SELECT
        c.month_start,
        c.month_end,
        c.window_start_24m,
        cd.order_date,
        cd.customer_cpf,
        cd.order_line_id,
        cd.brand,
        cd.payment_method,
        cd.product_type,
        cd.fulfillment_modality,

        -- ── OVERALL (cross-brand) ──────────────────────────
        COUNT(DISTINCT CASE WHEN cd.order_date <  c.window_start_24m                                THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf)
          AS total_hist_overall,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.window_start_24m AND cd.order_date < c.month_start THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf)
          AS total_roll_overall,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.month_start AND cd.order_date <= c.month_end        THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf)
          AS total_curr_overall,

        -- ── BY BRAND ──────────────────────────────────────
        COUNT(DISTINCT CASE WHEN cd.order_date <  c.window_start_24m                                THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.brand)
          AS total_hist_brand,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.window_start_24m AND cd.order_date < c.month_start THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.brand)
          AS total_roll_brand,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.month_start AND cd.order_date <= c.month_end        THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.brand)
          AS total_curr_brand,

        -- ── BY PAYMENT METHOD ─────────────────────────────
        COUNT(DISTINCT CASE WHEN cd.order_date <  c.window_start_24m                                THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.payment_method)
          AS total_hist_payment,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.window_start_24m AND cd.order_date < c.month_start THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.payment_method)
          AS total_roll_payment,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.month_start AND cd.order_date <= c.month_end        THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.payment_method)
          AS total_curr_payment,

        -- ── BY PRODUCT TYPE ───────────────────────────────
        COUNT(DISTINCT CASE WHEN cd.order_date <  c.window_start_24m                                THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.product_type)
          AS total_hist_product,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.window_start_24m AND cd.order_date < c.month_start THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.product_type)
          AS total_roll_product,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.month_start AND cd.order_date <= c.month_end        THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.product_type)
          AS total_curr_product,

        -- ── BY FULFILLMENT MODALITY ───────────────────────
        COUNT(DISTINCT CASE WHEN cd.order_date <  c.window_start_24m                                THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.fulfillment_modality)
          AS total_hist_modality,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.window_start_24m AND cd.order_date < c.month_start THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.fulfillment_modality)
          AS total_roll_modality,

        COUNT(DISTINCT CASE WHEN cd.order_date >= c.month_start AND cd.order_date <= c.month_end        THEN cd.order_line_id END)
          OVER (PARTITION BY c.month_start, cd.customer_cpf, cd.fulfillment_modality)
          AS total_curr_modality

      FROM customer_dates cd
      CROSS JOIN calendar c
      -- Optimisation: skip months before the customer's first purchase
      WHERE c.month_start >= DATE_TRUNC(cd.first_order_date, MONTH)
    ),

    -- ──────────────────────────────────────────────────────────
    -- STEP 4 — Apply 5-state lifecycle classification
    --
    --  State logic (same for each dimension):
    --
    --  NEW       → no history at all; first purchase this month
    --  CHURNED   → had history (hist or roll) but zero activity this month
    --              and zero in the last 23 months
    --  RECOVERED → had history older than 24 months, skipped 23 months,
    --              then purchased again this month
    --  RECURRING → 2+ orders across roll + curr window
    --  INACTIVE  → exactly 1 order in the roll window, nothing this month
    -- ──────────────────────────────────────────────────────────
    classifications AS (
      SELECT
        month_start,
        customer_cpf,
        order_line_id,
        order_date,
        brand,
        payment_method,
        product_type,
        fulfillment_modality,

        -- OVERALL
        CASE
          WHEN total_hist_overall = 0 AND total_roll_overall = 0 AND total_curr_overall > 0 THEN 'new'
          WHEN total_hist_overall > 0 AND total_roll_overall = 0 AND total_curr_overall = 0 THEN 'churned'
          WHEN total_hist_overall > 0 AND total_roll_overall = 0 AND total_curr_overall > 0 THEN 'recovered'
          WHEN (total_roll_overall + total_curr_overall) >= 2                               THEN 'recurring'
          WHEN total_roll_overall = 1 AND total_curr_overall = 0                            THEN 'inactive'
          ELSE 'error'
        END AS lifecycle_overall,

        -- BY BRAND
        CASE
          WHEN brand IS NULL THEN NULL
          WHEN total_hist_brand = 0 AND total_roll_brand = 0 AND total_curr_brand > 0 THEN 'new'
          WHEN total_hist_brand > 0 AND total_roll_brand = 0 AND total_curr_brand = 0 THEN 'churned'
          WHEN total_hist_brand > 0 AND total_roll_brand = 0 AND total_curr_brand > 0 THEN 'recovered'
          WHEN (total_roll_brand + total_curr_brand) >= 2                              THEN 'recurring'
          WHEN total_roll_brand = 1 AND total_curr_brand = 0                           THEN 'inactive'
          ELSE 'error'
        END AS lifecycle_brand,

        -- BY PAYMENT METHOD
        CASE
          WHEN payment_method IS NULL THEN NULL
          WHEN total_hist_payment = 0 AND total_roll_payment = 0 AND total_curr_payment > 0 THEN 'new'
          WHEN total_hist_payment > 0 AND total_roll_payment = 0 AND total_curr_payment = 0 THEN 'churned'
          WHEN total_hist_payment > 0 AND total_roll_payment = 0 AND total_curr_payment > 0 THEN 'recovered'
          WHEN (total_roll_payment + total_curr_payment) >= 2                               THEN 'recurring'
          WHEN total_roll_payment = 1 AND total_curr_payment = 0                            THEN 'inactive'
          ELSE 'error'
        END AS lifecycle_payment,

        -- BY PRODUCT TYPE
        CASE
          WHEN product_type IS NULL THEN NULL
          WHEN total_hist_product = 0 AND total_roll_product = 0 AND total_curr_product > 0 THEN 'new'
          WHEN total_hist_product > 0 AND total_roll_product = 0 AND total_curr_product = 0 THEN 'churned'
          WHEN total_hist_product > 0 AND total_roll_product = 0 AND total_curr_product > 0 THEN 'recovered'
          WHEN (total_roll_product + total_curr_product) >= 2                               THEN 'recurring'
          WHEN total_roll_product = 1 AND total_curr_product = 0                            THEN 'inactive'
          ELSE 'error'
        END AS lifecycle_product,

        -- BY FULFILLMENT MODALITY
        CASE
          WHEN fulfillment_modality IS NULL THEN NULL
          WHEN total_hist_modality = 0 AND total_roll_modality = 0 AND total_curr_modality > 0 THEN 'new'
          WHEN total_hist_modality > 0 AND total_roll_modality = 0 AND total_curr_modality = 0 THEN 'churned'
          WHEN total_hist_modality > 0 AND total_roll_modality = 0 AND total_curr_modality > 0 THEN 'recovered'
          WHEN (total_roll_modality + total_curr_modality) >= 2                               THEN 'recurring'
          WHEN total_roll_modality = 1 AND total_curr_modality = 0                            THEN 'inactive'
          ELSE 'error'
        END AS lifecycle_modality

      FROM rolling_metrics
    )

    -- ──────────────────────────────────────────────────────────
    -- STEP 5 — Final output: rejoin full order dimensions
    -- ──────────────────────────────────────────────────────────
    SELECT
      o.order_month,
      COUNT(DISTINCT o.order_line_id)               AS total_orders,
      cls.customer_cpf,
      o.gender,
      o.customer_birthdate,
      o.brand,
      o.region,
      o.store,
      o.product_category,
      o.payment_method,
      o.payment_detail,
      o.product_type,
      o.fulfillment_modality,
      o.age_bracket,
      ROUND(SUM(o.revenue), 2)                      AS revenue,
      SUM(o.unit_volume)                            AS unit_volume,
      o.channel_group,
      o.campaign_non_direct,
      o.order_origin,
      o.crm_journey,
      o.crm_channel,
      o.crm_interaction_group,
      cls.month_start                               AS analysis_month,
      cls.lifecycle_overall,
      cls.lifecycle_brand,
      cls.lifecycle_payment,
      cls.lifecycle_product,
      cls.lifecycle_modality
    FROM classifications cls
    LEFT JOIN base_orders o
      ON  cls.customer_cpf   = o.customer_cpf
      AND cls.order_line_id  = o.order_line_id
    GROUP BY ALL;

    -- Advance to the next month
    SET current_month = DATE_ADD(current_month, INTERVAL 1 MONTH);

  END WHILE;

END;
