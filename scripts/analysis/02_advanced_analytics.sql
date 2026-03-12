/*
===============================================================================
Analysis Script: Advanced SQL Data Analysis — Gold Layer
===============================================================================
Script Purpose:
    This script performs structured advanced analytics on the 'gold' schema
    of the DataWarehouse database. It is designed to uncover deeper business
    insights beyond basic EDA by applying window functions, CTEs, segmentation
    logic, and reusable reporting views.

    The analysis is organised into the following sections:
        1. Sales Performance Over Time   – Monthly and yearly revenue trends
        2. Running Totals & Moving Avg   – Cumulative growth and price trends
        3. Yearly Product Performance    – Avg comparison and YoY analysis
        4. Part-to-Whole Analysis        – Category revenue contribution %
        5. Data Segmentation             – Product cost bands, customer tiers
        6. Customer Report View          – Full customer KPI consolidation
        7. Product Report View           – Full product KPI consolidation

Database : DataWarehouse
Schema   : gold
Tables   : gold.fact_sales | gold.dim_customers | gold.dim_products
Author   : Adetona Yusuf Olalekan
===============================================================================
*/


-- =============================================================================
-- SECTION 1: SALES PERFORMANCE OVER TIME
-- =============================================================================
-- Purpose:
--     Analyse how revenue, customer count, and quantity sold evolve month by
--     month and year by year. Time-based analysis reveals growth trajectories,
--     seasonal peaks, and periods of underperformance — all critical for
--     strategic planning and forecasting.
-- =============================================================================

-- Monthly sales performance broken down by year and month number
-- Returns one row per month with total revenue, unique customers, and quantity
-- Filtering NULL order dates ensures only valid transactions are included
SELECT
    YEAR(order_date)                    AS order_year,
    MONTH(order_date)                   AS order_month,
    SUM(sales_amount)                   AS total_sales,
    COUNT(DISTINCT customer_key)        AS total_customers,
    SUM(quantity)                       AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- Alternative: truncate order_date to the first day of each month
-- DATETRUNC produces a clean date column that is easier to use in BI tools
-- and avoids needing both YEAR() and MONTH() for grouping and ordering
SELECT
    DATETRUNC(MONTH, order_date)        AS order_month,
    SUM(sales_amount)                   AS total_sales,
    COUNT(DISTINCT customer_key)        AS total_customers,
    SUM(quantity)                       AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date);


-- =============================================================================
-- SECTION 2: RUNNING TOTALS & MOVING AVERAGE
-- =============================================================================
-- Purpose:
--     Track cumulative revenue growth and smoothed price trends over time.
--     Running totals answer "how much have we made so far?" while moving
--     averages filter out short-term noise to reveal the underlying trend.
--
-- =============================================================================

-- Monthly running total of sales with cumulative average price
-- SUM() OVER (ORDER BY ...) accumulates revenue across all prior months
-- AVG() OVER (ORDER BY ...) computes the average price of all months up to now
SELECT
    order_month,
    total_sales,
    SUM(total_sales)  OVER (ORDER BY order_month)   AS running_total_sales,
    ROUND(
        AVG(avg_price) OVER (ORDER BY order_month)
    , 2)                                             AS cumulative_avg_price
FROM (
    SELECT
        DATETRUNC(MONTH, order_date)    AS order_month,
        SUM(sales_amount)               AS total_sales,
        AVG(price)                      AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) monthly_data
ORDER BY order_month;

-- Yearly running total with true 3-month moving average price
-- ROWS BETWEEN 2 PRECEDING AND CURRENT ROW limits the window to exactly
-- 3 periods (current + 2 prior), producing a genuine rolling average
-- rather than an ever-expanding cumulative one
SELECT
    order_year,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_year)      AS running_total_sales,
    ROUND(
        AVG(avg_price) OVER (
            ORDER BY order_year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )
    , 2)                                             AS moving_avg_price_3yr
FROM (
    SELECT
        DATETRUNC(YEAR, order_date)     AS order_year,
        SUM(sales_amount)               AS total_sales,
        AVG(price)                      AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(YEAR, order_date)
) yearly_data
ORDER BY order_year;


-- =============================================================================
-- SECTION 3: YEARLY PRODUCT PERFORMANCE ANALYSIS
-- =============================================================================
-- Purpose:
--     Evaluate each product's annual sales against two benchmarks:
--       (1) Its own historical average across all years
--       (2) Its performance in the prior year (YoY comparison)
--     This identifies which products are improving, declining, or stagnant,
--     and whether any given year was above or below the product's long-term norm.
--
-- Approach:
--     A CTE first aggregates sales by product and year. The outer query then
--     applies window functions (AVG with PARTITION BY, LAG) to compute both
--     benchmarks without requiring self-joins. A nested CTE is used to avoid
--     repeating the window function expressions inside CASE statements.
-- =============================================================================

WITH yearly_product_sales AS (
-- Step 1: Aggregate total sales per product per year
    SELECT
        YEAR(f.order_date)          AS order_year,
        p.product_name,
        SUM(f.sales_amount)         AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
),

product_benchmarks AS (
-- Step 2: Compute benchmark values as separate columns before using them in CASE
-- This avoids repeating the full window function expression inside every CASE block
    SELECT
        order_year,
        product_name,
        current_sales,
        AVG(current_sales) OVER (PARTITION BY product_name)                             AS avg_sales,
        LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year)         AS py_sales
    FROM yearly_product_sales
)

-- Step 3: Apply difference calculations and classification labels
SELECT
    order_year,
    product_name,
    current_sales,
    avg_sales,
    current_sales - avg_sales                           AS diff_vs_avg,
    CASE
        WHEN current_sales > avg_sales  THEN 'Above Avg'
        WHEN current_sales < avg_sales  THEN 'Below Avg'
        ELSE                                 'On Avg'
    END                                                 AS avg_benchmark,
    py_sales,
    current_sales - py_sales                            AS diff_vs_py,
    CASE
        WHEN current_sales > py_sales   THEN 'Increase'
        WHEN current_sales < py_sales   THEN 'Decrease'
        ELSE                                 'No Change'
    END                                                 AS yoy_change
FROM product_benchmarks
ORDER BY product_name, order_year;


-- =============================================================================
-- SECTION 4: PART-TO-WHOLE ANALYSIS
-- =============================================================================
-- Purpose:
--     Quantify each category's contribution to overall revenue so stakeholders
--     can identify concentration risk and strategic priority areas.
--     SUM() OVER () computes the grand total inline without a separate subquery,
--     keeping the logic clean and efficient.
-- =============================================================================

-- Revenue contribution (%) per product category
-- Note: p.category prefix is required to avoid ambiguous column reference
-- CAST to FLOAT is unnecessary here — multiplying by 100.0 already forces
-- decimal division, so ROUND() alone is sufficient
WITH category_sales AS (
    SELECT
        p.category,
        SUM(s.sales_amount)         AS total_sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON p.product_key = s.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER ()                                            AS overall_sales,
    CONCAT(
        ROUND(total_sales * 100.0 / SUM(total_sales) OVER (), 2), '%'
    )                                                                   AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;


-- =============================================================================
-- SECTION 5: DATA SEGMENTATION
-- =============================================================================
-- Purpose:
--     Group products and customers into meaningful tiers based on cost and
--     spending behaviour. Segmentation converts raw metrics into actionable
--     categories that support pricing strategy, customer retention programmes,
--     and targeted marketing campaigns.
-- =============================================================================

-- Segment products by unit cost into four price bands
-- Helps pricing and procurement teams understand the product portfolio spread
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE
            WHEN cost < 100                     THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500       THEN '100 - 500'
            WHEN cost BETWEEN 500 AND 1000      THEN '500 - 1000'
            ELSE                                     'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT
    cost_range,
    COUNT(product_key)                  AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

-- Segment customers into VIP, Regular, and New tiers based on lifespan and spend
-- VIP      : 12+ months of purchase history AND total spend > £5,000
-- Regular  : 12+ months of purchase history AND total spend <= £5,000
-- New      : Less than 12 months of purchase history (regardless of spend)
-- This segmentation drives retention and upsell strategy decisions
WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(s.sales_amount)                                 AS total_spending,
        MIN(s.order_date)                                   AS first_order,
        MAX(s.order_date)                                   AS last_order,
        DATEDIFF(MONTH, MIN(s.order_date), MAX(s.order_date)) AS lifespan
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c                          -- Fixed: was gold_dim_customers
        ON s.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT
    customer_segment,
    COUNT(customer_key)                 AS total_customers
FROM (
    SELECT
        customer_key,
        CASE
            WHEN lifespan >= 12 AND total_spending > 5000  THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE                                                'New'
        END AS customer_segment
    FROM customer_spending
) segmented
GROUP BY customer_segment
ORDER BY total_customers DESC;


-- =============================================================================
-- SECTION 6: CUSTOMER REPORT VIEW
-- =============================================================================
-- Purpose:
--     This view consolidates all key customer metrics and behaviours into a
--     single reusable object, eliminating the need to re-join and re-aggregate
--     raw tables in every downstream report or dashboard query.
--
-- Highlights:
--     1. Gathers essential fields: names, age, and transaction details
--     2. Segments customers by value tier (VIP, Regular, New) and age group
--     3. Aggregates customer-level metrics:
--           - total orders, total sales, total quantity, total products, lifespan
--     4. Calculates KPIs:
--           - recency (months since last order)
--           - average order value (AOV)
--           - average monthly spend
-- =============================================================================

CREATE VIEW gold.report_customers AS

WITH base_query AS (
/*---------------------------------------------------------------------------
  1) Base Query: Joins fact_sales to dim_customers and retrieves core columns
---------------------------------------------------------------------------*/
    SELECT
        s.order_number,
        s.product_key,
        s.order_date,
        s.sales_amount,
        s.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name)          AS customer_name,
        DATEDIFF(YEAR, c.birthdate, GETDATE())           AS age
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c                       -- Fixed: was gold_dim_customers
        ON c.customer_key = s.customer_key
    WHERE s.order_date IS NOT NULL
),

customer_aggregations AS (
/*---------------------------------------------------------------------------
  2) Customer Aggregations: Summarises key metrics at the customer level
---------------------------------------------------------------------------*/
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number)                     AS total_orders,
        SUM(sales_amount)                                AS total_sales,
        SUM(quantity)                                    AS total_quantity,
        COUNT(DISTINCT product_key)                      AS total_products,
        MAX(order_date)                                  AS last_order_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY
        customer_key,
        customer_number,
        customer_name,
        age
)

/*---------------------------------------------------------------------------
  3) Final Query: Applies segmentation logic and computes KPIs
---------------------------------------------------------------------------*/
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE
        WHEN age < 20                       THEN 'Below 20'
        WHEN age BETWEEN 20 AND 29          THEN '20 - 29'
        WHEN age BETWEEN 30 AND 39          THEN '30 - 39'
        WHEN age BETWEEN 40 AND 49          THEN '40 - 49'   -- Fixed: was '40.49'
        ELSE                                     'Above 50'
    END                                         AS age_group,
    CASE
        WHEN lifespan >= 12 AND total_sales > 5000  THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE                                             'New'
    END                                         AS customer_segment,
    last_order_date,
    DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    -- Average Order Value (AOV): revenue per order placed
    -- Guard against division by zero with a CASE check
    CASE
        WHEN total_orders = 0   THEN 0
        ELSE total_sales / total_orders
    END                                         AS avg_order_value,
    -- Average Monthly Spend: revenue normalised across the customer lifespan
    -- If lifespan = 0 (only one month of activity), return total_sales as-is
    CASE
        WHEN lifespan = 0       THEN total_sales
        ELSE total_sales / lifespan
    END                                         AS avg_monthly_spend
FROM customer_aggregations
ORDER BY total_sales DESC;


-- =============================================================================
-- SECTION 7: PRODUCT REPORT VIEW
-- =============================================================================
-- Purpose:
--     This view consolidates all key product metrics and behaviours into a
--     single reusable object for use in downstream reporting and dashboards.
--
-- Highlights:
--     1. Gathers essential fields: product name, category, subcategory, cost
--     2. Segments products by total revenue (High-Performer, Mid-Range, Low-Performer)
--     3. Aggregates product-level metrics:
--           - total orders, total sales, total quantity, total customers, lifespan
--     4. Calculates KPIs:
--           - recency (months since last sale)
--           - average selling price
--           - average order revenue (AOR)
--           - average monthly revenue
-- =============================================================================

CREATE VIEW gold.report_products AS

WITH base_query AS (
/*---------------------------------------------------------------------------
  1) Base Query: Joins fact_sales to dim_products and retrieves core columns
---------------------------------------------------------------------------*/
    SELECT
        s.order_number,
        s.order_date,
        s.customer_key,
        s.sales_amount,
        s.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON s.product_key = p.product_key
    WHERE s.order_date IS NOT NULL
),

product_aggregations AS (
/*---------------------------------------------------------------------------
  2) Product Aggregations: Summarises key metrics at the product level
---------------------------------------------------------------------------*/
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        MIN(order_date)                                                     AS first_sale_date,
        MAX(order_date)                                                     AS last_sale_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date))                   AS lifespan,
        COUNT(DISTINCT order_number)                                        AS total_orders,
        COUNT(DISTINCT customer_key)                                        AS total_customers,
        SUM(sales_amount)                                                   AS total_sales,
        SUM(quantity)                                                       AS total_quantity,
        ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1)   AS avg_selling_price
    FROM base_query
    GROUP BY
        product_key,
        product_name,
        category,
        subcategory,
        cost
)

/*---------------------------------------------------------------------------
  3) Final Query: Applies segmentation logic and computes product KPIs
---------------------------------------------------------------------------*/
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    DATEDIFF(MONTH, last_sale_date, GETDATE())  AS recency_in_months,
    CASE
        WHEN total_sales > 50000    THEN 'High-Performer'
        WHEN total_sales >= 10000   THEN 'Mid-Range'
        ELSE                             'Low-Performer'
    END                                         AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR): revenue generated per order
    CASE
        WHEN total_orders = 0   THEN 0
        ELSE total_sales / total_orders
    END                                         AS avg_order_revenue,
    -- Average Monthly Revenue: revenue normalised across the product lifespan
    CASE
        WHEN lifespan = 0       THEN total_sales
        ELSE total_sales / lifespan
    END                                         AS avg_monthly_revenue
FROM product_aggregations
ORDER BY total_sales DESC;


-- =============================================================================
-- SECTION 8: VALIDATE VIEWS
-- =============================================================================
-- Purpose:
--     Run these SELECT statements separately after the views have been created
--     to confirm they return expected results. These must NOT be run inside
--     the same batch as the CREATE VIEW statements above.
-- =============================================================================

-- Preview the customer report view
SELECT * FROM gold.report_customers;

-- Preview the product report view
SELECT * FROM gold.report_products;
