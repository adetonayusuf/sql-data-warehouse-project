/*
===============================================================================
EDA Script: Exploratory Data Analysis (EDA) — Gold Layer
===============================================================================
Script Purpose:
    This script performs a structured Exploratory Data Analysis (EDA) on the
    'gold' schema of the DataWarehouse database. It is designed to help analysts
    and business stakeholders understand the shape, quality, and key patterns
    within the data before proceeding to advanced analytics or reporting.

    The analysis is organised into the following sections:
        1. Database Exploration      – Inspect tables and columns in the schema
        2. Dimensions Exploration    – Understand key dimension attributes
        3. Date Exploration          – Identify the temporal range of the data
        4. Key Business Metrics      – High-level KPI summary (Big Numbers)
        5. Magnitude Analysis        – Breakdown of metrics by dimension groups
        6. Ranking Analysis          – Top and bottom performers
        7. Time-Series & Trends      – Revenue and order trends over time
        8. Customer Segmentation     – Group customers by value and behaviour
        9. Part-to-Whole Analysis    – Contribution and revenue share breakdowns

Database : DataWarehouse
Schema   : gold
Tables   : gold.dim_customers | gold.dim_products | gold.fact_sales
Author   : Adetona Yusuf Olalekan
===============================================================================
*/


-- =============================================================================
-- SECTION 1: DATABASE EXPLORATION
-- =============================================================================
-- Purpose:
--     Inspect the structure of the DataWarehouse database. This is the first
--     step of any EDA — understanding what tables and columns exist before
--     writing any analytical queries.
-- =============================================================================

-- Retrieve all tables available in the database
SELECT *
FROM INFORMATION_SCHEMA.TABLES;

-- Retrieve all columns across all tables in the database
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS;

-- Retrieve all columns for a specific table
-- Useful for confirming data types, nullability, and column names before querying
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';


-- =============================================================================
-- SECTION 2: DIMENSIONS EXPLORATION
-- =============================================================================
-- Purpose:
--     Explore the dimension tables to understand the categorical attributes
--     available in the data. This reveals the 'shape' of the business —
--     which markets it operates in, how products are organised, and what
--     descriptive fields are available for slicing and filtering.
-- =============================================================================

-- Identify all unique countries that customers come from
-- Helps understand the geographic spread of the customer base
SELECT DISTINCT country
FROM gold.dim_customers
ORDER BY country;

-- Explore the full product hierarchy: category → subcategory → product name
-- Gives a complete picture of the product catalogue structure
SELECT DISTINCT
    category,
    subcategory,
    product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;


-- =============================================================================
-- SECTION 3: DATE EXPLORATION
-- =============================================================================
-- Purpose:
--     Establish the temporal boundaries of the dataset. Understanding the
--     date range and customer age spread is critical before performing any
--     trend, cohort, or time-based analysis.
-- =============================================================================

-- Find the date of the first and last order placed
-- Also calculate the total trading period in years to understand data coverage
SELECT
    MIN(order_date)                                   AS first_order_date,
    MAX(order_date)                                   AS last_order_date,
    DATEDIFF(YEAR, MIN(order_date), MAX(order_date))  AS order_range_years
FROM gold.fact_sales;

-- Find the age range of customers based on their birthdates
-- This gives the youngest and oldest customer ages to support demographic analysis
SELECT
    MIN(birthdate)                                    AS oldest_birthdate,
    DATEDIFF(YEAR, MIN(birthdate), GETDATE())         AS oldest_customer_age,
    MAX(birthdate)                                    AS youngest_birthdate,
    DATEDIFF(YEAR, MAX(birthdate), GETDATE())         AS youngest_customer_age
FROM gold.dim_customers;


-- =============================================================================
-- SECTION 4: KEY BUSINESS METRICS (BIG NUMBERS)
-- =============================================================================
-- Purpose:
--     Generate a single consolidated view of the most important high-level
--     KPIs for the business. These 'big numbers' give an immediate sense of
--     scale and serve as the baseline for all subsequent analysis.
-- =============================================================================

-- Consolidated KPI summary — all key metrics in one result set
-- Allows for a quick executive-level snapshot of the entire business
SELECT 'Total Revenue'          AS metric_name, SUM(sales_amount)              AS metric_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity Sold',                    SUM(quantity)                                FROM gold.fact_sales
UNION ALL
SELECT 'Average Selling Price',                  AVG(price)                                   FROM gold.fact_sales
UNION ALL
SELECT 'Total No. Orders',                       COUNT(DISTINCT order_number)                 FROM gold.fact_sales
UNION ALL
SELECT 'Total No. Products',                     COUNT(DISTINCT product_key)                  FROM gold.dim_products
UNION ALL
SELECT 'Total No. Customers',                    COUNT(DISTINCT customer_key)                 FROM gold.dim_customers
UNION ALL
SELECT 'Customers Who Ordered',                  COUNT(DISTINCT customer_key)                 FROM gold.fact_sales;

-- Average revenue per order (Average Order Value)
-- A key commercial metric that reflects the typical transaction size
SELECT
    ROUND(SUM(sales_amount) * 1.0 / COUNT(DISTINCT order_number), 2) AS avg_order_value
FROM gold.fact_sales;

-- Average revenue generated per customer (Customer Revenue Value)
-- Measures how much, on average, each customer contributes to revenue
SELECT
    ROUND(SUM(sales_amount) * 1.0 / COUNT(DISTINCT customer_key), 2) AS avg_revenue_per_customer
FROM gold.fact_sales;


-- =============================================================================
-- SECTION 5: MAGNITUDE ANALYSIS
-- =============================================================================
-- Purpose:
--     Break down key metrics by dimension attributes to understand where the
--     business volume is concentrated. This reveals which segments, geographies,
--     categories, and customer groups are driving the most activity.
-- =============================================================================

-- Total number of customers by country
-- Shows the geographic distribution of the customer base
SELECT
    country,
    COUNT(customer_key)   AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Total number of customers by gender
-- Reveals gender composition of the customer base
SELECT
    gender,
    COUNT(customer_key)   AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- Total number of customers by marital status
-- Useful for demographic profiling and targeted marketing strategies
SELECT
    marital_status,
    COUNT(customer_key)   AS total_customers
FROM gold.dim_customers
GROUP BY marital_status
ORDER BY total_customers DESC;

-- Total number of products by category
-- Shows how the product catalogue is distributed across categories
SELECT
    category,
    COUNT(product_key)    AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- Average cost per product category
-- Helps understand the price positioning of each product category
SELECT
    category,
    ROUND(AVG(cost), 2)   AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;

-- Total revenue generated per product category
-- Identifies which categories are the strongest revenue drivers
SELECT
    p.category,
    SUM(s.sales_amount)   AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Total revenue generated per customer
-- Surfaces the highest and lowest value customers across the entire base
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount)   AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC;

-- Total items sold by country
-- Shows which geographies have the highest purchase volume
SELECT
    c.country,
    SUM(s.quantity)       AS total_items_sold
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.country
ORDER BY total_items_sold DESC;


-- =============================================================================
-- SECTION 6: RANKING ANALYSIS
-- =============================================================================
-- Purpose:
--     Identify top and bottom performers across products and customers.
--     DENSE_RANK() is used instead of ROW_NUMBER() to handle ties fairly —
--     two products with equal revenue will share the same rank rather than
--     being arbitrarily split.
-- =============================================================================

-- Top 5 best-performing products by total revenue
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(s.sales_amount)                                       AS total_revenue,
        DENSE_RANK() OVER (ORDER BY SUM(s.sales_amount) DESC)     AS revenue_rank
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
    GROUP BY p.product_name
) ranked
WHERE revenue_rank <= 5;

-- Bottom 5 worst-performing products by total revenue
-- Useful for identifying products to review, discount, or discontinue
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(s.sales_amount)                                       AS total_revenue,
        DENSE_RANK() OVER (ORDER BY SUM(s.sales_amount) ASC)      AS revenue_rank
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
    GROUP BY p.product_name
) ranked
WHERE revenue_rank <= 5;

-- Top 10 customers by total revenue generated
-- Identifies the most commercially valuable customers in the business
SELECT *
FROM (
    SELECT
        c.customer_key,
        c.first_name + ' ' + c.last_name                         AS customer_name,
        SUM(s.sales_amount)                                       AS total_revenue,
        DENSE_RANK() OVER (ORDER BY SUM(s.sales_amount) DESC)     AS revenue_rank
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
    GROUP BY c.customer_key, c.first_name, c.last_name
) ranked
WHERE revenue_rank <= 10;

-- Bottom 3 customers by total number of orders placed
-- Highlights the least engaged customers, useful for re-engagement campaigns
SELECT TOP 3
    c.customer_key,
    c.first_name + ' ' + c.last_name      AS customer_name,
    COUNT(DISTINCT s.order_number)         AS total_orders
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_orders ASC;

-- Top 3 subcategories by revenue ranked within each parent category
-- Uses PARTITION BY to produce separate rankings per category group
SELECT *
FROM (
    SELECT
        p.category,
        p.subcategory,
        SUM(s.sales_amount)                                                      AS total_revenue,
        RANK() OVER (PARTITION BY p.category ORDER BY SUM(s.sales_amount) DESC)  AS rank_within_category
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
    GROUP BY p.category, p.subcategory
) ranked
WHERE rank_within_category <= 3
ORDER BY category, rank_within_category;


-- =============================================================================
-- SECTION 7: TIME-SERIES & TREND ANALYSIS
-- =============================================================================
-- Purpose:
--     Analyse how key metrics evolve over time. Time-series analysis is
--     essential for identifying growth trajectories, seasonality patterns,
--     and periods of exceptional or poor performance.
-- =============================================================================

-- Monthly revenue trend — volume, orders, and items sold per month
-- The foundational time-series view; shows how the business performs month by month
SELECT
    FORMAT(order_date, 'yyyy-MM')   AS order_month,
    SUM(sales_amount)               AS monthly_revenue,
    COUNT(DISTINCT order_number)    AS total_orders,
    SUM(quantity)                   AS total_items_sold
FROM gold.fact_sales
GROUP BY FORMAT(order_date, 'yyyy-MM')
ORDER BY order_month;

-- Annual revenue with year-over-year (YoY) growth percentage
-- Uses LAG() to compare each year against the previous year
-- NULLIF() prevents division-by-zero errors in the growth calculation
SELECT
    YEAR(order_date)                                                     AS order_year,
    SUM(sales_amount)                                                    AS annual_revenue,
    LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date))             AS prev_year_revenue,
    ROUND(
        (SUM(sales_amount) - LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)))
        * 100.0
        / NULLIF(LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)), 0)
    , 2)                                                                 AS yoy_growth_pct
FROM gold.fact_sales
GROUP BY YEAR(order_date)
ORDER BY order_year;

-- Revenue by month-of-year to detect seasonality patterns
-- Aggregates across all years so January always represents all Januaries, etc.
SELECT
    MONTH(order_date)               AS month_number,
    DATENAME(MONTH, order_date)     AS month_name,
    SUM(sales_amount)               AS total_revenue,
    COUNT(DISTINCT order_number)    AS total_orders
FROM gold.fact_sales
GROUP BY MONTH(order_date), DATENAME(MONTH, order_date)
ORDER BY month_number;

-- Cumulative (running total) revenue over time
-- Uses a window SUM to show how total revenue accumulates month by month
SELECT
    FORMAT(order_date, 'yyyy-MM')                    AS order_month,
    SUM(sales_amount)                                AS monthly_revenue,
    SUM(SUM(sales_amount)) OVER (
        ORDER BY FORMAT(order_date, 'yyyy-MM')
    )                                                AS cumulative_revenue
FROM gold.fact_sales
GROUP BY FORMAT(order_date, 'yyyy-MM')
ORDER BY order_month;

-- 3-month moving average of monthly revenue
-- Smooths out short-term noise to reveal the underlying revenue trend
-- ROWS BETWEEN 2 PRECEDING AND CURRENT ROW includes the current and 2 prior months
SELECT
    FORMAT(order_date, 'yyyy-MM')                    AS order_month,
    SUM(sales_amount)                                AS monthly_revenue,
    ROUND(AVG(SUM(sales_amount)) OVER (
        ORDER BY FORMAT(order_date, 'yyyy-MM')
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                            AS moving_avg_3m
FROM gold.fact_sales
GROUP BY FORMAT(order_date, 'yyyy-MM')
ORDER BY order_month;


-- =============================================================================
-- SECTION 8: CUSTOMER SEGMENTATION
-- =============================================================================
-- Purpose:
--     Group customers into meaningful segments based on their spending behaviour,
--     purchase frequency, and demographic profile. Segmentation transforms raw
--     customer data into actionable business intelligence, enabling targeted
--     retention, upsell, and re-engagement strategies.
-- =============================================================================

-- Segment customers by total lifetime revenue into value tiers
-- High Value: >= £5,000 | Mid Value: >= £1,000 | Low Value: below £1,000
-- Thresholds should be reviewed and adjusted based on actual revenue distribution
SELECT
    customer_segment,
    COUNT(customer_key)    AS total_customers,
    SUM(total_revenue)     AS segment_revenue
FROM (
    SELECT
        c.customer_key,
        SUM(s.sales_amount) AS total_revenue,
        CASE
            WHEN SUM(s.sales_amount) >= 5000 THEN 'High Value'
            WHEN SUM(s.sales_amount) >= 1000 THEN 'Mid Value'
            ELSE                                  'Low Value'
        END                 AS customer_segment
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
    GROUP BY c.customer_key
) segmented
GROUP BY customer_segment
ORDER BY segment_revenue DESC;

-- Segment customers by order frequency
-- Champion: 10+ orders | Regular: 3–9 orders | One-Time: 1–2 orders
-- Identifies loyal customers vs. one-time buyers for targeted campaigns
SELECT
    frequency_segment,
    COUNT(customer_key)    AS total_customers
FROM (
    SELECT
        customer_key,
        COUNT(DISTINCT order_number) AS order_count,
        CASE
            WHEN COUNT(DISTINCT order_number) >= 10 THEN 'Champion'
            WHEN COUNT(DISTINCT order_number) >= 3  THEN 'Regular'
            ELSE                                         'One-Time'
        END                          AS frequency_segment
    FROM gold.fact_sales
    GROUP BY customer_key
) segmented
GROUP BY frequency_segment
ORDER BY total_customers DESC;

-- Segment customers by age group
-- Reveals the demographic composition of the customer base
SELECT
    age_group,
    COUNT(customer_key)    AS total_customers
FROM (
    SELECT
        customer_key,
        CASE
            WHEN DATEDIFF(YEAR, birthdate, GETDATE()) < 30 THEN 'Under 30'
            WHEN DATEDIFF(YEAR, birthdate, GETDATE()) < 45 THEN '30 - 44'
            WHEN DATEDIFF(YEAR, birthdate, GETDATE()) < 60 THEN '45 - 59'
            ELSE                                                  '60+'
        END AS age_group
    FROM gold.dim_customers
) aged
GROUP BY age_group
ORDER BY total_customers DESC;


-- =============================================================================
-- SECTION 9: PART-TO-WHOLE ANALYSIS
-- =============================================================================
-- Purpose:
--     Understand how individual segments contribute to the overall total.
--     Part-to-whole analysis answers questions like "what percentage of total
--     revenue does each category drive?" — revealing concentration risk and
--     strategic importance of each business segment.
-- =============================================================================

-- Revenue share (%) per product category as a proportion of total revenue
-- Uses SUM() OVER() as a window function to calculate the grand total inline
-- without a separate subquery, keeping the logic clean and efficient
SELECT
    p.category,
    SUM(s.sales_amount)                                                    AS category_revenue,
    ROUND(
        SUM(s.sales_amount) * 100.0 / SUM(SUM(s.sales_amount)) OVER ()
    , 2)                                                                   AS revenue_share_pct
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
GROUP BY p.category
ORDER BY category_revenue DESC;

-- Average order value per product category
-- Shows the typical transaction size within each category
-- Useful for comparing category pricing power and customer spend depth
SELECT
    p.category,
    ROUND(SUM(s.sales_amount) * 1.0 / COUNT(DISTINCT s.order_number), 2)  AS avg_order_value
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p ON p.product_key = s.product_key
GROUP BY p.category
ORDER BY avg_order_value DESC;

-- Revenue share (%) per country as a proportion of total revenue
-- Identifies which geographies are commercially most significant
SELECT
    c.country,
    SUM(s.sales_amount)                                                    AS country_revenue,
    ROUND(
        SUM(s.sales_amount) * 100.0 / SUM(SUM(s.sales_amount)) OVER ()
    , 2)                                                                   AS revenue_share_pct
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c ON c.customer_key = s.customer_key
GROUP BY c.country
ORDER BY country_revenue DESC;
