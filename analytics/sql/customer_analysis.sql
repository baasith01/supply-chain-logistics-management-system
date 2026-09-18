-- Customer Analysis
-- Purpose:
--   Analyze customer value, ordering behavior, delivery experience,
--   regional distribution, repeat activity, and customer risk signals.
--
-- Expected mart:
--   delivery_mart
--
-- Assumptions:
--   - One row in delivery_mart represents one order.
--   - customer_id identifies a customer.
--   - order_date is the order date.
--   - order_value is the order value.
--   - delivery_delay_min is the delivery delay in minutes.
--   - is_delayed is 1/0.
--   - delivery_status / order_status contain operational statuses.
--   - region identifies the customer's/order's operating region.
--
-- Adapt schema qualification to the target warehouse if required.

-- ============================================================
-- 1. Overall Customer KPIs
-- ============================================================

SELECT
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(*) AS total_orders,
    ROUND(
        COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT customer_id), 0),
        2
    ) AS orders_per_customer,
    ROUND(
        SUM(order_value),
        2
    ) AS total_order_value,
    ROUND(
        AVG(order_value),
        2
    ) AS average_order_value,
    ROUND(
        AVG(delivery_delay_min),
        2
    ) AS average_delivery_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS customer_order_delay_rate_pct
FROM delivery_mart;


-- ============================================================
-- 2. Customer-Level Value and Activity
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS average_order_value,
    ROUND(AVG(delivery_distance_km), 2) AS average_delivery_distance_km,
    ROUND(AVG(delivery_delay_min), 2) AS average_delivery_delay_min,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY customer_id
ORDER BY total_order_value DESC;


-- ============================================================
-- 3. Top Customers by Revenue Contribution
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(
        SUM(order_value) * 100.0 /
        NULLIF((SELECT SUM(order_value) FROM delivery_mart), 0),
        2
    ) AS revenue_contribution_pct
FROM delivery_mart
GROUP BY customer_id
ORDER BY total_order_value DESC
LIMIT 20;


-- ============================================================
-- 4. Customer Segmentation by Order Value
-- ============================================================

WITH customer_value AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_order_value
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    CASE
        WHEN total_order_value >= 100000 THEN 'High Value'
        WHEN total_order_value >= 50000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_value_segment
FROM customer_value
ORDER BY total_order_value DESC;


-- ============================================================
-- 5. Customer Segmentation by Order Frequency
-- ============================================================

WITH customer_frequency AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    CASE
        WHEN total_orders >= 15 THEN 'Highly Frequent'
        WHEN total_orders >= 8 THEN 'Frequent'
        WHEN total_orders >= 3 THEN 'Occasional'
        ELSE 'Low Frequency'
    END AS frequency_segment
FROM customer_frequency
ORDER BY total_orders DESC;


-- ============================================================
-- 6. Repeat Customer Analysis
-- ============================================================

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN total_orders = 1 THEN 'One-Time'
        ELSE 'Repeat'
    END AS customer_type,
    COUNT(*) AS customer_count,
    ROUND(
        COUNT(*) * 100.0 /
        NULLIF((SELECT COUNT(*) FROM customer_orders), 0),
        2
    ) AS customer_share_pct
FROM customer_orders
GROUP BY
    CASE
        WHEN total_orders = 1 THEN 'One-Time'
        ELSE 'Repeat'
    END
ORDER BY customer_count DESC;


-- ============================================================
-- 7. Customer Retention Proxy
--    Customers ordering in more than one calendar month
--    are treated as retained/re-engaged customers.
-- ============================================================

WITH customer_months AS (
    SELECT
        customer_id,
        COUNT(DISTINCT
            EXTRACT(YEAR FROM order_date) * 100
            + EXTRACT(MONTH FROM order_date)
        ) AS active_months
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN active_months > 1 THEN 'Multi-Month Customer'
        ELSE 'Single-Month Customer'
    END AS retention_segment,
    COUNT(*) AS customer_count,
    ROUND(
        COUNT(*) * 100.0 /
        NULLIF((SELECT COUNT(*) FROM customer_months), 0),
        2
    ) AS customer_share_pct
FROM customer_months
GROUP BY
    CASE
        WHEN active_months > 1 THEN 'Multi-Month Customer'
        ELSE 'Single-Month Customer'
    END
ORDER BY customer_count DESC;


-- ============================================================
-- 8. Regional Customer Analysis
-- ============================================================

SELECT
    region,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS average_order_value,
    ROUND(AVG(delivery_delay_min), 2) AS average_delivery_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY region
ORDER BY total_order_value DESC;


-- ============================================================
-- 9. Customer Experience Analysis
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS on_time_rate_pct,
    CASE
        WHEN AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) >= 0.50
            THEN 'Poor Experience'
        WHEN AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) >= 0.25
            THEN 'Needs Attention'
        ELSE 'Good Experience'
    END AS experience_segment
FROM delivery_mart
GROUP BY customer_id
ORDER BY average_delay_min DESC;


-- ============================================================
-- 10. High-Value Customers with Poor Delivery Experience
-- ============================================================

WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_order_value,
        AVG(delivery_delay_min) AS average_delay_min,
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) AS delay_rate
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    ROUND(total_order_value, 2) AS total_order_value,
    ROUND(average_delay_min, 2) AS average_delay_min,
    ROUND(delay_rate * 100, 2) AS delay_rate_pct
FROM customer_metrics
WHERE total_order_value >= 50000
  AND delay_rate >= 0.25
ORDER BY total_order_value DESC, delay_rate DESC;


-- ============================================================
-- 11. Customer Delivery Risk
-- ============================================================

WITH customer_risk AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
        AVG(delivery_delay_min) AS average_delay_min
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    delayed_orders,
    ROUND(average_delay_min, 2) AS average_delay_min,
    CASE
        WHEN delayed_orders >= 5 OR average_delay_min >= 30
            THEN 'High Risk'
        WHEN delayed_orders >= 2 OR average_delay_min >= 15
            THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS delivery_risk
FROM customer_risk
ORDER BY
    CASE
        WHEN delayed_orders >= 5 OR average_delay_min >= 30 THEN 1
        WHEN delayed_orders >= 2 OR average_delay_min >= 15 THEN 2
        ELSE 3
    END,
    average_delay_min DESC;


-- ============================================================
-- 12. Customer Order Trend by Month
-- ============================================================

SELECT
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    COUNT(DISTINCT customer_id) AS active_customers,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(order_value), 2) AS average_order_value,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date)
ORDER BY order_year, order_month;


-- ============================================================
-- 13. Customer Concentration
--    Measures how much of total order value comes from the
--    top 10% of customers.
-- ============================================================

WITH customer_value AS (
    SELECT
        customer_id,
        SUM(order_value) AS total_order_value
    FROM delivery_mart
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT
        customer_id,
        total_order_value,
        NTILE(10) OVER (ORDER BY total_order_value DESC) AS customer_decile
    FROM customer_value
)
SELECT
    customer_decile,
    COUNT(*) AS customers,
    ROUND(SUM(total_order_value), 2) AS order_value,
    ROUND(
        SUM(total_order_value) * 100.0 /
        NULLIF((SELECT SUM(total_order_value) FROM customer_value), 0),
        2
    ) AS value_share_pct
FROM ranked_customers
GROUP BY customer_decile
ORDER BY customer_decile;


-- ============================================================
-- 14. Customer Value vs Delivery Experience
-- ============================================================

WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_order_value,
        AVG(delivery_delay_min) AS average_delay_min,
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) AS delay_rate
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN total_order_value >= 50000 THEN 'High Value'
        WHEN total_order_value >= 20000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_orders), 2) AS avg_orders_per_customer,
    ROUND(AVG(total_order_value), 2) AS avg_customer_value,
    ROUND(AVG(average_delay_min), 2) AS avg_delay_min,
    ROUND(AVG(delay_rate) * 100, 2) AS avg_delay_rate_pct
FROM customer_metrics
GROUP BY
    CASE
        WHEN total_order_value >= 50000 THEN 'High Value'
        WHEN total_order_value >= 20000 THEN 'Medium Value'
        ELSE 'Low Value'
    END
ORDER BY avg_customer_value DESC;


-- ============================================================
-- 15. Customers with Repeated Delays
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    MAX(delivery_delay_min) AS maximum_delay_min
FROM delivery_mart
GROUP BY customer_id
HAVING SUM(CASE WHEN is_delayed = 1 THEN 1 ELSE 0 END) >= 3
ORDER BY delayed_orders DESC, average_delay_min DESC;


-- ============================================================
-- 16. Customer Value by Weather Condition
-- ============================================================

SELECT
    weather_condition,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_value), 2) AS total_order_value,
    ROUND(AVG(delivery_delay_min), 2) AS average_delay_min,
    ROUND(
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) * 100,
        2
    ) AS delay_rate_pct
FROM delivery_mart
GROUP BY weather_condition
ORDER BY total_order_value DESC;


-- ============================================================
-- 17. Executive Customer Snapshot
-- ============================================================

WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS orders,
        SUM(order_value) AS customer_value,
        AVG(delivery_delay_min) AS avg_delay,
        AVG(CASE WHEN is_delayed = 1 THEN 1.0 ELSE 0.0 END) AS delay_rate
    FROM delivery_mart
    GROUP BY customer_id
)
SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
    SUM(CASE WHEN customer_value >= 50000 THEN 1 ELSE 0 END) AS high_value_customers,
    SUM(CASE WHEN delay_rate >= 0.25 THEN 1 ELSE 0 END) AS customers_with_high_delay_rate,
    ROUND(AVG(orders), 2) AS avg_orders_per_customer,
    ROUND(AVG(customer_value), 2) AS avg_customer_value,
    ROUND(AVG(avg_delay), 2) AS avg_customer_delay_min
FROM customer_metrics;
