-- 1. Monthly revenue and sales trends
SELECT
    d.year,
    d.month,
    ROUND(SUM(f.net_revenue), 2) AS monthly_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(*) AS order_count
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2. Top 10 customers by completed revenue
SELECT
    f.customer_key AS customer_id,
    ROUND(SUM(f.net_revenue), 2) AS total_revenue,
    COUNT(*) AS order_count
FROM fact_sales f
WHERE LOWER(f.order_status) = 'completed'
GROUP BY f.customer_key
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Product category performance
SELECT
    p.category,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.net_revenue), 2) AS revenue,
    COUNT(*) AS order_count
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE LOWER(f.order_status) = 'completed'
GROUP BY p.category
ORDER BY revenue DESC;

-- 4. Regional sales performance
SELECT
    f.region,
    COUNT(*) AS order_count,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.net_revenue), 2) AS revenue
FROM fact_sales f
WHERE LOWER(f.order_status) = 'completed'
GROUP BY f.region
ORDER BY revenue DESC;

-- 5. Payment method analysis
SELECT
    f.payment_method,
    COUNT(*) AS order_count,
    ROUND(SUM(f.net_revenue), 2) AS revenue,
    ROUND(AVG(f.net_revenue), 2) AS average_order_value
FROM fact_sales f
WHERE LOWER(f.order_status) = 'completed'
GROUP BY f.payment_method
ORDER BY revenue DESC;

-- 6. Order status distribution
SELECT
    f.order_status,
    COUNT(*) AS order_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM fact_sales f
GROUP BY f.order_status
ORDER BY order_count DESC;

-- 7. Monthly completed revenue by product category
SELECT
    d.year,
    d.month,
    p.category,
    ROUND(SUM(f.net_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE LOWER(f.order_status) = 'completed'
GROUP BY d.year, d.month, p.category
ORDER BY d.year, d.month, revenue DESC;

-- 8. Average discount by product category
SELECT
    p.category,
    ROUND(AVG(f.discount_pct), 2) AS average_discount_pct,
    ROUND(SUM(f.discount_amount), 2) AS total_discount
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY average_discount_pct DESC;

-- 9. Daily sales trends
SELECT
    d.full_date,
    COUNT(*) AS order_count,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.net_revenue), 2) AS daily_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE LOWER(f.order_status) = 'completed'
GROUP BY d.full_date
ORDER BY d.full_date;

-- 10. Overall business KPI summary
SELECT
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (
        WHERE LOWER(order_status) = 'completed'
    ) AS completed_orders,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(net_revenue), 2) AS total_recorded_revenue,
    ROUND(SUM(net_revenue) FILTER (
        WHERE LOWER(order_status) = 'completed'
    ), 2) AS completed_revenue,
    ROUND(AVG(net_revenue) FILTER (
        WHERE LOWER(order_status) = 'completed'
    ), 2) AS completed_average_order_value,
    ROUND(AVG(discount_pct), 2) AS average_discount_pct
FROM fact_sales;
