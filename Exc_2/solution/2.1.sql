-- Activity 2.1: E-commerce analytical queries (PostgreSQL)

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
  order_id         SERIAL PRIMARY KEY,
  customer_name    TEXT,
  product_category TEXT,
  quantity         INTEGER,
  price_per_unit   NUMERIC(10,2),   -- NUMERIC for money: no float rounding errors
  order_date       DATE,
  country          TEXT
);

\COPY orders(customer_name,product_category,quantity,price_per_unit,order_date,country) FROM '/data/orders_1M.csv' DELIMITER ',' CSV HEADER;

\timing on
SELECT COUNT(*) FROM orders;

-- A. Order with the highest price_per_unit (all ties shown)
SELECT *
FROM orders
WHERE price_per_unit = (SELECT MAX(price_per_unit) FROM orders);

-- B. Top 3 product categories by total quantity sold
SELECT product_category, SUM(quantity) AS total_qty
FROM orders
GROUP BY product_category
ORDER BY total_qty DESC
LIMIT 3;

-- C. Revenue per product category
SELECT product_category, SUM(price_per_unit * quantity) AS revenue
FROM orders
GROUP BY product_category
ORDER BY revenue DESC;

-- D. Top 5 customers by total spending (with order counts, needed for E)
SELECT customer_name,
       SUM(price_per_unit * quantity) AS total_spent,
       COUNT(*)                       AS num_orders
FROM orders
GROUP BY customer_name
ORDER BY total_spent DESC
LIMIT 5;

-- E. Helper: how many distinct customer names are there?
SELECT COUNT(DISTINCT customer_name) FROM orders;
