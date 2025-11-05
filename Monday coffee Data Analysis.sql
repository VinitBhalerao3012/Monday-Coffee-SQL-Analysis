-- Activity 1
CREATE DATABASE monday_coffee;
USE monday_coffee;

-- Create customers exactly as per CSV
CREATE TABLE IF NOT EXISTS customers (
  customer_id   INT NOT NULL,
  customer_name VARCHAR(255) NOT NULL,
  city_id       INT NOT NULL,
  PRIMARY KEY (customer_id),
  KEY idx_customers_city_id (city_id)
) ENGINE=InnoDB;

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE '/Users/vinitbhalerao/Desktop/FC T/SQL/Monday Coffee /customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'      -- if it errors, try '\r\n'
IGNORE 1 LINES
(customer_id, customer_name, city_id);


-- Create Products exactly as per CSV
CREATE TABLE IF NOT EXISTS products (
  product_id   INT NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  price        DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (product_id)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE '/Users/vinitbhalerao/Desktop/FC T/SQL/Monday Coffee /products (1).csv'
INTO TABLE products
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(product_id, product_name, price);


-- Create Sales exactly as per CSV
CREATE TABLE IF NOT EXISTS sales (
  sale_id      INT NOT NULL,
  sale_date    DATE NOT NULL,
  product_id   INT NOT NULL,
  quantity     INT NOT NULL,
  customer_id  INT NOT NULL,
  total_amount DECIMAL(12,2) NOT NULL,
  rating       INT,
  PRIMARY KEY (sale_id),
  FOREIGN KEY (product_id)  REFERENCES products(product_id),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE '/Users/vinitbhalerao/Desktop/FC T/SQL/Monday Coffee /sales (4).csv'
INTO TABLE sales
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'      -- if it errors, try '\r\n'
IGNORE 1 LINES
(sale_id, @sale_date, product_id, quantity, customer_id, total_amount, rating)
SET sale_date = STR_TO_DATE(@sale_date, '%m/%d/%Y');


-- Activity 2
	-- NULLs in CUSTOMERS
	SELECT 
	  SUM(customer_id IS NULL)  AS null_customer_id,
	  SUM(customer_name IS NULL) AS null_customer_name,
	  SUM(city_id IS NULL)      AS null_city_id
	FROM customers;

	-- NULLs in PRODUCTS
	SELECT 
	  SUM(product_id IS NULL)    AS null_product_id,
	  SUM(product_name IS NULL)  AS null_product_name,
	  SUM(price IS NULL)         AS null_price
	FROM products;

	-- NULLs in SALES
	SELECT 
	  SUM(sale_id IS NULL)      AS null_sale_id,
	  SUM(sale_date IS NULL)    AS null_sale_date,
	  SUM(product_id IS NULL)   AS null_product_id,
	  SUM(quantity IS NULL)     AS null_quantity,
	  SUM(customer_id IS NULL)  AS null_customer_id,
	  SUM(total_amount IS NULL) AS null_total_amount,
	  SUM(rating IS NULL)       AS null_rating
	FROM sales;
    
-- 2.2 Duplicate records of cutomers table  
	-- A) Customers
	-- By ID (should be none because of the PK)
	SELECT customer_id, COUNT(*) AS cnt
	FROM customers
	GROUP BY customer_id
	HAVING cnt > 1;

	-- By natural key (same name in the same city)
	SELECT customer_name, city_id, COUNT(*) AS cnt
	FROM customers
	GROUP BY customer_name, city_id
	HAVING cnt > 1
	ORDER BY cnt DESC, customer_name;
	-- B) Products
    -- By ID
	SELECT product_id, COUNT(*) AS cnt
	FROM products
	GROUP BY product_id
	HAVING cnt > 1;

	-- Possible duplicates by name+price (same catalog item repeated)
	SELECT product_name, price, COUNT(*) AS cnt
	FROM products
	GROUP BY product_name, price
	HAVING cnt > 1
	ORDER BY cnt DESC, product_name;
    
    -- C) Sales 
    -- By sale_id (should be none because of the PK)
	SELECT sale_id, COUNT(*) AS cnt
	FROM sales
	GROUP BY sale_id
	HAVING cnt > 1;

	-- Potential duplicate orders: same customer, product, date, qty, and amount
	SELECT sale_date, customer_id, product_id, quantity, total_amount, COUNT(*) AS cnt
	FROM sales
	GROUP BY sale_date, customer_id, product_id, quantity, total_amount
	HAVING cnt > 1
	ORDER BY cnt DESC, sale_date;
    
	-- See each duplicate group and the IDs involved
	SELECT 
	  customer_name,
	  city_id,
	  GROUP_CONCAT(customer_id ORDER BY customer_id) AS ids_in_group,
	  COUNT(*) AS cnt
	FROM customers
	GROUP BY customer_name, city_id
	HAVING COUNT(*) > 1
	ORDER BY cnt DESC, customer_name;
    
    -- List all rows that belong to duplicate groups (so you can eyeball them)
		WITH dup_groups AS (
	  SELECT customer_name, city_id
	  FROM customers
	  GROUP BY customer_name, city_id
	  HAVING COUNT(*) > 1
	)
	SELECT c.*
	FROM customers c
	JOIN dup_groups d
	  ON c.customer_name = d.customer_name
	 AND c.city_id = d.city_id
	ORDER BY c.customer_name, c.city_id, c.customer_id;

	-- Build a merge plan preview (which ID to keep per group)
	WITH d AS (
	  SELECT customer_name, city_id
	  FROM customers
	  GROUP BY customer_name, city_id
	  HAVING COUNT(*) > 1
	),
	canon AS (
	  SELECT c.customer_name, c.city_id, MIN(c.customer_id) AS keep_id
	  FROM customers c
	  JOIN d USING (customer_name, city_id)
	  GROUP BY c.customer_name, c.city_id
	)
	SELECT 
	  c.customer_id AS old_id,
	  canon.keep_id,
	  c.customer_name,
	  c.city_id
	FROM customers c
	JOIN canon
	  ON c.customer_name = canon.customer_name
	 AND c.city_id = canon.city_id
	WHERE c.customer_id <> canon.keep_id
	ORDER BY canon.keep_id, old_id;

	-- Check impact on sales before we merge
	WITH d AS (
	  SELECT customer_name, city_id
	  FROM customers
	  GROUP BY customer_name, city_id
	  HAVING COUNT(*) > 1
	),
	canon AS (
	  SELECT c.customer_name, c.city_id, MIN(c.customer_id) AS keep_id
	  FROM customers c
	  JOIN d USING (customer_name, city_id)
	  GROUP BY c.customer_name, c.city_id
	),
	mapping AS (
	  SELECT c.customer_id AS old_id, canon.keep_id
	  FROM customers c
	  JOIN canon
		ON c.customer_name = canon.customer_name
	   AND c.city_id = canon.city_id
	  WHERE c.customer_id <> canon.keep_id
	)
	SELECT 
	  m.keep_id,
	  m.old_id,
	  COUNT(s.sale_id) AS affected_sales_rows
	FROM mapping m
	LEFT JOIN sales s
	  ON s.customer_id = m.old_id
	GROUP BY m.keep_id, m.old_id
	ORDER BY m.keep_id, m.old_id;

	-- Execute the merge (customers)
    
    START TRANSACTION;

	-- 1) Build a mapping (old_id -> keep_id) for duplicate name+city groups
	CREATE TEMPORARY TABLE dup_customer_map AS
	WITH d AS (
	  SELECT customer_name, city_id
	  FROM customers
	  GROUP BY customer_name, city_id
	  HAVING COUNT(*) > 1
	),
	canon AS (
	  SELECT c.customer_name, c.city_id, MIN(c.customer_id) AS keep_id
	  FROM customers c
	  JOIN d USING (customer_name, city_id)
	  GROUP BY c.customer_name, c.city_id
	)
	SELECT c.customer_id AS old_id, canon.keep_id
	FROM customers c
	JOIN canon
	  ON c.customer_name = canon.customer_name
	 AND c.city_id = canon.city_id
	WHERE c.customer_id <> canon.keep_id;

	-- 2) Point existing sales to the kept customer_id
	UPDATE sales s
	JOIN dup_customer_map m ON s.customer_id = m.old_id
	SET s.customer_id = m.keep_id;

	-- 3) Delete the duplicate customer rows (the old IDs)
	DELETE c
	FROM customers c
	JOIN dup_customer_map m ON c.customer_id = m.old_id;

	COMMIT;
	-- Sanity checks
	-- No duplicate name+city left?
	SELECT customer_name, city_id, COUNT(*) AS cnt
	FROM customers
	GROUP BY customer_name, city_id
	HAVING cnt > 1;

	-- All sales now reference existing customers?
	SELECT COUNT(*) AS missing_customer_refs
	FROM sales s
	LEFT JOIN customers c ON c.customer_id = s.customer_id
	WHERE c.customer_id IS NULL;


-- 2.3 — Check total_amount vs. price × quantity
	-- A) Find any mismatches (preview)
	-- How many rows don’t match?
	SELECT COUNT(*) AS mismatches
	FROM sales s
	JOIN products p USING (product_id)
	WHERE s.total_amount <> p.price * s.quantity;

	-- Show a sample of the mismatches (if any)
	SELECT 
    s.sale_id,
    s.product_id,
    p.product_name,
    s.quantity,
    p.price AS product_price,
    s.total_amount,
    (p.price * s.quantity) AS expected_total,
    (s.total_amount - p.price * s.quantity) AS diff
FROM
    sales s
        JOIN
    products p USING (product_id)
WHERE
    s.total_amount <> p.price * s.quantity
ORDER BY ABS(s.total_amount - p.price * s.quantity) DESC
LIMIT 20;
    
-- B) (Only if mismatches exist) Fix them

 
	START TRANSACTION;

	UPDATE sales s
	JOIN products p ON p.product_id = s.product_id
	SET s.total_amount = p.price * s.quantity
	WHERE s.total_amount <> p.price * s.quantity;

	COMMIT;
	--  Verify it worked
	SELECT COUNT(*) AS mismatches_after_fix
	FROM sales s
	JOIN products p USING (product_id)
	WHERE s.total_amount <> p.price * s.quantity;

-- Activity 3 
	-- 3.1 — Create the reporting view
	USE monday_coffee;

	DROP VIEW IF EXISTS v_sales_report;
	CREATE VIEW v_sales_report AS
	SELECT
	  s.sale_id,
	  s.sale_date,
	  s.customer_id,
	  c.customer_name,
	  c.city_id,
	  s.product_id,
	  p.product_name,
	  p.price            AS product_price,
	  s.quantity,
	  (p.price * s.quantity) AS expected_total,
	  s.total_amount,
	  s.rating
	FROM sales     AS s
	JOIN customers AS c USING (customer_id)
	JOIN products  AS p USING (product_id);

	-- Quick Check 
	-- Should match the sales row count
	SELECT (SELECT COUNT(*) FROM sales)   AS sales_rows,
		   (SELECT COUNT(*) FROM v_sales_report) AS view_rows;

	-- Peek at a few rows
	SELECT * FROM v_sales_report ORDER BY sale_date, sale_id LIMIT 10;

	-- Date range present in the data
	SELECT MIN(sale_date) AS first_date, MAX(sale_date) AS last_date
	FROM v_sales_report;

--  Step 3.2: City-level KPIs.
	-- KPIs by city
	SELECT
	  city_id,
	  COUNT(*)                         AS orders,
	  COUNT(DISTINCT customer_id)      AS unique_customers,
	  SUM(quantity)                    AS items_sold,
	  SUM(total_amount)                AS revenue,
	  ROUND(SUM(total_amount)/COUNT(*), 2) AS aov,        -- Average Order Value
	  ROUND(AVG(rating), 2)            AS avg_rating      -- NULLs ignored automatically
	FROM v_sales_report
	GROUP BY city_id
	ORDER BY revenue DESC;
	--  overall KPIs for a quick sanity check:
	SELECT
	  COUNT(*)                         AS orders,
	  COUNT(DISTINCT customer_id)      AS unique_customers,
	  SUM(quantity)                    AS items_sold,
	  SUM(total_amount)                AS revenue,
	  ROUND(SUM(total_amount)/COUNT(*), 2) AS aov,
	  ROUND(AVG(rating), 2)            AS avg_rating
	FROM v_sales_report;

	-- Step 3.3 — Product demand & top sellers
	-- Demand summary by product( Overall product demand)
	SELECT
	  product_id,
	  product_name,
	  COUNT(*)                         AS orders,
	  SUM(quantity)                    AS units_sold,
	  SUM(total_amount)                AS revenue,
	  ROUND(AVG(rating),2)             AS avg_rating
	FROM v_sales_report
	GROUP BY product_id, product_name
	ORDER BY units_sold DESC;

	-- Top 5 products(Two ways)
	-- By units sold
	SELECT product_name, SUM(quantity) AS units_sold
	FROM v_sales_report
	GROUP BY product_name
	ORDER BY units_sold DESC
	LIMIT 5;

	-- By revenue
	SELECT product_name, SUM(total_amount) AS revenue
	FROM v_sales_report
	GROUP BY product_name
	ORDER BY revenue DESC
	LIMIT 5;

	-- Product demand by city (who buys what)
	SELECT
	  city_id,
	  product_name,
	  SUM(quantity)     AS units_sold,
	  SUM(total_amount) AS revenue
	FROM v_sales_report
	GROUP BY city_id, product_name
	ORDER BY city_id, units_sold DESC;

	-- Top product per city (winner in each city) 
	WITH ranked AS (
	  SELECT
		city_id,
		product_name,
		SUM(quantity) AS units_sold,
		ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY SUM(quantity) DESC) AS rn
	  FROM v_sales_report
	  GROUP BY city_id, product_name
	)
	SELECT city_id, product_name, units_sold
	FROM ranked
	WHERE rn = 1
	ORDER BY city_id;

	-- Step 3.4.1 — Monthly revenue & order trends
	SELECT
	  DATE_FORMAT(sale_date, '%Y-%m') AS month,
	  COUNT(*)                        AS total_orders,
	  SUM(total_amount)               AS total_revenue,
	  ROUND(AVG(total_amount), 2)     AS avg_order_value,
	  ROUND(AVG(rating), 2)           AS avg_rating
	FROM v_sales_report
	GROUP BY DATE_FORMAT(sale_date, '%Y-%m')
	ORDER BY month;

	-- Step 3.4.2 — Month-over-month growth (revenue growth %)
	WITH monthly AS (
	  SELECT
		DATE_FORMAT(sale_date, '%Y-%m') AS month,
		SUM(total_amount) AS revenue
	  FROM v_sales_report
	  GROUP BY DATE_FORMAT(sale_date, '%Y-%m')
	)
	SELECT
	  month,
	  revenue,
	  ROUND(
		(revenue - LAG(revenue) OVER (ORDER BY month)) 
		/ LAG(revenue) OVER (ORDER BY month) * 100, 2
	  ) AS revenue_growth_percent
	FROM monthly
	ORDER BY month;

	-- Step 3.4.3 — Moving 3-month average revenue (smoothing trend)
	WITH monthly AS (
	  SELECT
		DATE_FORMAT(sale_date, '%Y-%m') AS month,
		SUM(total_amount) AS revenue
	  FROM v_sales_report
	  GROUP BY DATE_FORMAT(sale_date, '%Y-%m')
	)
	SELECT
	  month,
	  revenue,
	  ROUND(
		AVG(revenue) OVER (
		  ORDER BY month
		  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
		), 2
	  ) AS moving_avg_3m
	FROM monthly
	ORDER BY month;

	-- Step 3.5 — Advanced Insights
	-- Step 3.5.1 — Top customers (most valuable)
	SELECT
	  customer_id,
	  customer_name,
	  COUNT(*) AS total_orders,
	  SUM(total_amount) AS total_spent,
	  ROUND(AVG(total_amount),2) AS avg_order_value
	FROM v_sales_report
	GROUP BY customer_id, customer_name
	ORDER BY total_spent DESC
	LIMIT 10;

	-- Step 3.5.2 — Best-selling & highest-rated products
	-- Top 5 products by revenue
	SELECT
	  product_name,
	  SUM(total_amount) AS revenue,
	  SUM(quantity) AS total_units,
	  ROUND(AVG(rating),2) AS avg_rating
	FROM v_sales_report
	GROUP BY product_name
	ORDER BY revenue DESC
	LIMIT 5;

	-- Top 5 highest-rated products (with at least 5 ratings)
	SELECT
	  product_name,
	  COUNT(rating) AS rating_count,
	  ROUND(AVG(rating),2) AS avg_rating
	FROM v_sales_report
	WHERE rating IS NOT NULL
	GROUP BY product_name
	HAVING COUNT(rating) >= 5
	ORDER BY avg_rating DESC
	LIMIT 5;

	-- Step 3.5.3 — City performance comparison
	SELECT
	  city_id,
	  SUM(total_amount) AS revenue,
	  COUNT(DISTINCT customer_id) AS unique_customers,
	  ROUND(AVG(rating),2) AS avg_rating,
	  ROUND(SUM(total_amount)/COUNT(*),2) AS aov
	FROM v_sales_report
	GROUP BY city_id
	ORDER BY revenue DESC;
	-- Step 3.5.4 — Top city-product combos
	SELECT
	  city_id,
	  product_name,
	  SUM(quantity) AS total_units,
	  SUM(total_amount) AS total_revenue
	FROM v_sales_report
	GROUP BY city_id, product_name
	ORDER BY total_revenue DESC
	LIMIT 10;


-- Activity 4 — Final Reporting & Export (the presentation layer).  
	-- Step 4.1 — Create a final summary view
	USE monday_coffee;

	DROP VIEW IF EXISTS v_sales_summary;
	CREATE VIEW v_sales_summary AS
	SELECT
	  city_id,
	  COUNT(DISTINCT customer_id) AS unique_customers,
	  COUNT(*)                    AS total_orders,
	  SUM(quantity)               AS total_units_sold,
	  SUM(total_amount)           AS total_revenue,
	  ROUND(SUM(total_amount)/COUNT(*), 2) AS avg_order_value,
	  ROUND(AVG(rating), 2)       AS avg_rating
	FROM v_sales_report
	GROUP BY city_id
	ORDER BY total_revenue DESC;

	SELECT * FROM v_sales_summary;

-- Activity 5: Decision-Making & Recommendations
	-- Step 5.1 — Identify Top 3 Cities
	SELECT city_id,
		   total_revenue,
		   unique_customers,
		   total_orders,
		   RANK() OVER (ORDER BY total_revenue DESC)      AS rev_rank,
		   RANK() OVER (ORDER BY unique_customers DESC)   AS cust_rank,
		   RANK() OVER (ORDER BY total_orders DESC)       AS order_rank,
		   (RANK() OVER (ORDER BY total_revenue DESC)
		  + RANK() OVER (ORDER BY unique_customers DESC)
		  + RANK() OVER (ORDER BY total_orders DESC)) AS combined_score
	FROM v_sales_summary
	ORDER BY combined_score ASC
	LIMIT 3;

	-- Step 5.2 — Final Recommendations (qualitative)
	-- 1.	Expand footprint in these top cities via new café locations or delivery partnerships.
	-- 	2.	Boost marketing in second-tier cities showing upward growth trends (from your monthly data).
	-- 3.	Promote top-selling products (e.g., Instant Coffee Powder 100 g, Specialty Subscription) nationwide.
	-- 4.	Use loyalty programs in mature cities to increase repeat orders and maintain AOV.
	-- 5.	Continue monitoring monthly sales and customer ratings to refine the strategy quarterly.
   
   
	

    
