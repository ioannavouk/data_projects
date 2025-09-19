SELECT *
FROM coffee_shop_sales
ORDER BY 1;

CREATE TABLE sales_2
LIKE coffee_shop_sales;

SELECT *
FROM sales_2;

INSERT sales_2
SELECT * FROM coffee_shop_sales;

DESCRIBE sales_2;

-- DUPLICATES
    
WITH duplicate_rows AS (
	SELECT transaction_id, ROW_NUMBER() OVER (PARTITION BY transaction_id) AS row_num
    FROM sales_2
) 
SELECT *
FROM duplicate_rows
WHERE row_num>=2;

-- no duplicate transactions found


-- COLUMNS

ALTER TABLE sales_2
DROP COLUMN transaction_time;

SELECT *
FROM sales_2
order by 1;

UPDATE sales_2
SET transaction_date = STR_TO_DATE(transaction_date, '%d/%m/%Y');

ALTER TABLE sales_2
MODIFY COLUMN transaction_date DATE;

SELECT DISTINCT store_location
FROM sales_2;

--
SELECT DISTINCT unit_price
FROM sales_2; -- all 3

SELECT DISTINCT Total_Bill
FROM sales_2; -- all 3

SELECT DISTINCT transaction_qty
FROM sales_2; -- all 3

-- all is 3 so it's not giving us anything to work with

ALTER TABLE sales_2
DROP COLUMN Total_Bill;

ALTER TABLE sales_2
DROP COLUMN unit_price;

ALTER TABLE sales_2
DROP COLUMN transaction_qty;

SELECT *
FROM sales_2
order by 1;

SELECT DISTINCT product_category
FROM sales_2;

SELECT DISTINCT product_type
FROM sales_2;

UPDATE sales_2
SET product_type = 'Brewed Herbal Tea'
WHERE product_type = 'Brewed herbal tea';

UPDATE sales_2
SET product_type = 'Drip Coffee'
WHERE product_type = 'Drip coffee';

UPDATE sales_2
SET product_type = 'Organic Brewed Coffee'
WHERE product_type = 'Organic brewed coffee'
;

SELECT DISTINCT product_detail
FROM sales_2;

UPDATE sales_2
SET product_detail = 'Espresso Shot'
WHERE product_detail = 'Espresso shot';

UPDATE sales_2
SET product_detail = 'Ouro Brasileiro Shot'
WHERE product_detail = 'Ouro Brasileiro shot';

SELECT DISTINCT Size, COUNT(Size)
FROM sales_2
GROUP BY Size
Order by Size DESC;

SELECT * 
FROM sales_2
WHERE Size = 'Not Defined'
ORDER BY transaction_id;

SELECT * 
FROM sales_2
WHERE product_detail LIKE '%shot%' AND Size = 'Not Defined'
ORDER BY transaction_id;

-- noticing only shot goes with not defined so changing to small

UPDATE sales_2
SET Size = 'Small'
WHERE product_detail Like '%shot%';

SELECT DISTINCT Month Name
FROM sales_2
Order by 1;

SELECT DISTINCT `Day of Week`,`Day Name`, Count(`Day Name`)
FROM sales_2
Group by `Day Name`,`Day of Week`
Order by `Day of Week`;

SELECT * 
FROM sales_2;

SELECT DISTINCT Hour
FROM sales_2
order by 1;

-- add time period column

ALTER TABLE sales_2
ADD COLUMN time_period VARCHAR(20);

UPDATE sales_2
SET time_period = CASE 
    WHEN HOUR <=11 THEN 'Morning'
		WHEN HOUR <=14 THEN 'Noon'
        WHEN HOUR <=16 THEN 'Afternoon'
        WHEN HOUR <=20 THEN 'Evening'
END;

SELECT * 
FROM sales_2;

-- didn't find any null values

SELECT distinct store_location,store_id
FROM sales_2
order by store_id;

-- dropping store_id because we already have the 3 locations to identify from ASTORIA-3, LOWER M.-5, HELL'S KITCHEN-8
ALTER TABLE sales_2
DROP COLUMN store_id;

--
-- 
-- EXPLORATORY DATA ANALYSIS
--
--

SELECT *
FROM sales_2
order by transaction_id;

SELECT MIN(transaction_date) AS first_date, MAX(transaction_date) AS last_date
FROM sales_2;

SELECT store_location, COUNT(transaction_id)
FROM sales_2
GROUP BY store_location
ORDER BY 2 DESC;
-- ASTORIA BEING THE ONE WITH MOST TRANSACTIONS

SELECT Month, COUNT(transaction_id)
FROM sales_2
GROUP BY Month
ORDER BY 1 ASC;

WITH total_by_month AS(
	SELECT Month, COUNT(transaction_id) total_trans
	FROM sales_2
	GROUP BY Month
    )
    SELECT Month, total_trans, sum(total_trans) OVER (ORDER BY Month) AS Rolling_Total
    FROM total_by_month
    GROUP BY Month;
    
-- SUM OF TRANSACTIONS FROM JAN - JUN IS 12657 
-- after the 3rd month we can see an increase in sales

WITH total_by_month AS(
	SELECT Month, COUNT(transaction_id) total_trans
	FROM sales_2
	GROUP BY Month
    )
    SELECT Month, total_trans, sum(total_trans) OVER (ORDER BY Month) AS Rolling_Total_Transactions
    FROM total_by_month
    GROUP BY Month
    ORDER BY total_trans;
    
-- most transactions done in Jun(2971) and least in Feb(1388)


WITH product_type_count AS (
    SELECT product_type, COUNT(transaction_id) AS count
    FROM sales_2
    GROUP BY product_type
),
rank_by_product AS (
    SELECT product_type, count,
           DENSE_RANK() OVER (ORDER BY count DESC) AS ranking
    FROM product_type_count
)
SELECT *
FROM rank_by_product;

-- brewed herbal tea had the most transactions


WITH product_type_count AS (
    SELECT product_type, month, COUNT(transaction_id) AS count
    FROM sales_2
    GROUP BY product_type, month
),
rank_by_product AS (
    SELECT product_type, month, count,
           DENSE_RANK() OVER (PARTITION BY month ORDER BY count DESC) AS ranking
    FROM product_type_count
)
SELECT month, product_type,count
FROM rank_by_product
where ranking = 1
ORDER BY month, ranking
;

-- we can see that black and herbal tea scored the highest sales in the first half of 2023

select distinct product_type
from sales_2;

WITH product_type_count AS (
    SELECT product_type, month, COUNT(transaction_id) AS count
    FROM sales_2
    GROUP BY product_type, month
),
rank_by_product AS (
    SELECT product_type, month, count,
           DENSE_RANK() OVER (PARTITION BY month ORDER BY count DESC) AS ranking
    FROM product_type_count
)
SELECT month, product_type,count,ranking
FROM rank_by_product
where ranking>5
ORDER BY month, ranking
;

-- chai and green tea seem to be having the least sales in the first half of 2023

SELECT store_location, product_category, COUNT(*) AS sales_count
FROM sales_2
GROUP BY store_location, product_category
ORDER BY sales_count DESC;

-- astoria store had the most coffee and tea sales

SELECT store_location, time_period, COUNT(*) AS transactions
FROM sales_2
GROUP BY store_location, time_period
ORDER BY store_location, transactions DESC;

-- all stores had the most sales in the morning hours (peak) with hell's kitchen having the most at that time
-- while both hell's kitchen and lower manhattan sales go down after the morning hours astoria store keeps a stable number of sales throughout the day


SELECT
    store_location, month,
    COUNT(CASE WHEN product_category = 'tea' THEN 1 END) AS tea_transactions,
    COUNT(CASE WHEN product_category = 'coffee' THEN 1 END) AS coffee_transactions
FROM sales_2
WHERE time_period = 'Morning'
GROUP BY store_location, month
ORDER BY month, store_location;



WITH monthly_sales AS (
    SELECT
        store_location, month,
        COUNT(CASE WHEN product_category = 'tea' THEN 1 END) AS tea_transactions
    FROM sales_2
    GROUP BY store_location, month
),
ranked_sales AS (
    SELECT
        store_location, month, tea_transactions,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY tea_transactions DESC) AS tea_rank
    FROM monthly_sales
)
SELECT
    month,
    store_location AS top_tea_store,
    tea_transactions,
tea_rank
FROM ranked_sales
ORDER BY month,tea_rank;

WITH monthly_sales AS (
    SELECT
        store_location, month,
        COUNT(CASE WHEN product_category = 'coffee' THEN 1 END) AS coffee_transactions
    FROM sales_2
    GROUP BY store_location, month
),
ranked_sales AS (
    SELECT
        store_location, month, coffee_transactions,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY coffee_transactions DESC) AS coffee_rank
    FROM monthly_sales
)
SELECT
    month,
    store_location AS top_coffee_store,
    coffee_transactions,coffee_rank
FROM ranked_sales
ORDER BY month,coffee_rank;

-- astoria=most tea, hell's kitchen=most coffee



-- peak time by day of week in temp table --

CREATE TEMPORARY TABLE peak_sales_period AS
WITH time_of_day AS (
    SELECT 
        `Day of Week`, 
        COUNT(CASE WHEN time_period = 'Morning' THEN 1 END) AS morning,
        COUNT(CASE WHEN time_period = 'Noon' THEN 1 END) AS noon,
        COUNT(CASE WHEN time_period = 'Afternoon' THEN 1 END) AS afternoon,
        COUNT(CASE WHEN time_period = 'Evening' THEN 1 END) AS evening
    FROM sales_2
    GROUP BY `Day of Week`
    ORDER BY 1
), 
most_period AS (
    SELECT 
        `Day of Week`, morning, noon, afternoon, evening,
        GREATEST(morning, noon, afternoon, evening) AS most
    FROM time_of_day
)
SELECT 
    `Day of Week`, morning, noon, afternoon, evening,
    CASE 
        WHEN most = morning THEN 'Morning'
        WHEN most = noon THEN 'Noon'
        WHEN most = afternoon THEN 'Afternoon'
        ELSE 'Evening'
    END AS peak_time_period
FROM most_period
ORDER BY `Day of Week`;

SELECT * FROM peak_sales_period;

--
-- 

SELECT 
    `Day of Week`,
    product_type,
    COUNT(*) AS transactions
FROM sales_2
WHERE product_category = 'Coffee'
GROUP BY `Day of Week`, product_type
ORDER BY `Day of Week`, transactions DESC;

SELECT 
    `Day of Week`,
    product_type,
    COUNT(*) AS transactions
FROM sales_2
WHERE product_category = 'Tea'
GROUP BY `Day of Week`, product_type
ORDER BY `Day of Week`, transactions DESC;

select distinct product_detail
from sales_2;

DELIMITER $$

CREATE PROCEDURE GetStoreTransactionsByMonth(
    IN location TEXT,
    IN month_num INT
)
BEGIN
    SELECT 
        store_location,
        Month,
        COUNT(*) AS total_transactions
    FROM sales_2
    WHERE store_location = location
      AND Month = month_num
    GROUP BY store_location, Month;
END$$

DELIMITER ;

CALL GetStoreTransactionsByMonth('Astoria', 5);
CALL GetStoreTransactionsByMonth("Hell's Kitchen", 3);
CALL GetStoreTransactionsByMonth('Lower Manhattan', 4);
