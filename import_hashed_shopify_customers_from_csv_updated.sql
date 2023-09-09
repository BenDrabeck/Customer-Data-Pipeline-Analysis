
-- SQL script to import hashed customer data from the CSV file

-- Drop table if exists
DROP TABLE IF EXISTS hashed_shopify_customers;

-- Create the table
CREATE TABLE hashed_shopify_customers (
    customer_id TEXT,
first_name TEXT,
last_name TEXT,
email TEXT,
accepts_email_marketing TEXT,
company TEXT,
address1 TEXT,
address2 TEXT,
city TEXT,
province TEXT,
province_code TEXT,
country TEXT,
country_code TEXT,
zip TEXT,
phone TEXT,
accepts_sms_marketing TEXT,
total_spent TEXT,
total_orders TEXT,
tags TEXT,
note TEXT,
tax_exempt TEXT
);

-- Import data commands go here (you'll do this part manually or via a data ingestion process)
-- Example:
-- .mode csv
-- .import 'path/to/your/final_customers_data.csv' hashed_shopify_customers



-- Sample Queries for Analysis
-- 1. Count the number of customers in the table
SELECT COUNT(*) FROM hashed_shopify_customers;

-- 2. Find the top 10 customers by total spent
SELECT customer_id, total_spent 
FROM hashed_shopify_customers 
ORDER BY total_spent DESC 
LIMIT 10;


LOAD DATA INFILE '/mnt/data/hashed_customers_data.csv' INTO TABLE hashed_shopify_customers;
