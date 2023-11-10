
  
    

    create or replace table `online-retail-project`.`retail`.`dim_customer`
      
    
    

    OPTIONS()
    as (
      -- dim_customer.sql

-- Create the dimension table
WITH customer_cte AS (
	SELECT DISTINCT
	    to_hex(md5(cast(coalesce(cast(CustomerID as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as string), '_dbt_utils_surrogate_key_null_') as string))) as customer_id,
	    Country AS country
	FROM `online-retail-project`.`retail`.`raw_invoices`
	WHERE CustomerID IS NOT NULL
)
SELECT
    t.*,
	cm.alpha3Code
FROM customer_cte t
LEFT JOIN `online-retail-project`.`retail`.`country` cm ON t.country = cm.name
    );
  