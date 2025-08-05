  /*
===============================================================================
DDL SCRIPT: CREATE GOLD VIEWS
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- CREATE DIMENSIONS: GOLD DIM CUSTOMERS
-- =============================================================================
  
  USE DataWarehouse
    GO 
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT ROW_NUMBER() OVER (ORDER BY cust_id )AS customer_key
       ,ci.cust_id AS customer_id
      ,ci.cust_key AS customer_number
      ,ci.cust_firstname AS first_name
      ,ci.cust_lastname AS last_name
       ,la.cntry AS country
      , CONCAT(
    UPPER(LEFT(ci.cust_marital_status, 1)),        
    LOWER(SUBSTRING(ci.cust_marital_status, 2, LEN(ci.cust_marital_status)))) AS marital_status
    
      ,CASE WHEN CONCAT(UPPER(LEFT(ci.cust_gndr, 1)),LOWER(SUBSTRING(ci.cust_gndr, 2, LEN(ci.cust_gndr)))) 
      != 'n/a' THEN CONCAT(UPPER(LEFT(ci.cust_gndr, 1)),LOWER(SUBSTRING(ci.cust_gndr, 2, LEN(ci.cust_gndr)))) 
  ELSE COALESCE(ca.gen, 'n/a') 
  END gender 
  ,ca.bdate AS birthdate
      ,ci.cust_create_date AS create_date
     
      
      --,ca.gen 
  FROM silver.crm_cust_info AS ci
  LEFT JOIN silver.erp_loc_a101 AS la ON
  ci.cust_key = la.cid
  LEFT JOIN silver.erp_cust_az12 AS ca ON
  ci.cust_key = ca.cid



 --GOLD DIMENSIONS PRODUCT

  CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY prd_key, prd_start_dt) AS product_key
      ,pn.[prd_id] AS product_id
      ,pn.[prd_key] AS product_number
      ,pn.[prd_nm] AS product_name
      ,pn.[cat_id] AS category_id
      ,pc.cat AS category
      ,pc.subcat AS sub_category
      ,pc.maintenance
      ,pn.[prd_cost] AS product_cost
      ,pn.[prd_line] AS product_line
      ,pn.[prd_start_dt] AS start_date
  FROM [DataWarehouse].[silver].[crm_prd_info] AS pn 
  LEFT JOIN silver.erp_px_cat_g1v2 AS pc
  ON pn.cat_id = pc.id
   WHERE prd_end_dt IS NULL --TO FILTER OUT HISTORICAL DATA

 --GOLD FACTS SALES

USE DataWarehouse
GO

CREATE VIEW gold.fact_sales
AS
SELECT sd.[sls_ord_num] AS order_number
      ,pr.product_key
      ,cu.customer_key
      ,sd.[sls_order_dt] AS order_date
      ,sd.[sls_ship_dt] AS shipping_date
      ,sd.[sls_due_dt] AS due_date
      ,sd.[sls_sales] AS sales_amount
      ,sd.[sls_quantity] AS quantity
      ,sd.[sls_price] AS price
      
  FROM[silver].[crm_sales_details] AS sd
  LEFT JOIN gold.dim_products AS pr
  ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold.dim_customers AS cu
  ON sd.sls_cust_id = cu.customer_id
