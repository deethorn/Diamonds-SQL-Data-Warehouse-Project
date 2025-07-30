CREATE OR ALTER PROCEDURE silver.load_silver 
AS BEGIN
DECLARE @start_time DATE, @end_time DATE, @batchstart_time DATE, @batchend_time DATE

--BEGIN TRY
SET @batchstart_time = GETDATE();
PRINT '=============================================';
PRINT 'LOADING SILVER LAYER';
PRINT '=============================================';

PRINT '---------------------------------------------';
PRINT 'LOADING CRM TABLES';
PRINT '---------------------------------------------';

--INSERTING DATA FROM BRONZE CRM CUST INFO INTO SILVER CRM CUST INFO
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> INSERT DATA INTO TABLE: silver.crm_cust_info'
INSERT INTO silver.crm_cust_info(
cust_id,
cust_key,
cust_firstname,
cust_lastname,
cust_marital_status,
cust_gndr,
cust_create_date )
SELECT
	cust_id,
	cust_key,
	TRIM(cust_firstname)AS cst_firstname,
	TRIM(cust_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cust_marital_status))= 'S' THEN 'SINGLE'
		WHEN UPPER(TRIM(cust_marital_status))= 'M' THEN 'MARRIED'
		ELSE 'N/A'
	END cust_marital_status,
	CASE WHEN UPPER(TRIM(cust_gndr))= 'F' THEN 'FEMALE'
		WHEN UPPER(TRIM(cust_gndr))= 'M' THEN 'MALE'
		ELSE 'N/A'
	END cust_gndr,
	cust_create_date
	FROM (
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cust_create_date DESC)
		AS flag_last FROM bronze.crm_cust_info WHERE cust_id IS NOT NULL) T 
		WHERE flag_last = 1;
	
SET @end_time = GETDATE();
PRINT 'CRM CUST INFO EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
--Cast(@start_time AS NVARCHAR) +' '+ cast(@end_time AS NVARCHAR)


-- INSERTING DATA FROM BRONZE CRM PRD INFO INTO SILVER CRM PRD INFO
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> INSERT DATA INTO TABLE: silver.crm_prd_info'

INSERT INTO  silver.crm_prd_info(
 prd_id,
 cat_id,
 prd_key,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt 
 )
SELECT [prd_id]
      ,REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id
      ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
      ,[prd_nm]
      ,[prd_cost]
      ,CASE UPPER(TRIM(prd_line))
            WHEN 'T' THEN 'Touring'
            WHEN 'M' THEN 'Mountains'
            WHEN 'R' THEN 'Roads'
            WHEN 'S' THEN 'Other Sales'
            ELSE 'N/A'
        END prd_line
      ,CAST([prd_start_dt] AS DATE)
      ,CAST(LEAD([prd_start_dt]) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE)AS prd_end_dt        
  FROM [DataWarehouse].[bronze].[crm_prd_info]

SET @end_time = GETDATE();
PRINT 'CRM PRD INFO EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
	


-- INSERTING DATA FROM BRONZE CRM SALES DETAILS INTO SILVER CRM SALES DETAILS
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> INSERT DATA INTO TABLE: silver.crm_sales_details'
INSERT INTO silver.crm_sales_details(
 [sls_ord_num]
,[sls_prd_key]
,[sls_cust_id],
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
[sls_quantity],
[sls_price]
)

SELECT
       [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id],
      CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 THEN NULL
      ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) END AS sls_order_dt
      
      ,CASE WHEN [sls_ship_dt] <= 0 OR LEN([sls_ship_dt])!= 8 THEN NULL
      ELSE CAST(CAST([sls_ship_dt] AS NVARCHAR) AS DATE) END AS [sls_ship_dt]
      
      ,CASE WHEN [sls_due_dt] <= 0 OR LEN([sls_due_dt])!= 8 THEN NULL
      ELSE CAST(CAST([sls_due_dt] AS NVARCHAR) AS DATE) END AS [sls_due_dt]
      
      ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
          THEN sls_quantity * ABS(sls_price)
          ELSE sls_sales 
       END AS sls_sales
      ,[sls_quantity]
      ,CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF (sls_quantity, 0)
        ELSE sls_price
      END AS sls_price
  FROM [DataWarehouse].[bronze].[crm_sales_details]
SET @end_time = GETDATE();
PRINT 'CRM SALES DETAILS EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
	

PRINT '---------------------------------------------';
PRINT 'LOADING ERP TABLES';
PRINT '---------------------------------------------';

-- INSERTING DATA FROM BRONZE ERP CUST AZ12 INTO SILVER ERP CUST AZ12
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> INSERT DATA INTO TABLE: silver.erp_cust_az12'
INSERT INTO silver.erp_cust_az12(cid ,bdate, gen)
SELECT
      CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid, 4, LEN(cid))
      ELSE cid
      END AS cid
      ,CASE WHEN bdate > GETDATE() THEN NULL
          ELSE bdate
      END AS bdate
      ,CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
           WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
           ELSE 'n/a'
       END AS gen
  FROM bronze.[erp_cust_az12]
SET @end_time = GETDATE();
PRINT 'ERP CUST AZ12 EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';	


-- INSERTING DATA FROM BRONZE ERP LOC A101 INTO SILVER ERP LOC A101
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> INSERT DATA INTO TABLE: silver.erp_loc_a101'
INSERT INTO [silver].[erp_loc_a101](cid, cntry)
SELECT distinct
REPLACE(cid, '-','') AS cid
,CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM [bronze].[erp_loc_a101]
SET @end_time = GETDATE();
PRINT 'ERP LOC A101 EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';	
	


-- INSERTING DATA FROM BRONZE ERP PX_CAT_G1V2 INTO SILVER ERP PX_CAT_G1V2
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> INSERT DATA INTO TABLE: silver.erp_loc_a101'
INSERT INTO silver.erp_px_cat_g1v2 ([id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
      )
SELECT [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]
SET @end_time = GETDATE();
PRINT 'ERP PX_CAT_G1V2 EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';	

SET @batchend_time =GETDATE();
PRINT 'BATCH EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @batchstart_time, @batchend_time) AS NVARCHAR) + 'SECONDS';
	END TRY
	BEGIN CATCH
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================='
	END CATCH
END 
