/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Purpose:
    This procedure loads data into the 'bronze' schema using external CSV files.
    It carries out the following steps:
    - Clears the bronze tables before loading.
    - Uses `BULK INSERT` to transfer data from CSV files into bronze tables.

Parameters:
    None.
    This procedure does not take parameters or return results.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS BEGIN
DECLARE @start_time DATE, @end_time DATE, @batchstart_time DATE, @batchend_time DATE

BEGIN TRY
SET @batchstart_time = GETDATE();
--INSERTING DATA FROM A FLAT FILE INTO CRM CUST INFO
PRINT '=============================================';
PRINT 'LOADING BRONZE LAYER';
PRINT '=============================================';

PRINT '---------------------------------------------';
PRINT 'LOADING CRM TABLES';
PRINT '---------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info'
TRUNCATE TABLE bronze.crm_cust_info;
PRINT '>> INSERT DATA INTO TABLE: bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\DIAMOND CHIZOTA\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK ); 
SET @end_time = GETDATE();
PRINT 'CRM CUST INFO EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';


-- INSERTING DATA FORM THE FLAT FILE INTO CRM PRD INFO
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info'
TRUNCATE TABLE bronze.crm_prd_info;
PRINT '>> INSERT DATA INTO TABLE: bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\DIAMOND CHIZOTA\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK ); 
SET @end_time = GETDATE();
PRINT 'CRM PRD INFO EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
	


-- INSERTING DATA FORM THE FLAT FILE INTO CRM SALES DETAILS
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details'
TRUNCATE TABLE bronze.crm_sales_details;
PRINT '>> INSERT DATA INTO TABLE: bronze.crm_sales_details'
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\DIAMOND CHIZOTA\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK ); 
SET @end_time = GETDATE();
PRINT 'CRM SALES DETAILS EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
	

PRINT '---------------------------------------------';
PRINT 'LOADING ERP TABLES';
PRINT '---------------------------------------------';

-- INSERTING DATA FORM THE FLAT FILE INTO ERP CUST AZ12
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12'
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT '>> INSERT DATA INTO TABLE: bronze.erp_cust_az12'
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\DIAMOND CHIZOTA\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK ); 
SET @end_time = GETDATE();
PRINT 'ERP CUST AZ12 EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';	


-- INSERTING DATA FORM THE FLAT FILE INTO ERP LOC A101
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101'
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT '>> INSERT DATA INTO TABLE: bronze.erp_loc_a101'
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\DIAMOND CHIZOTA\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK );
SET @end_time = GETDATE();
PRINT 'ERP LOC A101 EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';	
	


-- INSERTING DATA FORM THE FLAT FILE INTO ERP PX_CAT_G1V2
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12'
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT '>> INSERT DATA INTO TABLE: bronze.erp_loc_a101'
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\DIAMOND CHIZOTA\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK ); 
SET @end_time = GETDATE();
PRINT 'ERP PX_CAT_G1V2 EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';	

SET @batchend_time =GETDATE();
PRINT 'BATCH EXECUTION DURATION (IN SECONDS):' + ' ' + CAST(DATEDIFF(SECOND, @batchstart_time, @batchend_time) AS NVARCHAR) + 'SECONDS';
	END TRY
	BEGIN CATCH
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================='
	END CATCH
END 
