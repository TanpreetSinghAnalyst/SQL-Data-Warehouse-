	USE DataWarehouse;
	GO
	EXEC silver.load_silver;
	GO

		/*
	===============================================================================
	Stored Procedure: Load Silver Layer (Bronze -> Silver)
	===============================================================================
	Script Purpose:
		This stored procedure performs the ETL (Extract, Transform, Load) process to 
		populate the 'silver' schema tables from the 'bronze' schema.
		Actions Performed:
			- Truncates Silver tables.
			- Inserts transformed and cleansed data from Bronze into Silver tables.
		
	Parameters:
		None. 
			This stored procedure does not accept any parameters or return any values.

	Usage Example:
		EXEC Silver.load_silver;
	===============================================================================
	*/

	CREATE OR ALTER PROCEDURE silver.load_silver AS
	BEGIN
		DECLARE 
			@Start_time       DATETIME,
			@end_time         DATETIME,  
			@batch_Start_time DATETIME,
			@batch_end_time   DATETIME;

		BEGIN TRY
			PRINT '=================================================================';
			PRINT 'LOADING SILVER LAYER';
			PRINT '=================================================================';

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------------------';

			/* =====================================================================================
			   CRM Customer Info
			   ===================================================================================== */
			SET @batch_Start_time = GETDATE();
			SET @Start_time = GETDATE();

			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;

			PRINT '>> Inserting Data Into: silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,    -- Remove extra spaces from first name
				TRIM(cst_lastname)  AS cst_lastname,     -- Remove extra spaces from last name
				CASE 
					WHEN cst_marital_status = 'M' THEN 'Married'  -- Map single-letter codes to labels
					WHEN cst_marital_status = 'S' THEN 'Single'
					ELSE 'N/A' 
				END AS cst_marital_status,
				CASE
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'   -- Standardize gender to Male/Female/N/A
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					ELSE 'N/A' 
				END AS cst_gndr,
				cst_create_date
			FROM (
				SELECT *,                                   
					   ROW_NUMBER() OVER (                   -- Deduplicate by keeping the latest record per customer
						   PARTITION BY cst_ID 
						   ORDER BY cst_create_date DESC
					   ) AS flag_last
				FROM bronze.crm_cust_info
			) t
			WHERE flag_last = 1                              -- Keep only the latest record
			  AND cst_id IS NOT NULL;                        -- Exclude records missing primary ID

			SET @end_time = GETDATE();
			PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------'; 

			/* =====================================================================================
			   CRM Product Info
			   ===================================================================================== */
			SET @Start_time = GETDATE();

			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;

			PRINT '>> Inserting Data Into: silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract first 5 chars, replace '-' with '_'
				SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         -- Extract product key starting from position 7
				prd_nm,
				ISNULL(prd_cost, 0) AS prd_cost,                        -- Default missing product cost to 0
				CASE 
					WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'    -- Map codes to product line names
					WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line, 
				CAST(prd_start_dt AS DATE) AS prd_start_dt,             -- Convert start date to DATE type
				CAST(
					LEAD(prd_start_dt) OVER (                           -- Find next start date for end-date calculation
						PARTITION BY prd_key 
						ORDER BY prd_start_dt
					) - 1 AS DATE
				) AS prd_end_dt 
			FROM bronze.crm_prd_info;

			SET @end_time = GETDATE();
			PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------'; 

			/* =====================================================================================
			   CRM Sales Details
			   ===================================================================================== */
			SET @Start_time = GETDATE();

			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;

			PRINT '>> Inserting Data Into: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Handle invalid/missing dates
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL                          -- If sales is null or invalid
					  OR sls_sales <= 0 
					  OR sls_sales <> sls_quantity * ABS(sls_price) -- Or mismatched with quantity × price
					THEN sls_quantity * ABS(sls_price)              -- Recalculate sales
					ELSE sls_sales
				END AS sls_sales,
				sls_quantity,
				CASE 
					WHEN sls_price IS NULL OR sls_price <= 0        -- If missing or invalid price
					THEN sls_sales / NULLIF(sls_quantity,0)         -- Recalculate from sales ÷ quantity
					ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details;

			SET @end_time = GETDATE();
			PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------'; 

			PRINT '------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------';

			/* =====================================================================================
			   ERP Customer Data
			   ===================================================================================== */
			SET @Start_time = GETDATE();

			PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;

			PRINT '>> Inserting Data Into: silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12 (
				CID,
				BDATE,
				GEN
			)
			SELECT 
				CASE 
					WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID)) -- Remove NAS prefix if present
					ELSE CID
				END AS CID,
				CASE 
					WHEN BDATE > GETDATE() THEN NULL  -- Discard invalid future birthdates
					ELSE BDATE 
				END AS BDATE,
				CASE
					WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'   -- Normalize gender values
					WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
					ELSE 'N/A'
				END AS GEN
			FROM bronze.erp_cust_az12;

			SET @end_time = GETDATE();
			PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------'; 

			/* =====================================================================================
			   ERP Location Data
			   ===================================================================================== */
			SET @Start_time = GETDATE();

			PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;

			PRINT '>> Inserting Data Into: silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101 (
				CID,
				CNTRY
			)
			SELECT 
				REPLACE(CID, '-',''),                   -- Remove dashes from Customer ID
				CASE 
					WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'       -- Normalize country names
					WHEN UPPER(TRIM(CNTRY)) IN ('US','USA') THEN 'United States'
					WHEN UPPER(TRIM(CNTRY)) = '' OR CNTRY IS NULL THEN 'N/A'
					ELSE TRIM(CNTRY)
				END AS CNTRY
			FROM bronze.erp_loc_a101;

			SET @end_time = GETDATE();
			PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------'; 

			/* =====================================================================================
			   ERP Product Categories
			   ===================================================================================== */
			SET @Start_time = GETDATE();

			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;

			PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2 (
				ID,
				CAT,
				SUBCAT,
				MAINTENANCE
			)
			SELECT 
				ID,
				CAT,
				SUBCAT,
				MAINTENANCE
			FROM bronze.erp_px_cat_g1v2;

			SET @end_time = GETDATE();
			PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
			PRINT '---------------------------------------'; 

			SET @batch_end_time = GETDATE();

			PRINT '======================================================';
			PRINT '   - Total Load Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' Seconds';
			PRINT '======================================================'; 

		END TRY

		BEGIN CATCH
			PRINT '================================================================';
			PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
			PRINT 'Error Message ' + ERROR_MESSAGE();
			PRINT 'Error Number  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error State   ' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '================================================================';
		END CATCH 
	END
