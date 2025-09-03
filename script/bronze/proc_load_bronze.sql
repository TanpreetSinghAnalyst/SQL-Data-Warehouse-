USE DataWarehouse;
GO
--==============================================
Exec bronze.load_bronze
--==============================================


-- ================================================
-- Author:      Tanpreet Singh
-- Create date: 2025-08-30
-- Description: Loads raw CSV data into bronze layer
--              by truncating target tables and 
--              inserting fresh data using BULK INSERT
-- ================================================



/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS 
BEGIN
PRINT '==============================================='
PRINT 'Loading Bronze Layer'
PRINT '==============================================='
    -- ========================
    -- 1. Load CRM Customer Info
    -- ========================
	PRINT '---------------------------------'
	PRINT 'Loading CRM Tables'
	PRINT '---------------------------------'

	PRINT ' >> Truncating Table: bronze.crm_cust_info'
    TRUNCATE TABLE bronze.crm_cust_info;

	PRINT ' >> Inserting Data INTO : bronze.crm_cust_info'
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\SQL\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,              -- Skip header row
        FIELDTERMINATOR = ',',     -- CSV delimiter
        TABLOCK                    -- Improve bulk load performance
    );


    -- ========================
    -- 2. Load CRM Product Info
    -- ========================
	PRINT ' >> Truncating Table: bronze.crm_prd_info'
    TRUNCATE TABLE bronze.crm_prd_info;

	PRINT ' >> Inserting Data INTO : bronze.crm_prd_info'
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\SQL\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );


    -- =========================
    -- 3. Load CRM Sales Details
    -- =========================

	PRINT ' >> Truncating Table: bronze.crm_sales_details'
    TRUNCATE TABLE bronze.crm_sales_details;

	PRINT ' >> Inserting Data INTO : bronze.crm_sales_details'
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\SQL\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );


    -- =============================
    -- 4. Load ERP Customer Master (AZ12)
    -- =============================
PRINT '==============================================='
PRINT 'Loading Bronze Layer'
PRINT '==============================================='
    
	PRINT ' >> Truncating Table: bronze.erp_cust_az12'
	TRUNCATE TABLE bronze.erp_cust_az12;
	
	PRINT ' >> Inserting Data INTO : bronze.erp_cust_az12'
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\SQL\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );


    -- ============================
    -- 5. Load ERP Location Master
    -- ============================
    PRINT ' >> Truncating Table: bronze.erp_loc_a101'
	TRUNCATE TABLE bronze.erp_loc_a101;
	
	PRINT ' >> Inserting Data INTO : bronze.erp_loc_a101'
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\SQL\source_erp\loc_a101.csv'

    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );


    -- ===============================
    -- 6. Load ERP Product Category (PX_CAT_G1V2)
    -- ===============================
    
	PRINT ' >> Truncating Table: bronze.erp_px_cat_g1v2'
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT ' >> Inserting Data INTO : bronze.erp_px_cat_g1v2'
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\SQL\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

END;
GO

