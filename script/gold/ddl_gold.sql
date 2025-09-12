/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema).

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

USE DataWarehouse;
GO

/*=============================================================================
  Dimension View: gold.dim_customers
  Purpose:
    - Combines CRM and ERP sources for customer data
    - Generates a surrogate key
    - Applies logic for gender standardization
=============================================================================*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,  -- Surrogate key
    ci.cst_id            AS customer_id,        -- Business key
    ci.cst_key           AS customer_number,    -- External identifier
    ci.cst_firstname     AS first_name,
    ci.cst_lastname      AS last_name,
    la.CNTRY             AS country,            -- Location from ERP
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr <> 'N/A' 
            THEN ci.cst_gndr                  -- Use CRM gender if available
        ELSE COALESCE(GEN, 'N/A')             -- Otherwise fallback to ERP
    END AS gender,
    ca.BDATE             AS birthdate,
    ci.cst_create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.CID;
GO


/*=============================================================================
  Dimension View: gold.dim_products
  Purpose:
    - Builds product dimension with surrogate key
    - Joins category metadata
    - Filters out inactive (historical) products
=============================================================================*/
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;  -- Keep only active products
GO


/*=============================================================================
  Fact View: gold.fact_sales
  Purpose:
    - Creates sales fact view for reporting
    - Joins product and customer dimensions
    - Provides clean business metrics for analysis
=============================================================================*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num   AS order_number,
    pr.product_key   AS product_key,    -- FK to gold.dim_products
    cu.customer_key  AS customer_key,   -- FK to gold.dim_customers
    sd.sls_order_dt  AS order_date,
    sd.sls_ship_dt   AS shipping_date,
    sd.sls_due_dt    AS due_date,
    sd.sls_sales     AS sales_amount,
    sd.sls_quantity  AS quantity,
    sd.sls_price     AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
