-- =====================================================================
-- DDL Script: Create Silver Tables in PostgreSQL
-- =====================================================================
-- Script Purpose:
--     Creates tables in the 'silver' schema, dropping them first if they exist.
--     Use this script to re-define the DDL structure of 'silver' tables.
-- =====================================================================

-- Drop and create silver.crm_cust_info
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(20),
    cst_gndr            VARCHAR(20),
    cst_create_date     DATE,
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);

-- Drop and create silver.crm_prd_info

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    cat_id		 VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);

-- Drop and create silver.crm_sales_details

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);

-- Drop and create silver.erp_loc_a101

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);

-- Drop and create silver.erp_cust_az12

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);

-- Drop and create silver.erp_px_cat_g1v2

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);

-- Drop and create silver.silver_load_log (Log table)

DROP TABLE IF EXISTS silver.silver_load_log;
CREATE TABLE silver.silver_load_log (
    table_name TEXT,
    batch_id TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_secs NUMERIC,
    status TEXT,
    error_message TEXT,
    records_loaded INTEGER,
	dwh_table_create_date TIMESTAMP DEFAULT NOW()
);
