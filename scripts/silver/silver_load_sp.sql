CREATE OR REPLACE PROCEDURE silver.silver_layer_load()
LANGUAGE plpgsql
AS $$
    DECLARE
        batch_id TEXT := TO_CHAR ( NOW(), 'YYYYMMDD_HH24MISS');
        process_start TIMESTAMP := NOW();
        process_end TIMESTAMP;
        process_duration_secs NUMERIC;
        load_start TIMESTAMP;
        load_end TIMESTAMP;
        records_loaded INTEGER;
        status TEXT;
        error_msg TEXT;
    BEGIN
        RAISE NOTICE '====================================================================';
        RAISE NOTICE '==== Starting Silver Layer Load Process (Bronze -> Silver) ====';
        RAISE NOTICE 'Process start time: %', process_start;
        RAISE NOTICE 'Batch ID: %', batch_id;
        RAISE NOTICE '====================================================================';

        -- =====================1 CRM Customer Info =====================
        status := 'SUCCESS'; error_msg := NULL; records_loaded := 0;
        BEGIN
            ----------* Truncate
            RAISE NOTICE 'Truncating table: [silver.crm_cust_info]';
            TRUNCATE TABLE silver.crm_cust_info;
            RAISE NOTICE '[silver.crm_cust_info] truncated';

            ----------* Load
            load_start := NOW();
            RAISE NOTICE 'Transforming and loading table: [silver.crm_cust_info]';
            		--Handling duplicates and NULLs in pkey cst_id
			WITH cte_1_1 AS (
                SELECT *,
                    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
            ),
            		-- Trimming string fields and filtering the most recent entry for a cst_id
			cte_1_2 AS (
                SELECT 
                    cst_id, TRIM(cst_key) AS cst_key, TRIM(cst_firstname) AS cst_firstname,
                    TRIM(cst_lastname) AS cst_lastname, TRIM(cst_marital_status) AS cst_marital_status,
                    TRIM(cst_gndr) AS cst_gndr, cst_create_date, flag_last
                FROM cte_1_1
                WHERE flag_last = 1
            ),
            		-- Standardising and expanding abbreviations
			cte_1_3 AS (
                SELECT cst_id, cst_key, cst_firstname, cst_lastname,
                    CASE UPPER(cst_marital_status)
                        WHEN 'M' THEN 'Married'
                        WHEN 'S' THEN 'Single'
                        ELSE cst_marital_status
                    END AS cst_marital_status,
                    CASE UPPER(cst_gndr)
                        WHEN 'M' THEN 'Male'
                        WHEN 'F' THEN 'Female'
                        ELSE cst_gndr
                    END AS cst_gndr,
                    cst_create_date
                FROM cte_1_2
            )
            INSERT INTO silver.crm_cust_info(
                cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
            )
            SELECT * FROM cte_1_3;
			
            SELECT COUNT(*) INTO records_loaded FROM silver.crm_cust_info;
            load_end := NOW();
            status := 'SUCCESS'; error_msg := NULL;
            RAISE NOTICE '[silver.crm_cust_info] loaded. Number of records: %', records_loaded;

            ----------* Log
            INSERT INTO silver.silver_load_log
            VALUES ('silver.crm_cust_info', batch_id, load_start, load_end,
                    EXTRACT(EPOCH FROM (load_end - load_start)), status, error_msg, records_loaded);
					
			----------* Error Handling
        EXCEPTION WHEN OTHERS THEN
            load_start := NOW();  load_end := load_start;
            status := 'FAIL';  error_msg := SQLERRM;
            RAISE WARNING '[silver.crm_cust_info] Failure: %', error_msg;
            INSERT INTO silver.silver_load_log
            VALUES ('silver.crm_cust_info', batch_id, load_start, load_end,
                    0, status, error_msg, 0);
        END;

        -- =====================2 CRM Product Info =====================
        status := 'SUCCESS'; error_msg := NULL; records_loaded := 0;
        BEGIN
            ----------* Truncate
            RAISE NOTICE 'Truncating table: [silver.crm_prd_info]';
            TRUNCATE TABLE silver.crm_prd_info;
            RAISE NOTICE '[silver.crm_prd_info] truncated';

            ----------* Load
            load_start := NOW();
            RAISE NOTICE 'Transforming and loading table: [silver.crm_prd_info]';
            WITH cte_2_1 AS (
                SELECT
                    prd_id,
                    REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,
                    SUBSTRING(prd_key FROM 7) AS prd_key,
                    TRIM(prd_nm) AS prd_nm,
                    COALESCE(prd_cost, 0) AS prd_cost,
                    CASE UPPER(TRIM(prd_line))
                        WHEN 'M' THEN 'Mountain'
                        WHEN 'R' THEN 'Road'
                        WHEN 'S' THEN 'Other Sales'
                        WHEN 'T' THEN 'Touring'
                        ELSE prd_line
                    END AS prd_line,
                    prd_start_dt::DATE AS prd_start_dt,
                    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt
                FROM bronze.crm_prd_info
            )
            INSERT INTO silver.crm_prd_info (
                prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
            )
            SELECT * FROM cte_2_1;
			
            SELECT COUNT(*) INTO records_loaded FROM silver.crm_prd_info;
            load_end := NOW();
            status := 'SUCCESS'; error_msg := NULL;
            RAISE NOTICE '[silver.crm_prd_info] loaded. Number of records: %', records_loaded;

            ----------* Log
            INSERT INTO silver.silver_load_log
            VALUES ('silver.crm_prd_info', batch_id, load_start, load_end,
                    EXTRACT(EPOCH FROM (load_end - load_start)), status, error_msg, records_loaded);

    		----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
            load_start := NOW();  load_end := load_start;
            status := 'FAIL';  error_msg := SQLERRM;
            RAISE WARNING '[silver.crm_prd_info] Failure: %', error_msg;
            INSERT INTO silver.silver_load_log
            VALUES ('silver.crm_prd_info', batch_id, load_start, load_end,
                    0, status, error_msg, 0);
        END;

        -- =====================3 CRM Sales Details =====================
        status := 'SUCCESS'; error_msg := NULL; records_loaded := 0;
        BEGIN
            ----------* Truncate
            RAISE NOTICE 'Truncating table: [silver.crm_sales_details]';
            TRUNCATE TABLE silver.crm_sales_details;
            RAISE NOTICE '[silver.crm_sales_details] truncated';

            ----------* Load
            load_start := NOW();
            RAISE NOTICE 'Transforming and loading table: [silver.crm_sales_details]';
            		--Correcting the format of date fields
			WITH cte_3_1 AS (
                SELECT 
                    sls_ord_num,
                    sls_prd_key,
                    sls_cust_id,
                    CASE 
                        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
                    END AS sls_order_dt,
                    CASE 
                        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
                    END AS sls_ship_dt,
                    CASE 
                        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
                    END AS sls_due_dt,
                    sls_quantity,
                    sls_price,
                    sls_sales
                FROM bronze.crm_sales_details
            ),
					--Correcting sales and price fields
            cte_3_2 AS (
                SELECT 
                    sls_ord_num,
                    sls_prd_key,
                    sls_cust_id,
                    sls_order_dt,
                    sls_ship_dt,
                    sls_due_dt,
                    CASE 
                        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                            THEN sls_quantity * ABS(sls_price)
                        ELSE sls_sales
                    END AS sls_sales,
                    sls_quantity,
                    CASE 
                        WHEN sls_price IS NULL OR sls_price <= 0 
                            THEN sls_sales / NULLIF(sls_quantity, 0)
                        ELSE sls_price
                    END AS sls_price
                FROM cte_3_1
            )
            INSERT INTO silver.crm_sales_details (
                sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
            )
            SELECT * FROM cte_3_2;
			
            SELECT COUNT(*) INTO records_loaded FROM silver.crm_sales_details;
            load_end := NOW();
            status := 'SUCCESS'; error_msg := NULL;
            RAISE NOTICE '[silver.crm_sales_details] loaded. Number of records: %', records_loaded;

            ----------* Log
            INSERT INTO silver.silver_load_log
            VALUES ('silver.crm_sales_details', batch_id, load_start, load_end,
                    EXTRACT(EPOCH FROM (load_end - load_start)), status, error_msg, records_loaded);

			----------* Error Handling
        EXCEPTION WHEN OTHERS THEN
            load_start := NOW();  load_end := load_start;
            status := 'FAIL';  error_msg := SQLERRM;
            RAISE WARNING '[silver.crm_sales_details] Failure: %', error_msg;
            INSERT INTO silver.silver_load_log
            VALUES ('silver.crm_sales_details', batch_id, load_start, load_end,
                    0, status, error_msg, 0);
        END;

        -- ===================== ERP Customer AZ12 =====================
        status := 'SUCCESS'; error_msg := NULL; records_loaded := 0;
        BEGIN
            ----------* Truncate
            RAISE NOTICE 'Truncating table: [silver.erp_cust_az12]';
            TRUNCATE TABLE silver.erp_cust_az12;
            RAISE NOTICE '[silver.erp_cust_az12] truncated';

            ----------* Load
            load_start := NOW();
            RAISE NOTICE 'Transforming and loading table: [silver.erp_cust_az12]';
            WITH cte_4_1 AS (
                SELECT
                    REPLACE(UPPER(cid), 'NAS', '') AS cid,
                    CASE
                        WHEN bdate > CURRENT_DATE THEN NULL
                        ELSE bdate
                    END AS bdate,
                    CASE
                        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                        ELSE 'n/a'
                    END AS gen
                FROM bronze.erp_cust_az12
            )
            INSERT INTO silver.erp_cust_az12 (
                cid, bdate, gen
            )
            SELECT * FROM cte_4_1;
			
            SELECT COUNT(*) INTO records_loaded FROM silver.erp_cust_az12;
            load_end := NOW();
            status := 'SUCCESS'; error_msg := NULL;
            RAISE NOTICE '[silver.erp_cust_az12] loaded. Number of records: %', records_loaded;

            ----------* Log
            INSERT INTO silver.silver_load_log
            VALUES ('silver.erp_cust_az12', batch_id, load_start, load_end,
                    EXTRACT(EPOCH FROM (load_end - load_start)), status, error_msg, records_loaded);

			----------* Error Handling
        EXCEPTION WHEN OTHERS THEN
            load_start := NOW();  load_end := load_start;
            status := 'FAIL';  error_msg := SQLERRM;
            RAISE WARNING '[silver.erp_cust_az12] Failure: %', error_msg;
            INSERT INTO silver.silver_load_log
            VALUES ('silver.erp_cust_az12', batch_id, load_start, load_end,
                    0, status, error_msg, 0);
        END;

        -- ===================== ERP Location A101 =====================
        status := 'SUCCESS'; error_msg := NULL; records_loaded := 0;
        BEGIN
            ----------* Truncate
            RAISE NOTICE 'Truncating table: [silver.erp_loc_a101]';
            TRUNCATE TABLE silver.erp_loc_a101;
            RAISE NOTICE '[silver.erp_loc_a101] truncated';

            ----------* Load
            load_start := NOW();
            RAISE NOTICE 'Transforming and loading table: [silver.erp_loc_a101]';
            WITH cte_5_1 AS (
                SELECT
                    REPLACE(cid, '-', '') AS cid,
                    CASE
                        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                        ELSE TRIM(cntry)
                    END AS cntry
                FROM bronze.erp_loc_a101
            )
            INSERT INTO silver.erp_loc_a101 (
                cid, cntry
            )
            SELECT * FROM cte_5_1;
			
            SELECT COUNT(*) INTO records_loaded FROM silver.erp_loc_a101;
            load_end := NOW();
            status := 'SUCCESS'; error_msg := NULL;
            RAISE NOTICE '[silver.erp_loc_a101] loaded. Number of records: %', records_loaded;

            ----------* Log
            INSERT INTO silver.silver_load_log
            VALUES ('silver.erp_loc_a101', batch_id, load_start, load_end,
                    EXTRACT(EPOCH FROM (load_end - load_start)), status, error_msg, records_loaded);

			----------* Error Handling
        EXCEPTION WHEN OTHERS THEN
            load_start := NOW();  load_end := load_start;
            status := 'FAIL';  error_msg := SQLERRM;
            RAISE WARNING '[silver.erp_loc_a101] Failure: %', error_msg;
            INSERT INTO silver.silver_load_log
            VALUES ('silver.erp_loc_a101', batch_id, load_start, load_end,
                    0, status, error_msg, 0);
        END;

        -- ===================== ERP Product Category G1V2 =====================
        status := 'SUCCESS'; error_msg := NULL; records_loaded := 0;
        BEGIN
            ----------* Truncate
            RAISE NOTICE 'Truncating table: [silver.erp_px_cat_g1v2]';
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            RAISE NOTICE '[silver.erp_px_cat_g1v2] truncated';

            ----------* Load
            load_start := NOW();
            RAISE NOTICE 'Loading table: [silver.erp_px_cat_g1v2]';
            INSERT INTO silver.erp_px_cat_g1v2 (
                id, cat, subcat, maintenance
            )
            SELECT * FROM bronze.erp_px_cat_g1v2;
			
            SELECT COUNT(*) INTO records_loaded FROM silver.erp_px_cat_g1v2;
            load_end := NOW();
            status := 'SUCCESS'; error_msg := NULL;
            RAISE NOTICE '[silver.erp_px_cat_g1v2] loaded. Number of records: %', records_loaded;

            ----------* Log
            INSERT INTO silver.silver_load_log
            VALUES ('silver.erp_px_cat_g1v2', batch_id, load_start, load_end,
                    EXTRACT(EPOCH FROM (load_end - load_start)), status, error_msg, records_loaded);

			----------* Error Handling
        EXCEPTION WHEN OTHERS THEN
            load_start := NOW();  load_end := load_start;
            status := 'FAIL';  error_msg := SQLERRM;
            RAISE WARNING '[silver.erp_px_cat_g1v2] Failure: %', error_msg;
            INSERT INTO silver.silver_load_log
            VALUES ('silver.erp_px_cat_g1v2', batch_id, load_start, load_end,
                    0, status, error_msg, 0);
        END;

        process_end := NOW();
        process_duration_secs := EXTRACT(EPOCH FROM (process_end - process_start));

        RAISE NOTICE '====================================================================';
        RAISE NOTICE '==== Silver Layer Load Process Completed ====';
        RAISE NOTICE 'Process end time: %', process_end;
        RAISE NOTICE 'Total process time: %', process_duration_secs;
        RAISE NOTICE '====================================================================';

    EXCEPTION WHEN OTHERS THEN
        process_end := NOW();
        process_duration_secs := EXTRACT(EPOCH FROM (process_end - process_start));
        RAISE WARNING 'Silver Layer Load Aborted due to Error: %', SQLERRM;
        RAISE WARNING 'Process end time: %', process_end;
        RAISE WARNING 'Total process time: %', process_duration_secs;
END $$;
