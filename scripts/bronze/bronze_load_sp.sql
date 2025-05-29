CREATE OR REPLACE PROCEDURE bronze.bronze_layer_load()
LANGUAGE plpgsql
AS $$
	DECLARE
		batch_id TEXT := TO_CHAR ( NOW(), 'YYYYMMDD_HH24MISS'); -- Converting the timestamp to human readable string for logging
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
		RAISE NOTICE '==== Starting Bronze Layer Load Process (Source -> Bronze) ====';
		RAISE NOTICE 'Process start time: %', process_start;
		RAISE NOTICE 'Batch ID: %', batch_id;
		RAISE NOTICE '====================================================================';

		-- ===================== CRM Customer Info =====================
		status:= 'SUCCESS'; error_msg:= NULL; records_loaded:= 0;		
			
		BEGIN
				----------* Truncate 
			RAISE NOTICE 'Truncating table: [bronze.crm_cust_info]';
			TRUNCATE TABLE bronze.crm_cust_info;
			RAISE NOTICE '[bronze.crm_cust_info] truncated';

				----------* Load 
			load_start:= NOW();
			RAISE NOTICE 'Loading table: [bronze.crm_cust_info]';
			COPY bronze.crm_cust_info FROM 'C:/projects data/1/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true);
			SELECT COUNT(*) INTO records_loaded  FROM bronze.crm_cust_info;
			load_end:= NOW();
			status := 'SUCCESS'; error_msg := NULL;
	    RAISE NOTICE '[bronze.crm_cust_info] loaded. Number of records: %', records_loaded;

				----------* Log
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.crm_cust_info', batch_id, load_start, load_end,
					    EXTRACT(EPOCH FROM (load_end - load_start)) , status, error_msg, records_loaded);

				----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
			load_start := NOW();  load_end := load_start;
			status := 'FAIL';  error_msg := SQLERRM;
			RAISE WARNING '[bronze.crm_cust_info] Failure: %', error_msg;
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.crm_cust_info', batch_id, load_start, load_end,
					    0, status, error_msg, 0);
		END;
				

		-- ===================== CRM Product Info =====================
		status:= 'SUCCESS'; error_msg:= NULL; records_loaded:= 0;
		
		BEGIN
				----------* Truncate 
			RAISE NOTICE 'Truncating table: [bronze.crm_prd_info]';
			TRUNCATE TABLE bronze.crm_prd_info;
			RAISE NOTICE '[bronze.crm_prd_info] truncated';

				----------* Load 
			load_start:= NOW();
			RAISE NOTICE 'Loading table: [bronze.crm_prd_info]';
			COPY bronze.crm_prd_info FROM 'C:/projects data/1/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true);
			SELECT COUNT(*) INTO records_loaded  FROM bronze.crm_prd_info;
			load_end:= NOW();
			status := 'SUCCESS'; error_msg := NULL;
	    RAISE NOTICE '[bronze.crm_prd_info] loaded. Number of records: %', records_loaded;

				----------* Log
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.crm_prd_info', batch_id, load_start, load_end,
					    EXTRACT(EPOCH FROM (load_end - load_start)) , status, error_msg, records_loaded);

				----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
			load_start := NOW();  load_end := load_start;
			status := 'FAIL';  error_msg := SQLERRM;
			RAISE WARNING '[bronze.crm_prd_info] Failure: %', error_msg;
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.crm_prd_info', batch_id, load_start, load_end,
					    0, status, error_msg, 0);
		END;


		-- ===================== CRM Sales Details =====================
		status:= 'SUCCESS'; error_msg:= NULL; records_loaded:= 0;
		
		BEGIN
				----------* Truncate 
			RAISE NOTICE 'Truncating table: [bronze.crm_sales_details]';
			TRUNCATE TABLE bronze.crm_sales_details;
			RAISE NOTICE '[bronze.crm_sales_details] truncated';

				----------* Load 
			load_start:= NOW();
			RAISE NOTICE 'Loading table: [bronze.crm_sales_details]';
			COPY bronze.crm_sales_details FROM 'C:/projects data/1/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);
			SELECT COUNT(*) INTO records_loaded  FROM bronze.crm_sales_details;
			load_end:= NOW();
			status := 'SUCCESS'; error_msg := NULL;
	    RAISE NOTICE '[bronze.crm_sales_details] loaded. Number of records: %', records_loaded;

				----------* Log
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.crm_sales_details', batch_id, load_start, load_end,
					    EXTRACT(EPOCH FROM (load_end - load_start)) , status, error_msg, records_loaded);

				----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
			load_start := NOW();  load_end := load_start;
			status := 'FAIL';  error_msg := SQLERRM;
			RAISE WARNING '[bronze.crm_sales_details] Failure: %', error_msg;
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.crm_sales_details', batch_id, load_start, load_end,
					    0, status, error_msg, 0);
		END;

		
		-- ===================== ERP Customer AZ12 =====================
		status:= 'SUCCESS'; error_msg:= NULL; records_loaded:= 0;
		
		BEGIN
				----------* Truncate 
			RAISE NOTICE 'Truncating table: [bronze.erp_cust_az12]';
			TRUNCATE TABLE bronze.erp_cust_az12;
			RAISE NOTICE '[bronze.erp_cust_az12] truncated';

				----------* Load 
			load_start:= NOW();
			RAISE NOTICE 'Loading table: [bronze.erp_cust_az12]';
			COPY bronze.erp_cust_az12 FROM 'C:/projects data/1/source_erp/CUST_AZ12.csv' WITH (FORMAT csv, HEADER true);
			SELECT COUNT(*) INTO records_loaded  FROM bronze.erp_cust_az12;
			load_end:= NOW();
			status := 'SUCCESS'; error_msg := NULL;
	    RAISE NOTICE '[bronze.erp_cust_az12] loaded. Number of records: %', records_loaded;

				----------* Log
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.erp_cust_az12', batch_id, load_start, load_end,
					    EXTRACT(EPOCH FROM (load_end - load_start)) , status, error_msg, records_loaded);

				----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
			load_start := NOW();  load_end := load_start;
			status := 'FAIL';  error_msg := SQLERRM;
			RAISE WARNING '[bronze.erp_cust_az12] Failure: %', error_msg;
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.erp_cust_az12', batch_id, load_start, load_end,
					    0, status, error_msg, 0);
		END;

						
		-- ===================== ERP Location A101 =====================
		status:= 'SUCCESS'; error_msg:= NULL; records_loaded:= 0;
		
		BEGIN
				----------* Truncate 
			RAISE NOTICE 'Truncating table: [bronze.erp_loc_a101]';
			TRUNCATE TABLE bronze.erp_loc_a101;
			RAISE NOTICE '[bronze.erp_loc_a101] truncated';

				----------* Load 
			load_start:= NOW();
			RAISE NOTICE 'Loading table: [bronze.erp_loc_a101]';
			COPY bronze.erp_loc_a101 FROM 'C:/projects data/1/source_erp/LOC_A101.csv' WITH (FORMAT csv, HEADER true);
			SELECT COUNT(*) INTO records_loaded  FROM bronze.erp_loc_a101;
			load_end:= NOW();
			status := 'SUCCESS'; error_msg := NULL;
	    RAISE NOTICE '[bronze.erp_loc_a101] loaded. Number of records: %', records_loaded;

				----------* Log
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.erp_loc_a101', batch_id, load_start, load_end,
					    EXTRACT(EPOCH FROM (load_end - load_start)) , status, error_msg, records_loaded);

				----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
			load_start := NOW();  load_end := load_start;
			status := 'FAIL';  error_msg := SQLERRM;
			RAISE WARNING '[bronze.erp_loc_a101] Failure: %', error_msg;
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.erp_loc_a101', batch_id, load_start, load_end,
					    0, status, error_msg, 0);
		END;

								
		-- ===================== ERP Product Category G1V2 =====================
		status:= 'SUCCESS'; error_msg:= NULL; records_loaded:= 0;
		
		BEGIN
				----------* Truncate 
			RAISE NOTICE 'Truncating table: [bronze.erp_px_cat_g1v2]';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			RAISE NOTICE '[bronze.erp_px_cat_g1v2] truncated';

				----------* Load 
			load_start:= NOW();
			RAISE NOTICE 'Loading table: [bronze.erp_px_cat_g1v2]';
			COPY bronze.erp_px_cat_g1v2 FROM 'C:/projects data/1/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true);
			SELECT COUNT(*) INTO records_loaded  FROM bronze.erp_px_cat_g1v2;
			load_end:= NOW();
			status := 'SUCCESS'; error_msg := NULL;
	    RAISE NOTICE '[bronze.erp_px_cat_g1v2] loaded. Number of records: %', records_loaded;

				----------* Log
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.erp_px_cat_g1v2', batch_id, load_start, load_end,
					    EXTRACT(EPOCH FROM (load_end - load_start)) , status, error_msg, records_loaded);

				----------* Error Handling
		EXCEPTION WHEN OTHERS THEN
			load_start := NOW();  load_end := load_start;
			status := 'FAIL';  error_msg := SQLERRM;
			RAISE WARNING '[bronze.erp_px_cat_g1v2] Failure: %', error_msg;
			INSERT INTO bronze.bronze_load_log 
			VALUES ('bronze.erp_px_cat_g1v2', batch_id, load_start, load_end,
					    0, status, error_msg, 0);
		END;
		

		process_end:= NOW();
		process_duration_secs:= EXTRACT(EPOCH FROM (process_end - process_start));

		RAISE NOTICE '====================================================================';
		RAISE NOTICE '==== Bronze Layer Load Process Completed ====';
		RAISE NOTICE 'Process end time: %', process_end;
		RAISE NOTICE 'Total process time: %', process_duration_secs;
		RAISE NOTICE '====================================================================';

	EXCEPTION WHEN OTHERS THEN
		process_end:= NOW();
		process_duration_secs:= EXTRACT(EPOCH FROM (process_end - process_start));
		RAISE WARNING 'Bronze Layer Load Aborted due to Error: %', SQLERRM;
		RAISE WARNING 'Process end time: %', process_end;
		RAISE WARNING 'Total process time: %', process_duration_secs;

END $$;
