/*
		DDL scritpt : Creating Gold layer Views
		---------------------------------------
	Script purpose:
		- This script creates views for the Gold layer in the data warehouse
		- The views represent the business objects Customers, Products and Sales
		  modelled in Star Schema

	Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

	Usage:
    	- These views can be queried directly for analytics and reporting.
*/

/*========== Create Dimension : gold.dim_customers ==========*/

CREATE OR REPLACE VIEW gold.dim_customers AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY ci.cst_id ) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,	
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		COALESCE(ci.cst_gndr,ca.gen) AS gender, -- crm_cust_info is the master table
		ca.bdate AS birth_date,
		ci.cst_create_date AS create_date
	FROM	silver.crm_cust_info ci
	LEFT JOIN	silver.erp_cust_az12 ca
	ON	ci.cst_key = ca.cid
	LEFT JOIN	silver.erp_loc_a101 la
	ON	ci.cst_key = la.cid

/*========== Create Dimension : gold.dim_products ==========*/

CREATE OR REPLACE VIEW gold.dim_products AS
	SELECT
		ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key  ) AS product_key,
		pi.prd_id AS product_id,	
		pi.prd_key AS product_number,
		pi.prd_nm AS product_name,
		pi.cat_id AS category_id,
		px.cat AS category,
		px.subcat AS subcategory,
		px.maintenance,
		pi.prd_cost AS cost,
		pi.prd_line AS product_line,
		pi.prd_start_dt AS start_date
	FROM	silver.crm_prd_info pi
	LEFT JOIN  silver.erp_px_cat_g1v2 px
	ON pi.cat_id = px.id
	WHERE 	pi.prd_end_dt IS NULL  -- removes historical updates of products

/*========== Create Fact table : gold.fact_sales ==========*/

CREATE OR REPLACE VIEW gold.fact_sales AS
	SELECT
		cs.sls_ord_num AS order_number,
		pr.product_key,
		cu.customer_key,
		cs.sls_order_dt AS order_date,
		cs.sls_ship_dt AS shipping_date,
		cs.sls_due_dt AS due_date,
		cs.sls_sales AS sales_amount,
		cs.sls_quantity AS quantity,
		cs.sls_price AS price
	FROM	silver.crm_sales_details cs
	LEFT JOIN	gold.dim_customers cu
	ON cs.sls_cust_id = cu.customer_id
	LEFT JOIN	gold.dim_products pr
	ON cs.sls_prd_key = pr.product_number
