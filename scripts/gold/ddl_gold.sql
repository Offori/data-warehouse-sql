/*
================================================================================================
This script create views for the gold layers that can be used for analytics and reporting
================================================================================================
*/


create view gold.dim_customers as
select
	row_number() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'N/A' then ci.cst_gndr --because CRM is the master table whre u'll find cst_gendr
		else coalesce(ca.gen, 'N/A') 
	end gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on	ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

create view	gold.dim_products as
select
	row_number() over (order by prd_key) as product_unique_key,
	pn.prd_key as product_key,
	pn.prd_id as product_id,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pn.prd_nm as product_name,
	pn.prd_cost as product_cost,
	pc.maintenance,
	pn.prd_line as product_line,
	pn.prd_start_dt as product_start_date,
	case when pn.prd_end_dt is null then '1999-01-01'
		else pn.prd_start_dt
	end prd_end_dt
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on	pn.cat_id = pc.id

create view gold.fact_sales as 
select 
	sd.sls_ord_num as order_number,
	pr.product_unique_key as product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipment_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_key
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
