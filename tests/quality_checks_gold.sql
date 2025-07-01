/*
===========================================================================
Quality checks for the bronze layer
===========================================================================
*/

	select * from gold.dim_customers;

	select distinct gender from gold.dim_customers;


--checking if there is row wise duplicate 
select 
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_start_dt,
	prd_line,
	cat,
	maintenance,
	subcat, 
	count(*) as unique_key_checking
from(
	select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_start_dt,
	pn.prd_line,
	pc.cat,
	pc.maintenance,
	pc.subcat,
	case when pn.prd_end_dt is null then '1999-01-01'
		else pn.prd_start_dt
	end prd_end_dt
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on	pn.cat_id = pc.id)b
group by 
	prd_key,
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_start_dt,
	prd_line,
	cat,
	maintenance,
	subcat
having count(*) > 1;

select * from gold.dim_products;


select * from gold.fact_sales
select * from gold.dim_products
select * from gold.dim_customers

--foreign key intgrity
select *
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where c.customer_key is null

select *
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_unique_key
where p.product_key is null
