/*
====================================================================================================
Store procedure: Load silver layer from bronze (bronze -> silver)
----------------------------------------------------------------------------------------------------
This store procedure performs the ETL(Extract, Transform, Load) to populate the silver 
schema tables from bronze layer tables

Actions: 
  - truncate silver tables
  - insert trasformed table from bronze to silver
paameters:
  - No parameters
  -No return values
Usage:
  -exec silver.load_silver;
====================================================================================================
*/

print '*********************************************************************************';
print '>>> Store procedure from bronze to silver layer';
print '*********************************************************************************';

create or alter procedure silver.load_silver as 
begin
	begin try
		print '*********************************************************************************';
		print '>>> Silver Layer loading';
		print '*********************************************************************************';

		print '*********************************************************************************';
		print '>>> Inserting data from bronze into silver.crm_cust_info';
		print '*********************************************************************************';
		print '*********************************************************************************';
		print '>>> trucate and insert';
		print '*********************************************************************************';

		declare @startime datetime, @endtime datetime;
		set @startime = getdate();
		truncate table silver.crm_cust_info;
		set @startime = getdate();
		insert into silver.crm_cust_info
		(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		select
			cst_id,
			cst_key,
			trim(cst_firstname),
			trim(cst_lastname),
			case	
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'N/A'
			end cst_marital_status,
			case	
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				else 'N/A'
			end cst_gndr,
			cst_create_date
		from 
			(
			SELECT *, 
				   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) AS ranked
		where rn = 1;
		set @endtime = GETDATE();

		print 'Load duration: ' + cast(datediff(second, @startime, @endtime) as nvarchar) + 'second'
		print '>>> ..........'

		print '*********************************************************************************';
		print '>>> Inserting data from bronze into silver.crm_prd_info';
		print '*********************************************************************************';
		print '*********************************************************************************';
		print '>>> trucate and insert';
		print '*********************************************************************************';

		set @startime = getdate();
		truncate table silver.crm_prd_info;
		WITH OrderedData AS (
			SELECT 
				*,
				LEAD(prd_start_dt) OVER (ORDER BY prd_start_dt) AS next_start_dt
			FROM bronze.crm_prd_info
		),
		FixDates AS (
			SELECT
				*,
				CASE 
					WHEN next_start_dt = prd_start_dt THEN DATEADD(DAY, 1, next_start_dt)
					WHEN next_start_dt > prd_start_dt THEN DATEADD(DAY, -1, next_start_dt)
					ELSE NULL
				END AS prd_end_dt_corr
			FROM OrderedData
		)

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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'M' THEN 'Mountain'
				WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'R' THEN 'Road'
				WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'S' THEN 'Other Sales'
				WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			prd_start_dt,
			prd_end_dt_corr AS prd_end_dt
		FROM FixDates
		ORDER BY prd_start_dt;
		set @endtime = GETDATE();

		print 'Load duration: ' + cast(datediff(second, @startime, @endtime) as nvarchar) + 'second'
		print '>>> ..........'

		print '*********************************************************************************';
		print '>>> Inserting data from bronze into silver.crm_sales_details';
		print '*********************************************************************************';
		print '*********************************************************************************';
		print '>>> trucate and insert';
		print '*********************************************************************************';
		truncate table silver.crm_sales_details;
		set @startime = GETDATE();
		insert into silver.crm_sales_details(
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

		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt <= 0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as nvarchar) as date)
			end as sls_order_dt,
			case when sls_ship_dt <= 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as nvarchar) as date)
			end as sls_ship_dt,
			case when sls_due_dt <= 0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as nvarchar) as date)
			end as sls_due_dt,
			case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
				else sls_sales
			end as sls_sales,
			sls_quantity,
			case when sls_price <= 0 or sls_price is null then abs(sls_sales) / nullif(sls_quantity, 0)
				else sls_price
			end as sls_price
		from bronze.crm_sales_details;
		set @endtime = getdate();

		print 'Load duration ' + cast(datediff(second, @startime, @endtime) as nvarchar) + 'second'

		print '*********************************************************************************';
		print '>>> Inserting data from bronze into silver.erp_cust_az12';
		print '*********************************************************************************';
		print '*********************************************************************************';
		print '>>> Silver loading';
		print '*********************************************************************************';

		set @startime = getdate();
		truncate table silver.erp_cust_az12;
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen)
		select
			case when cid like 'NAS%' then substring(cid, 4, len(cid))
				else cid
			end cid,
			case when bdate > GETDATE() then null
				else bdate
			end bdate,
			case 
				when trim(isnull(gen, 'N/A')) = 'M' then 'Male'
				when trim(isnull(gen, 'N/A')) = 'F' then 'Female'
				when gen is null or gen = '' then 'N/A'
				else trim(gen)
			end gen
		from bronze.erp_cust_az12

		truncate table silver.erp_loc_a101;
		insert into silver.erp_loc_a101(
		cid, cntry
		)
		select
			replace(cid, '-', '') as cid,
			case 
				when trim(isnull(cntry, 'N/A')) = 'US' then 'United State'
				when trim(isnull(cntry, 'N/A')) = 'USA' then 'United State'
				when trim(isnull(cntry, 'N/A')) = 'DE' then 'German'
				when ltrim(rtrim(cntry)) = '' then 'N/A'
				else isnull(cntry, 'N/A')
			end cntry
		from
			bronze.erp_loc_a101;
		set @endtime = getdate();

		print 'Loading duration ' + cast(datediff(second, @startime, @endtime) as nvarchar) + ' second'
		print'>>>>> ...................'

		print '*********************************************************************************';
		print '>>> Inserting data from bronze into silver.erp_cust_az12';
		print '*********************************************************************************';
		print 'Silver loading';
		truncate table silver.erp_px_cat_g1v2;
		set @startime = getdate();
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2
		set @startime = getdate();

		print 'Loading time ' + cast(datediff(second, @startime, @endtime) as nvarchar)
		print'>>> ......................'
		set @endtime = getdate()
		print 'Complete loading duration ' + cast(datediff(second, @startime, @endtime) as nvarchar);
	end try
	begin catch
		print '=============================================================';
		print 'ERROR OCCURED DURING LOADING SILVER LAYER PROCESS';
		print 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS VARCHAR);
		print 'ERROR MESSAGE' + CAST (ERROR_STATE() AS VARCHAR);
		print '=============================================================';
	end catch
end
exec silver.load_silver;
