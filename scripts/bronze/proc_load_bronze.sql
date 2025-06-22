
/*
========================================================================================================================
========================================================================================================================

Store procedure: Load Bronze Layer (source -> Bronze)

========================================================================================================================

Script pupose:
    This store procedure loads data into the bronze schema from external csv (crm, erp)
    It performs the following data:
        --First it trocates the bronze tables before loading data
        --Use the Bulk insert command to load data from csv files to bronze tables

Parameters:
    None.
    This store procedure does not accept any parameter or return any value.

Usage:
e.g: exec bronze.load_bronze;

========================================================================================================================
========================================================================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	--declaring variable to use them later to identify the exac start and end time of tables loaded--
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_en_time datetime; 
	begin try
		print '====================================================================================';
		print 'Loading all the data from local sources erp/crm to the bronze layer';
		print '====================================================================================';
	
		print '------------------------------------------------------------------------------------';
		print 'crm bronze layer dataset table';
		print '------------------------------------------------------------------------------------';

		print '.....................................................';
		print 'customer information table from crm source';
		print '.....................................................';

		print '......................................................................................................';
		print '>> First Truncate then Insert bulk data into bronze layer in the specify category (e.g:erp_px_cat_g1v2)';
		print '......................................................................................................';

		truncate table bronze.crm_cust_info; --to empty the table to avoid duplicate values(refreshing the complete file)--
		set @batch_start_time = getdate();
		set @start_time = getdate();
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		 firstrow = 2,
		 fieldterminator = ',',
		 tablock
		);
		set @end_time = getdate();
		print '>>-----------------------';
		print 'Load duration ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' second';
	
		print '.....................................................';
		print 'product information table from crm sourse';
		print '.....................................................';

		print '......................................................................................................';
		print '>> First Truncate then Insert bulk data into bronze layer in the specify category (e.g:erp_px_cat_g1v2)';
		print '......................................................................................................';

		truncate table bronze.crm_prd_info;
		set @start_time = getdate();
		bulk insert  bronze.crm_prd_info
		from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate()
		print '>>-----------------------';
		print 'Load duration ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' second';

		print '.....................................................';
		print 'sale details table from crm sourse'
		print '.....................................................';

		print '......................................................................................................';
		print '>> First Truncate then Insert bulk data into bronze layer in the specify category (e.g:erp_px_cat_g1v2)';
		print '......................................................................................................';

		truncate table bronze.crm_sales_details;
		set @start_time = getdate();
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print '>>-----------------------';
		print 'Load duration ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' second';

	-- erp --
	
		print '------------------------------------------------------------------------------------';
		print 'crm bronze layer dataset table';
		print '------------------------------------------------------------------------------------';

		print '.....................................................';
		print 'customer table from erp source';
		print '.....................................................';

		print '......................................................................................................';
		print '>> First Truncate then Insert bulk data into bronze layer in the specify category (e.g:erp_px_cat_g1v2)';
		print '......................................................................................................';

		truncate table bronze.erp_cust_az12;
		set @start_time = getdate();
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>-----------------------';
		print 'Load duration ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' second';

		print '.....................................................';
		print 'location table from erp source'
		print '.....................................................';

		print '......................................................................................................';
		print '>> First Truncate then Insert bulk data into bronze layer in the specify category (e.g:erp_px_cat_g1v2)';
		print '......................................................................................................';

		truncate table bronze.erp_loc_a101;
		set @start_time = getdate();
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>-----------------------';
		print 'Load duration ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' second';

		print '.....................................................';
		print 'price categories table from erp source'
		print '.....................................................';

		print '......................................................................................................';
		print '>> First Truncate then Insert bulk data into bronze layer in the specify category (e.g:erp_px_cat_g1v2)';
		print '......................................................................................................';

		truncate table bronze.erp_px_cat_g1v2;
		set @start_time = getdate();
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>-----------------------';
		print 'Load duration ' + cast(datediff(second, @start_time, @end_time) as varchar) + ' second';
		set @batch_en_time = getdate();
		print '====================================================================';
		print'>> Complete load duration of bronze layer: ' + cast(datediff(second, @batch_start_time, @batch_en_time) as varchar) + ' second';
		print '====================================================================';
	end try
	
	begin catch
		print '=============================================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS VARCHAR);
		print 'ERROR MESSAGE' + CAST (ERROR_STATE() AS VARCHAR);
		print '=============================================================';
	end catch --The goal is to run the catch session if and only if the try session fail to show error handling--
end

--excute the store procedure--
exec bronze.load_bronze --to execute the complete bronze layer--
