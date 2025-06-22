/*
======================================================================================
  DDL Scripts: Create Bronze Tables
======================================================================================
  Script purpose
    This script creates tables in the 'bronze' 
    schema and dropping them if they already exist 
    Run this script to redefine the DDL scrupture of 'Bronze' table
======================================================================================
*/

use master;
use DataWarehouse;

--created bronze silver and gold layers--
create schema bronze;

go;

create schema sivler;

go;

create schema gold;
--End created bronze silver and gold layers--

  
--start of bronze tables creation trial--
--DDL(Data Defintion Language) creation--
--Bronze layer--
if OBJECT_ID('bronze.crm_cust_info', 'U') is not null
	drop bronze.crm_cust_info; --To drop table if it already exists--
create table bronze.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date Date
);

go;

if OBJECT_ID('bronze.crm_prd_info', 'U') is not null
	drop bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost nvarchar(50),
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date
);

go;

if OBJECT_ID('bronze.crm_sales_details', 'U') is not null
	drop bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)

go;

if OBJECT_ID('bronze.erp_cust_az12', 'U') is not null
	drop bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);

go;

if OBJECT_ID('bronze.erp_loc_a101', 'U') is not null
	drop bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);

go;

if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is not null
	drop bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);
--End of bronze tables creation trial--
--complete bulk insertion of data from source to sql server into the bronze layer--
--crm--
truncate table bronze.crm_cust_info; --to empty the table to avoid duplicate values(refreshing the complete file)--
bulk insert bronze.crm_cust_info -- inserting data from source crm-- 
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
 firstrow = 2,
 fieldterminator = ',',
 tablock
);

go;

truncate table bronze.crm_prd_info;
bulk insert  bronze.crm_prd_info
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);

go;

truncate table bronze.crm_sales_details;
bulk insert bronze.crm_sales_details
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);

go; 

truncate table bronze.erp_cust_az12;
bulk insert bronze.erp_cust_az12
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
--erp--
go;

truncate table bronze.erp_cust_az12;
bulk insert bronze.erp_cust_az12
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

go;

truncate table bronze.erp_loc_a101;
bulk insert bronze.erp_loc_a101
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

go;

truncate table bronze.erp_px_cat_g1v2;
bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\Admin\Downloads\youtube\Data warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
);

