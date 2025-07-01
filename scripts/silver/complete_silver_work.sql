/*
================================================================================================================
Scripts: The complete ETL process and table creation silver from bronze
================================================================================================================
*/

/*

================================================================================================

DDL Scripts:
Create silver layers (Tables)

------------------------------------------------------------------------------------------------

Script description:
This script create different tables for silver schema.

================================================================================================

*/
if OBJECT_ID('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info; --To drop table table if it already exists--
create table silver.crm_cust_info(
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date Date,
	dwh_create_date datetime default getdate()
);

go;

if OBJECT_ID('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id varchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost nvarchar(50),
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime default getdate()
);

go;

if OBJECT_ID('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime default getdate()
)

go;

if OBJECT_ID('silver.erp_cust_az12', 'U') is not null
	drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50),
	dwh_create_date datetime default getdate()
);

go;

if OBJECT_ID('silver.erp_loc_a101', 'U') is not null
	drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50),
	dwh_create_date datetime default getdate()
);

go;

if OBJECT_ID('silver.erp_px_cat_g1v2', 'U') is not null
	drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50),
	dwh_create_date datetime default getdate()
);


/*
================================================================================================================
Scripts: ETL from bronze to silver layer

----------------------------------------------------------------------------------------------------------------
Quality checks on layer.crm_cust_info table

Check for error and null values [expectation >> NULL]
================================================================================================================
*/

select cst_id, count(cst_id) as count_primeray_key 
from bronze.crm_cust_info 
group by cst_id 
having count(cst_id) > 1 or cst_id is null;

/*
Does not meet the expectation !!!!!!!
>> Let's work on each issue
*/

-- First issue 29466 cst_id--
select * from bronze.crm_cust_info where cst_id = 29466;

/*
==========================================================================================================================
 we have 3 duplicate cst_id with the same cst_key and the first row has 4 null values and the second
 1 null value, all with the creation date from the same customer Lance Jimenez
 But the last row is more complete and it's the last record

 >> Let's make a rank on all those values based on the created date and pick only the highest one using 
	WINDOW FUNCTION (ROW_NUMBER)
	row_number() assigns a unique number to each row in a result set based on a defined order
==========================================================================================================================
*/
select *,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id = 29466;

/*
==========================================================================================================================

!!!!!!!!!!	After ranking sct_id 29466 we found that the date 2026-01-27 is the latest one and more suitable for
			analysis

==========================================================================================================================
*/

--Issue 29483 cst_id--
select *, row_number() over (partition by cst_id order by cst_create_date desc) as last_flag
from bronze.crm_cust_info
where cst_id = 29483;
/*
==========================================================================================================================

!!!!!!!!! We always has the fresh cst_id as the best choice likewise here again with cst_id = 29483

************************************************************************************************************
			Therefore let's rank every cst_id and remove the duplicate by keeping latest/fresh cst_id
			to get a final clean result.
************************************************************************************************************

==========================================================================================================================

SOLUTION !!!!!!!!!!!!!

*/

select * 
from
	(
	select *, 
		row_number() over (partition by cst_id order by cst_create_date) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	)t 
where flag_last = 1;

/*
==========================================================================================================================

Now let's have a lookon the unwanted spaces in string value columns wise

Expectation No result

==========================================================================================================================
*/

select * from bronze.crm_cust_info;
select cst_firstname from bronze.crm_cust_info where cst_firstname != trim(cst_firstname); --we check here if there are strings with spaces--
select cst_lastname from bronze.crm_cust_info where cst_lastname != trim(cst_lastname)
select cst_marital_status from bronze.crm_cust_info where cst_marital_status != trim(cst_marital_status)
select cst_gndr from bronze.crm_cust_info where cst_gndr != trim(cst_gndr)

/*
----------------------------------------------------------------------------
Unfortunately they're results for cst_firstname, cst_lastname!!!!!!!!
----------------------------------------------------------------------------

************************************************************************************************************
			Therefore We need to clean those extra-spaces from cst_firstname, cst_lastname columns 
			in bronze.crm_cust_info
************************************************************************************************************

==========================================================================================================================

SOLUTION !!!!!!!!!!!!!
*/
select
	cst_id,
	cst_key,
	trim(cst_firstname),
	trim(cst_lastname),
	cst_marital_status,
	cst_gndr,
	cst_create_date
from bronze.crm_cust_info

/*
==========================================================================================================================

Let's have a look on the two low cardinality columns cst_marital_status and cst_gndr

Expectation No other letters than M, F or NULL for cst_gndr and M, S or NULL for cst_marital_status

==========================================================================================================================
*/

select distinct cst_gndr from bronze.crm_cust_info;
select distinct cst_marital_status from bronze.crm_cust_info;

/*
----------------------------------------------------------------------------
Fortunately it's muching expected result !!!!!!!!
----------------------------------------------------------------------------

************************************************************************************************************
			Therefore we keep the transformation as it is but we will just 
			make some naming changes in the cst_marital_status, and cst_gndr 
			e.g: (M -> Male, S -> Single, ...) to make them more friendly readable
			But we also trimed the columns to be double sure there's no white space
************************************************************************************************************

==========================================================================================================================

SOLUTION !!!!!!!!!!!!!
*/
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
from bronze.crm_cust_info

/*
==========================================================================================================================

Let's have a look at the te date column

Expectation: Let's make sure it's a real date

NB:	you can also add character_maximum_length to the select part

==========================================================================================================================
*/
select * from bronze.crm_cust_info;
select 
	table_name, 
	column_name,
	data_type
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = 'crm_cust_info' and COLUMN_NAME = 'cst_create_date';

/*
----------------------------------------------------------------------------
Fortunately it's muching expected result !!!!!!!!
----------------------------------------------------------------------------

************************************************************************************************************
			Therefore we'll keep the same syntax
************************************************************************************************************

==========================================================================================================================

SOLUTION !!!!!!!!!!!!!
*/
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
from bronze.crm_cust_info;


/*
==========================================================================================================================

Now we need to load the transformation applied on the data from bronze layer in crm_cust_info dataset/table
into the silver layer

Also check if everything has clean as expected

Let's do it >>

Expectation: No result

==========================================================================================================================
*/
truncate table silver.crm_cust_info;

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

/*
-------------------------------------------------------------------------------------------------------------
*/

select * from silver.crm_cust_info;

select cst_id, count(cst_id) as duplicate_ID 
from silver.crm_cust_info 
group by cst_id 
having count(cst_id) > 1 or cst_id is null;

select cst_id, count(cst_id) as duplicate_ID 
from bronze.crm_cust_info 
group by cst_id 
having count(cst_id) > 1 or cst_id is null;

select * from siver.crm_cust_info;
select cst_firstname from silver.crm_cust_info where cst_firstname != trim(cst_firstname); --we check here if there are strings with spaces--
select cst_lastname from silver.crm_cust_info where cst_lastname != trim(cst_lastname)
select cst_marital_status from silver.crm_cust_info where cst_marital_status != trim(cst_marital_status)
select cst_gndr from silver.crm_cust_info where cst_gndr != trim(cst_gndr)

/*
----------------------------------------------------------------------------
Fortunately it's muching expected result !!!!!!!!
----------------------------------------------------------------------------

************************************************************************************************************
			We're all good 
			Because no triming or duplicate result and the low cardinality columns 
			shows as expected by doing these steps:
			>>	Data consistency: with trim() to avoid unwanted white spaces
			>>	Data normilization/standarization: to rename or change the expression of a columns
				We change coded values to meaningful/use-friendly expressions
				e.g: when upper(trim(cst_marital_status)) = 'S' then 'Single'
					 when upper(trim(cst_marital_status)) = 'M' then 'Married'
			>>	Handling missing values:
				Here we field the blanks by a default values 'N/A'
			>>	Data cleasing: Removing duplicates for having one record
				e.g: cst_id by ranking the most fresh id (which were the most relent to us) 
				and keep those on the record only
			>>	the same logic can be palced into data filtering 
			We can now say the quality of the silver layer is effect
************************************************************************************************************

			Finaly We are happy with the first transformed table silver.crm_cust_info

==========================================================================================================================
*/




/*
================================================================================================================
Quality checks on layer.crm_prod_info table

Check for error and null values [expectation >> NULL]
================================================================================================================
*/

select * from bronze.crm_prd_info;

--Checking duplicate values for only prd_id, and prd_key [Data cleasing and filtering]--
select prd_id, count(prd_id) as duplicate_ID 
from bronze.crm_prd_info
group by prd_id
having count(prd_id) > 1 or prd_id is null;

select prd_key, count(prd_key) as duplicate_ID 
from bronze.crm_prd_info
group by prd_key
having count(prd_key) > 1 or prd_key is null; 

--Checking null values for all columns wise [Data cleasing and filtering]--
select 
	sum(case when prd_id is null then 1 else 0 end) as prd_id,
	sum(case when prd_key is null then 1 else 0 end) as prd_key,
	sum(case when prd_nm is null then 1 else 0 end) as prd_nm,
	sum(case when prd_cost is null then 1 else 0 end) as prd_cost,
	sum(case when prd_line is null then 1 else 0 end) as prd_line,
	sum(case when prd_start_dt is null then 1 else 0 end) as prd_start_dt,
	sum(case when prd_end_dt is null then 1 else 0 end) as prd_end_dt
from bronze.crm_prd_info

--Transformation []--

update bronze.crm_prd_info
set
	prd_cost = ISNULL(prd_cost, 0),
	prd_line = isnull(prd_line, 'N/A'),
	prd_end_dt = isnull(prd_end_dt, '1900-01-01')
where prd_cost is null or prd_line is null or prd_end_dt is null;

--create new column cat_id substring of prd_key and making similar to id in erp_px_cat_g1v2 table ['-' >> '_']--
select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

--prd_key from crm_prd_info table vs id from erp_px_cat_g1v2 -->> id not matching--
select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info
where 
	replace(substring(prd_key, 1, 5), '-', '_') not in
	(select distinct id from bronze.erp_px_cat_g1v2);

--Create new column from prd_key with the left over character strating from the end cat_id called prd_key also--
select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

--Compare prd_key with sls_prd_key to see of there is no information not taken into account--
select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info
where
	substring(prd_key, 7, len(prd_key)) not in
	(select sls_prd_key from bronze.crm_sales_details);

--check if those id has orders--

select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info
where
	substring(prd_key, 7, len(prd_key)) not in
	(select sls_prd_key from bronze.crm_sales_details) and prd_key like '%FR-M2%';


/*
	Seems like most of the product don't have any order because either the prd_end_dt having
	1900-01-01 or N/A in prd_line or 0 in prd_cost which are placehoders we created

	NB::	as of now we will suppose all these keys have no orders even thought some has it
			Therefore after sometimes we can make a deep understanding
*/


--Let's check if there is unwanted space in crm_prd_info table --

DECLARE @TableName NVARCHAR(128) = 'crm_prd_info';
DECLARE @SchemaName NVARCHAR(128) = 'bronze';
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = STRING_AGG(
    'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) AS UnwantedSpacesCount ' +
    'FROM [' + @SchemaName + '].[' + @TableName + '] ' +
    'WHERE [' + COLUMN_NAME + '] LIKE '' %'' ' +
    '   OR [' + COLUMN_NAME + '] LIKE ''% '' ' +
    '   OR LTRIM(RTRIM([' + COLUMN_NAME + '])) = '''' ',
    'UNION ALL '
)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @TableName
  AND TABLE_SCHEMA = @SchemaName
  AND DATA_TYPE IN ('char', 'varchar', 'nvarchar', 'text', 'nchar');

EXEC sp_executesql @SQL;

--trim prd_line--

select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	prd_cost,
	trim(prd_line),
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

--Making sure there is no line--
select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	prd_cost,
	trim(prd_line),
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info
where prd_line != trim(prd_line);

--Let's have a look on numerical columns--
select column_name, data_type
from INFORMATION_SCHEMA.columns
where 
	TABLE_NAME = 'crm_prd_info' and TABLE_SCHEMA = 'bronze' and DATA_TYPE in 
	('int', 'bigint', 'smallint', 'tinyint',
        'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney')

--prd_cost is not reconized as a numerical value--
--We need to check its data type and change it into integer--
select
	column_name,
	CHARACTER_MAXIMUM_LENGTH,
	data_type,
	IS_NULLABLE
from INFORMATION_SCHEMA.columns
where 
	table_name = 'crm_prd_info'and
	TABLE_SCHEMA = 'bronze'
order by ordinal_position;

--prd_cost is a varchar we need to check if there are character out of numbers in that column prd_cost--
select prd_cost from bronze.crm_prd_info where prd_cost like '%[^0-9]%' or prd_cost is null

--Now let's convert this column into integer--
alter table bronze.crm_prd_info
alter column prd_cost int

select
	column_name,
	CHARACTER_MAXIMUM_LENGTH,
	data_type,
	IS_NULLABLE
from INFORMATION_SCHEMA.columns
where 
	table_name = 'crm_prd_info'and
	TABLE_SCHEMA = 'bronze'
order by ordinal_position;

--we need to chaeck if the numerical columns have null values or negative values-- 
select prd_id, prd_cost
from bronze.crm_prd_info
where prd_id < 0 or prd_cost < 0 or prd_id is null or prd_cost is null

--Solution by removing changing null values in prd_cost into 0--
select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	isnull(prd_cost, 0) as prd_cost,
	trim(prd_line) as prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

--Let's check on the low cardinality and make it more friendly readable--
select distinct prd_line from bronze.crm_prd_info;

select 
	prd_id,
	replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
	substring(prd_key, 7, len(prd_key)) as prd_key,
	prd_key,
	prd_nm,
	isnull(prd_cost, 0) as prd_cost,
	case 
		upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'N/A'
	end as prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info

--Let's deep dive into the last two columns start date and end date-- 
--end date should not be earlier than start date--
select * from bronze.crm_prd_info where prd_end_dt < prd_start_dt;

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
            ELSE NULL  -- or 'N/A' if prd_end_dt is VARCHAR
        END AS corrected_end_dt
    FROM OrderedData
)
SELECT 
    prd_id,
    prd_start_dt,
    prd_end_dt AS original_end_dt,
    corrected_end_dt
FROM FixDates
ORDER BY prd_start_dt;

-------------
--Result
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
            ELSE NULL  -- or 'N/A' if prd_end_dt is VARCHAR
        END AS corrected_end_dt
    FROM OrderedData
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_key AS original_prd_key,
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
    prd_end_dt AS original_end_dt,
    corrected_end_dt
FROM FixDates
ORDER BY prd_start_dt;

--Last version result
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
    prd_end_dt_corr as prd_end_dt
FROM FixDates
ORDER BY prd_start_dt;

--Now let's insert the data into the silver.prd_info layer/table
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

select * from silver.crm_prd_info;

/*
************************************************************************************************************
			First we created a CTE (Common table expression) which is a temporary table
			to lead to the next row ahead of the column prd_start_dt
			Then 
			>> Data Normalization/standaarization: we trnsformed prd_start_dt based on 
			the next date and save them into a new column, and trimed prd_line because they were 
			so much white spaces in.
			>> Handling missing values: We first did that in the entire table columns 
			but to make sure we specified that in the prd_line transformation
			>> Data cleasing: we got to make sure prd_id was unique
			>> Data filtering: we filtered end date that < start date which don't make any sense
								and ensure a valid modification
************************************************************************************************************

			Finaly We are happy with our new table silver.crm_prd_info

==========================================================================================================================
*/

/*
================================================================================================================
Quality checks on layer.crm_sales_details table

Check for error and null values [expectation >> NULL]
================================================================================================================
*/

select * from bronze.crm_sales_details;

-- Data consstency (Triming)

select * from bronze.crm_sales_details

select column_name from INFORMATION_SCHEMA.columns where TABLE_NAME = 'crm_sales_details' and TABLE_SCHEMA = 'bronze'

select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details

--Checkunwanted spaces
DECLARE @TableName NVARCHAR(128) = 'crm_sales_details';
DECLARE @SchemaName NVARCHAR(128) = 'bronze';
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = STRING_AGG(
    'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) AS UnwantedSpacesCount ' +
    'FROM [' + @SchemaName + '].[' + @TableName + '] ' +
    'WHERE [' + COLUMN_NAME + '] LIKE '' %'' ' +
    '   OR [' + COLUMN_NAME + '] LIKE ''% '' ' +
    '   OR LTRIM(RTRIM([' + COLUMN_NAME + '])) = '''' ',
    'UNION ALL '
)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @TableName
  AND TABLE_SCHEMA = @SchemaName
  AND DATA_TYPE IN ('char', 'varchar', 'nvarchar', 'text', 'nchar');

EXEC sp_executesql @SQL;

--Check integrity of each key

--sls_prd_key and prd_key
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

--cust_id and cust_id
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

select * from silver.crm_cust_info;

--Check data type [because the date seems integer]
select
	column_name,
	CHARACTER_MAXIMUM_LENGTH,
	data_type,
	IS_NULLABLE
from INFORMATION_SCHEMA.columns
where 
	table_name = 'crm_sales_details'and
	TABLE_SCHEMA = 'bronze'
order by ordinal_position;

--Let's check if there're values <= 0
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_order_dt <= 0 or
sls_ship_dt <= 0 or
sls_due_dt <= 0;

--Only the column sls_order_dt has so many 0

--Let's change 0 into null and add the business boundries
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	nullif(sls_order_dt, 0) as sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_order_dt <= 0 or 
len(sls_order_dt) != 8 or
sls_order_dt < 19000101 or --down_Boundry
sls_order_dt > 20500101 --Upper_boundry

--Converting the column sls_order_dt, ... into date and remove the null and length less than 8
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
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details

--Let's check if the shipping date < order date
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
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_ship_dt < sls_order_dt or sls_order_dt > sls_due_dt;

--Let's check if the three last columns are consistent
--sales = qty*price

select 
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where 
	sls_sales != sls_quantity * sls_price or
	sls_sales is null or sls_quantity is null or sls_price is null or
	sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0

--We checked on the stakeholders and we found these three rules for managing these last three columns

/*
	Rule 1: If the sales is 0, null or negative derive it from qty and price
	Rule 2: if the price is 0, null calculate it using qty and sales
	Rule 3: if the price < 0 (negative) make it positive (absolute value)
*/
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
from bronze.crm_sales_details

select * from silver.crm_sales_details;

/*
************************************************************************************************************
			>> Data Normalization/standaarization: we trnsformed sales qty and price by rules of 
			stakeholders
			>> Handling missing values: we worked on the missing values o nul or negative values 
			The approach was more visible in sales qty and prrice columns 
			Also we cast date columns from 'int' to 'date'
			>> Data cleasing: we cleaned data rows wise
			>> Data filtering: 
************************************************************************************************************

			Finaly We are happy with our new table silver.crm_sales_details

==========================================================================================================================
*/

/*
================================================================================================================
Quality checks on layer erp_cust_az12 table

Check for error and null values [expectation >> NULL]
================================================================================================================
*/

select * from bronze.erp_cust_az12
select * from bronze.crm_cust_info

--While checking cid column against cst_id in cust_az12 and cst_key in cust_info
--cid has some extra characters that stakeholders find not useful
--let's remove it[NAS] but before check unwanted spaces

DECLARE @TableName NVARCHAR(128) = 'erp_cust_az12';
DECLARE @SchemaName NVARCHAR(128) = 'bronze';
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = STRING_AGG(
    'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) AS UnwantedSpacesCount ' +
    'FROM [' + @SchemaName + '].[' + @TableName + '] ' +
    'WHERE [' + COLUMN_NAME + '] LIKE '' %'' ' +
    '   OR [' + COLUMN_NAME + '] LIKE ''% '' ' +
    '   OR LTRIM(RTRIM([' + COLUMN_NAME + '])) = '''' ',
    'UNION ALL '
)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @TableName
  AND TABLE_SCHEMA = @SchemaName
  AND DATA_TYPE IN ('char', 'varchar', 'nvarchar', 'text', 'nchar', 'date');
EXEC sp_executesql @SQL;

-- Removing the extra characters
select
	cid,
	case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end cid,
	bdate,
	isnull(trim(gen), 'N/A') as gen
from bronze.erp_cust_az12

--Let's check the integration of cst_id and cid
select
	case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end cid,
	bdate,
	trim(gen) as gen
from bronze.erp_cust_az12
where 
	case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end not in (select distinct cst_key from silver.crm_cust_info)


--Data normalization/standarization of gen column
select
	column_name,
	CHARACTER_MAXIMUM_LENGTH,
	data_type,
	IS_NULLABLE
from INFORMATION_SCHEMA.columns
where 
	table_name = 'erp_cust_az12'and
	TABLE_SCHEMA = 'bronze'
order by ordinal_position;


select distinct gen from bronze.erp_cust_az12

select * from bronze.erp_cust_az12 where gen not in ('Male', 'Female')

select
	case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end cid,
	bdate,
	case 
		when trim(isnull(gen, 'N/A')) = 'M' then 'Male'
		when trim(isnull(gen, 'N/A')) = 'F' then 'Female'
		when gen is null or gen = '' then 'N/A'
		else trim(gen)
	end gen
from bronze.erp_cust_az12
where gen is null

--let's check if the birth date column has date > 100 age and date < current date
select
	case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid
	end cid,
	bdate,
	case 
		when trim(isnull(gen, 'N/A')) = 'M' then 'Male'
		when trim(isnull(gen, 'N/A')) = 'F' then 'Female'
		when gen is null or gen = '' then 'N/A'
		else trim(gen)
	end gen
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()
order by bdate desc


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

--Let's insert the new transformed data into silver.erp_cust_az12
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

/*check data quality*/
select * from silver.erp_cust_az12;


/*
************************************************************************************************************
			>> Data Normalization/standaarization: we transformed cid to removed unwanted characters
			Also we changed F and M into Female and Male 
************************************************************************************************************

			Finaly We are happy with our new table silver.erp_cust_az12

==========================================================================================================================
*/

/*
================================================================================================================
Quality checks on layer erp_loc_a101 table [bronze -> silver]

Check for error and null values [expectation >> NULL]
================================================================================================================
*/

select * from bronze.erp_loc_a101;
select * from bronze.crm_cust_info;

select distinct cntry from bronze.erp_loc_a101;

select
	cid,
	case 
		when trim(isnull(cntry, 'N/A')) = 'US' then 'United State'
		when trim(isnull(cntry, 'N/A')) = 'USA' then 'United State'
		when trim(isnull(cntry, 'N/A')) = 'DE' then 'German'
		else isnull(cntry, 'N/A')
	end cntry
from
	bronze.erp_loc_a101
where cntry in ('US', 'USA', 'DE')
--We need to manage the null and and blank in cntry

--let's remove the underscord '-' between string values in cid column
insert into silver.erp_loc_a101(
cid, cntry
)
select distinct
	replace(cid, '-', '') as cid,
	case 
		when trim(isnull(cntry, 'N/A')) = 'US' then 'United State'
		when trim(isnull(cntry, 'N/A')) = 'USA' then 'United State'
		when trim(isnull(cntry, 'N/A')) = 'DE' then 'German'
		when ltrim(rtrim(cntry)) = '' then 'N/A'
		else isnull(cntry, 'N/A')
	end cntry
from
	bronze.erp_loc_a101

--Quality checking
select * 
from silver.erp_loc_a101
where
	cid not in (select cst_key from silver.crm_cust_info)


/*
************************************************************************************************************
			>> Data Normalization/standaarization: we transformed cid to removed unwanted characters
			Also we changed F and M into Female and Male 
************************************************************************************************************

			Finaly We are happy with our new table silver.erp_cust_az12

==========================================================================================================================
*/

/*
================================================================================================================
Quality checks on layer erp_px_cat_g1v2 table [bronze -> silver]

Check for error and null values [expectation >> NULL]
================================================================================================================
*/

select * from bronze.erp_px_cat_g1v2
select * from silver.crm_prd_info

DECLARE @TableName NVARCHAR(128) = 'erp_px_cat_g1v2';
DECLARE @SchemaName NVARCHAR(128) = 'bronze';
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = STRING_AGG(
    'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) AS UnwantedSpacesCount ' +
    'FROM [' + @SchemaName + '].[' + @TableName + '] ' +
    'WHERE [' + COLUMN_NAME + '] LIKE '' %'' ' +
    '   OR [' + COLUMN_NAME + '] LIKE ''% '' ' +
    '   OR LTRIM(RTRIM([' + COLUMN_NAME + '])) = '''' ',
    'UNION ALL '
)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @TableName
  AND TABLE_SCHEMA = @SchemaName
  AND DATA_TYPE IN ('char', 'varchar', 'nvarchar', 'text', 'nchar', 'date');
EXEC sp_executesql @SQL;

select
	column_name,
	CHARACTER_MAXIMUM_LENGTH,
	data_type,
	IS_NULLABLE
from INFORMATION_SCHEMA.columns
where 
	table_name = 'erp_px_cat_g1v2'and
	TABLE_SCHEMA = 'bronze'
order by ordinal_position;

select 
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_px_cat_g1v2

select
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from silver.crm_prd_info
where cat_id not in (select id from bronze.erp_px_cat_g1v2)
--insertion
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

--check quality
select * from silver.erp_px_cat_g1v2;

/*
**************************************************************************************************************

Let's now put all the silver table together and truncate before we insert them 
to avoid duplicate values

**************************************************************************************************************
*/
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

--Store procedure 
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
