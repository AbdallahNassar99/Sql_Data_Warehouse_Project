
/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

create table bronze.crm_cust_info
(
cst_id			int,
cst_key			int,
cst_firstname	nvarchar(50),
cst_lastname	nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr		nvarchar(50),
cst_create_date date,
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
create table bronze.crm_prd_info
(
prd_id		int,
prd_key		nvarchar(50),
prd_nm		nvarchar(50),
prd_cost	int ,
prd_line	nvarchar(50),
prd_start_dt date ,
prd_end_dt	 date 
);

if OBJECT_ID ('bronze.sales_details','U') is not null
 drop table bronze.sales_details
 go
create table bronze.sales_details
(
sls_ord_num nvarchar(50) ,
sls_prd_key nvarchar(50) ,
sls_cust_id int,
sls_order_d int ,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
);


if OBJECT_ID ('bronze.erp_cut_az12','U') is not null
 drop table bronze.erp_cut_az12
 go
create table bronze.erp_cut_az12
(
CID   int,
BDATE date ,
GEN   nvarchar(50) 
);

if OBJECT_ID ('bronze.erp_loc_a101','U') is not null
 drop table bronze.erp_loc_a101
 go
create table bronze.erp_loc_a101
(
CID int,
CNTRY nvarchar(50)
)


if OBJECT_ID ('bronze.erp_px_cat_g1v2','U') is not null
 drop table bronze.erp_px_cat_g1v2
 go
create table bronze.erp_px_cat_g1v2
(
ID			nvarchar(50),
CAT			nvarchar(50),
SUBCAT		nvarchar(50),
MAINTENANCE nvarchar(50)
)
