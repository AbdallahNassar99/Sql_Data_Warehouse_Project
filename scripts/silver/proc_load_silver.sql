/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

Create or alter procedure silver.load_silver as 
begin

declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime
begin try
set @batch_start_time=getdate()
set @start_time=GETDATE()
print '>> Truncate table: silver.crm_cust_info '
truncate table silver.crm_cust_info
Print '>> Insert Data into silver.crm_cust_info'
insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date )
select  
cst_id,
cst_key,
trim (cst_firstname) as cst_firstname ,
trim (cst_lastname) as cst_lastname,
case 
	 when upper(trim(cst_marital_status))= 'S'then 'Single'
	 when upper(trim(cst_marital_status))='M' then 'Married'
	 else 'n/a'
end cst_marital_status,
case 
	 when upper(trim(cst_gndr))='M'then 'Male'
	 when upper(trim(cst_gndr))='F'then 'Female'
	 else 'n/a'
end cst_gndr,
cst_create_date
from
(select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date ) as flag_last
from bronze.crm_cust_info)t
where flag_last=1 and cst_id is not null
set @end_time=GETDATE()
print '--Load Duration'+ cast (DATEDIFF(second , @start_time,@end_time)as nvarchar)+'seconed'
print'---------------------------------'

set @start_time=GETDATE()
print '>> Truncate table: silver.crm_prd_info '
truncate table silver.crm_prd_info
Print '>> Insert Data into silver.crm_prd_info'
insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt ,
prd_end_dt
)
select
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_')as cat_id,
SUBSTRING(prd_key,7,len(prd_key))as prd_key,
prd_nm,
isnull (prd_cost,0) as prd_cost,
case upper(trim(prd_line)) 
	when 'M' then 'Mountain'
	when 'R' then 'Road'
	when 'S' then 'Other Sales'
	when 'T' then 'Touring'
	else 'n/a'
end prd_line,
cast (prd_start_dt as date) as prd_start_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt ) as prd_end_dt
from bronze.crm_prd_info
set @end_time=GETDATE()
print '--Load Duration'+ cast (DATEDIFF(second , @start_time,@end_time)as nvarchar)+'seconed' 
print'---------------------------------'


set @start_time=GETDATE()
print '>> Truncate table: silver.sales_details '
truncate table silver.sales_details
Print '>> Insert Data into silver.sales_details'
insert into silver.sales_details(
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_d  ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity ,
sls_price 
)
select 
sls_ord_num  ,
sls_prd_key  ,
sls_cust_id ,
case when  sls_order_d =0 or len (sls_order_d) !=8 then null 
	else CAST (cast (sls_order_d as varchar)as date)
end sls_order_d,
case when  sls_ship_dt =0 or len (sls_ship_dt) !=8 then null 
	else CAST (cast (sls_ship_dt as varchar)as date)
end sls_ship_dt,
case when  sls_due_dt =0 or len (sls_due_dt) !=8 then null 
	else CAST (cast (sls_due_dt as varchar)as date)
end sls_ship_dt,
case when sls_sales IS NULL or sls_sales <0 or sls_sales !=sls_quantity * sls_price 
		then abs (sls_price) * abs (sls_quantity)
	 else sls_sales
end sls_sales ,
sls_quantity ,
case when sls_price <0 or sls_price IS NULL  
	then sls_sales /  nullif(sls_quantity,0)
	else sls_price
end sls_price
from bronze.sales_details
set @end_time=GETDATE()
print '--Load Duration'+ cast (DATEDIFF(second , @start_time,@end_time)as nvarchar)+'seconed' 
print'---------------------------------'


set @start_time=GETDATE()
print '>> Truncate table: silver.erp_cut_az12 '
truncate table silver.erp_cut_az12
Print '>> Insert Data into silver.erp_cut_az12'
insert into silver.erp_cut_az12(
CID,
BDATE,
GEN
)
select 
case when CID like 'NAS%' then SUBSTRING(CID,4,len(CID)) 
	 else CID 
end CID,
	case when BDATE > GETDATE() then NULL
	else BDATE
end As BDATE , 
case when  upper (TRIM(GEN)) in ('M','MALE') then 'Male'
	 when   upper (TRIM(GEN)) in ('F','FEMALE') then 'Female'
	else 'n/a'
end GEN
from bronze.erp_cut_az12
set @end_time=GETDATE()
print '--Load Duration'+ cast (DATEDIFF(second , @start_time,@end_time)as nvarchar)+'seconed' 
print'---------------------------------'


set @start_time=GETDATE()
print '>> Truncate table: silver.erp_loc_a101 '
truncate table silver.erp_loc_a101
Print '>> Insert Data into silver.erp_loc_a101'
insert into silver.erp_loc_a101
(
CID,
CNTRY
)
select
replace (CID , '-', '')as CID,
 case when CNTRY = 'DE' then 'Germany'
 when CNTRY in ('US' ,'USA') then 'Uited States'
 when CNTRY ='' or CNTRY is null then 'n/a'
 else CNTRY
 end CNTRY
 from bronze.erp_loc_a101
 set @end_time=GETDATE()
print '--Load Duration'+ cast (DATEDIFF(second , @start_time,@end_time)as nvarchar)+'seconed' 
print'---------------------------------'


set @start_time=GETDATE()
print '>> Truncate table: silver.erp_px_cat_g1v2 '
truncate table silver.erp_px_cat_g1v2
Print '>> Insert Data into silver.erp_px_cat_g1v2'
 insert into silver.erp_px_cat_g1v2
 (ID, CAT,SUBCAT,MAINTENANCE)
 select * from bronze.erp_px_cat_g1v2
 set @end_time=GETDATE()
print '--Load Duration'+ cast (DATEDIFF(second , @start_time,@end_time)as nvarchar)+'seconed'
print'---------------------------------'

 
 set @batch_end_time = GETDATE()
 print '=============================='
 print 'Loading silver is complate '
  print 'Totla Load Duration'+cast (datediff(second,@batch_start_time,@batch_end_time)as nvarchar) + 'second'
  print '=============================='

 end try
	    
 begin catch
	    PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print'Error Message'+error_message()
		print'Error Message'+ CAST (error_NUMBER()AS NVARCHAR)
		print'Error Message'+ CAST (error_STATE()AS NVARCHAR)

 end catch
 end 


 exec silver.load_silver
