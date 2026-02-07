/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER  PROCEDURE bronze.load_bronze as 
begin 
declare @Start_time datetime, @End_time datetime,@Batch_start_time datetime,@Batch_end_time datetime
begin try 
set @Batch_start_time =GETDATE();
print '================================='
print 'loading data'
print '================================='

print '     '
print ' ------------------------'
print 'Loading CRM Tables'
print ' ------------------------'
print'     '
print'<< -----Loading Table crm_cust_info----->>'
set @Start_time=GETDATE();
TRUNCATE TABLE  bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
from 'D:\BI ITI\SQL Server\Baraa\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
with 
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @End_time=GETDATE();
print 'Load duration: '+ cast( datediff(second,@Start_time,@End_time) as nvarchar)+ 'seconed';
print '-----------------------------------------------'

print '   '
print '<<-----Loading Table crm_prd_info----->>'
set @Start_time=GETDATE();

TRUNCATE TABLE  bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info 
from 'D:\BI ITI\SQL Server\Baraa\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
with 
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @End_time=GETDATE();
print 'Load duration: '+ cast( datediff(second,@Start_time,@End_time) as nvarchar)+ 'seconed';
print '--------------------------------------------------------'

print '   '
print '<<-----Loading Table crm_prd_inf----->>'
set @Start_time=GETDATE();
TRUNCATE TABLE  bronze.sales_details
BULK INSERT bronze.sales_details 
from 'D:\BI ITI\SQL Server\Baraa\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
with 
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @End_time=GETDATE();
print 'Load duration: '+ cast( datediff(second,@Start_time,@End_time) as nvarchar)+ 'seconed';
print '---------------------------------------------------------'

print '  '
print ' ------------------------'
print 'Loading ERP Tabels'
print ' ------------------------'

print '<<-----Loading Table erp_cut_az12----->>'
set @Start_time=GETDATE();
TRUNCATE TABLE  bronze.erp_cut_az12;
BULK INSERT bronze.erp_cut_az12
from 'D:\BI ITI\SQL Server\Baraa\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
with 
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @End_time=GETDATE();
print 'Load duration: '+ cast( datediff(second,@Start_time,@End_time) as nvarchar)+ 'seconed';
print '----------------------------------------------------------'
print'         '
print '<<-----Loading Table erp_loc_a101----->>'
set @Start_time=GETDATE();
TRUNCATE TABLE  bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
from 'D:\BI ITI\SQL Server\Baraa\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
with 
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @End_time=GETDATE();
print 'Load duration: '+ cast( datediff(second,@Start_time,@End_time) as nvarchar)+ 'seconed';
print '---------------------------------------------------------'
print'         '
print '<<----Loading Table erp_px_cat_g1v2----->>'
set @Start_time=GETDATE();
TRUNCATE TABLE  bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
from 'D:\BI ITI\SQL Server\Baraa\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
with 
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @End_time=GETDATE();
print 'Load duration: '+ cast( datediff(second,@Start_time,@End_time) as nvarchar)+ 'seconed';
print '--------------------------------------------------------'
print'    '
 set @Batch_end_time = GETDATE()
print '<<<<<------------------------------------->>>>>>'
print 'Load Bronze Layer is Complated'
 print 'Total Load duration: '+ cast( datediff(second,@Batch_start_time,@Batch_end_time) as nvarchar)+ 'second';
print'<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
end try 

begin catch 
print'Error Occured During Loading Data'
print'Error Message'+ERROR_MESSAGE();
print'Error Message'+ cast (ERROR_NUMBER()as Nvarchar);
print'Error Message'+ cast (ERROR_STATE()as NVarchar);
end catch
end


