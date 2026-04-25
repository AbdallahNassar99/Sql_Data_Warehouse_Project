/*
==========================================================
Check quality of data for silver
==========================================================
*/

select * from bronze.crm_cust_info

select cst_id , COUNT(cst_id) from bronze.crm_cust_info
group by cst_id
having COUNT(*)>1

select *,TRIM (cst_firstname) from bronze.crm_cust_info

select * from bronze.crm_prd_info

select prd_id , COUNT(prd_id) from bronze.crm_prd_info
group by prd_id
having COUNT(*)>1

select *,TRIM (cst_firstname) from bronze.crm_prd_info




select * from bronze.sales_details

select * from bronze.crm_prd_info


select sls_ord_num , COUNT(*) from bronze.sales_details
group by sls_ord_num
having COUNT(*)>1


select * from (
select *,
ROW_NUMBER() over(partition by sls_ord_num order by sls_price) as tst
from bronze.sales_details)t
where tst>1

select sls_sales, sls_quantity , sls_price from silver.sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_sales<=0
 or sls_quantity is null or sls_quantity<=0
 or sls_price is null or sls_price <=0
 order by  sls_sales, sls_quantity , sls_price

 select sls_sales, sls_quantity , sls_price from silver.sales_details 
 where sls_sales is null


 select CID , BDATE , GEN from bronze.erp_cut_az12
where BDATE>getdate()

select distinct GEN FROM  bronze.erp_cut_az12



