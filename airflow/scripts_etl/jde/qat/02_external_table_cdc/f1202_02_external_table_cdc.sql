--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f1202_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f1202_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f1202_cdc
(
	[sys_file_name] [NVARCHAR](100) 
	,[sys_file_ln] [VARCHAR](20) 
	,[sys_integration_id] [NVARCHAR](200) 
	,[sys_extract_dt] [NVARCHAR](40) 
	,[sys_cdc_dt] [NVARCHAR](40) 
	,[sys_cdc_scn] [NVARCHAR](14) 
	,[sys_cdc_operation_type] [NVARCHAR](15) 
	,[sys_cdc_before_after] [NVARCHAR](10) 
	,[sys_line_modified_ind] [VARCHAR](20) 
	,[flaid] [NVARCHAR](8) 
	,[flctry] [DECIMAL](38, 4) 
	,[flfy] [DECIMAL](38, 4) 
	,[flfq] [NVARCHAR](4) 
	,[fllt] [NVARCHAR](2) 
	,[flsbl] [NVARCHAR](8) 
	,[flco] [NVARCHAR](5) 
	,[flapyc] [DECIMAL](38, 4) 
	,[flan01] [DECIMAL](38, 4) 
	,[flan02] [DECIMAL](38, 4) 
	,[flan03] [DECIMAL](38, 4) 
	,[flan04] [DECIMAL](38, 4) 
	,[flan05] [DECIMAL](38, 4) 
	,[flan06] [DECIMAL](38, 4) 
	,[flan07] [DECIMAL](38, 4) 
	,[flan08] [DECIMAL](38, 4) 
	,[flan09] [DECIMAL](38, 4) 
	,[flan10] [DECIMAL](38, 4) 
	,[flan11] [DECIMAL](38, 4) 
	,[flan12] [DECIMAL](38, 4) 
	,[flan13] [DECIMAL](38, 4) 
	,[flan14] [DECIMAL](38, 4) 
	,[flapyn] [DECIMAL](38, 4) 
	,[flawtd] [DECIMAL](38, 4) 
	,[flborg] [DECIMAL](38, 4) 
	,[flpou] [DECIMAL](38, 4) 
	,[flpc] [DECIMAL](38, 4) 
	,[fltker] [DECIMAL](38, 4) 
	,[flbreq] [DECIMAL](38, 4) 
	,[flbapr] [DECIMAL](38, 4) 
	,[flmcu] [NVARCHAR](12) 
	,[flobj] [NVARCHAR](6) 
	,[flsub] [NVARCHAR](8) 
	,[flnumb] [DECIMAL](38, 4) 
	,[fladlm] [DECIMAL](38, 4) 
	,[fladm] [NVARCHAR](2) 
	,[flitac] [NVARCHAR](1) 
	,[fladmp] [DECIMAL](38, 4) 
	,[fladsn] [NVARCHAR](12) 
	,[fldir1] [NVARCHAR](1) 
	,[fldsd] [VARCHAR](20) 
	,[fluser] [NVARCHAR](10) 
	,[fllct] [VARCHAR](20) 
	,[flpid] [NVARCHAR](10) 
	,[flsblt] [NVARCHAR](1) 
	,[flcrcd] [NVARCHAR](3) 
	,[flupmj] [VARCHAR](20) 
	,[fljobn] [NVARCHAR](10) 
	,[flupmt] [DECIMAL](38, 4) 
	,[flchcd] [NVARCHAR](1) 
	,[fldpcf] [NVARCHAR](1) 
	,[flcbxr] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f1202/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)