--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f556202_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f556202_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f556202_cdc
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
	,[emctr] [NVARCHAR](3) 
	,[emco] [NVARCHAR](5) 
	,[emmcu] [NVARCHAR](12) 
	,[emlocn] [NVARCHAR](20) 
	,[emev01] [NVARCHAR](1) 
	,[emev02] [NVARCHAR](1) 
	,[emmath01] [DECIMAL](38, 4) 
	,[emmath02] [DECIMAL](38, 4) 
	,[emdl01] [NVARCHAR](30) 
	,[emdl02] [NVARCHAR](30) 
	,[emtrdj] [VARCHAR](20) 
	,[empddj] [VARCHAR](20) 
	,[emuser] [NVARCHAR](10) 
	,[empid] [NVARCHAR](10) 
	,[emjobn] [NVARCHAR](10) 
	,[emupmj] [VARCHAR](20) 
	,[emtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f556202/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
