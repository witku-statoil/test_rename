--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f0005_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f0005_init

CREATE EXTERNAL TABLE stg_jde.ext_f0005_init
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
	,[drsy] [NVARCHAR](4) 
	,[drrt] [NVARCHAR](2) 
	,[drky] [NVARCHAR](10) 
	,[drdl01] [NVARCHAR](30) 
	,[drdl02] [NVARCHAR](30) 
	,[drsphd] [NVARCHAR](10) 
	,[drudco] [NVARCHAR](1) 
	,[drhrdc] [NVARCHAR](1) 
	,[druser] [NVARCHAR](10) 
	,[drpid] [NVARCHAR](10) 
	,[drupmj] [VARCHAR](20) 
	,[drjobn] [NVARCHAR](10) 
	,[drupmt] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f0005/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
