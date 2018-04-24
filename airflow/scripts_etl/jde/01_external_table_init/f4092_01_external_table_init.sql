--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4092_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4092_init

CREATE EXTERNAL TABLE stg_jde.ext_f4092_init
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
	,[gpgpty] [NVARCHAR](1) 
	,[gpgpc] [NVARCHAR](8) 
	,[gpdl01] [NVARCHAR](30) 
	,[gpgpk1] [NVARCHAR](10) 
	,[gpgpk2] [NVARCHAR](10) 
	,[gpgpk3] [NVARCHAR](10) 
	,[gpgpk4] [NVARCHAR](10) 
	,[gpgpk5] [NVARCHAR](10) 
	,[gpgpk6] [NVARCHAR](10) 
	,[gpgpk7] [NVARCHAR](10) 
	,[gpgpk8] [NVARCHAR](10) 
	,[gpgpk9] [NVARCHAR](10) 
	,[gpgpk10] [NVARCHAR](10) 
)
WITH
(
    LOCATION='processing-clean/jde/f4092/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
