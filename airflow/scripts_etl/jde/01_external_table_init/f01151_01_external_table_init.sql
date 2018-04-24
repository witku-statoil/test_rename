--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f01151_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f01151_init

CREATE EXTERNAL TABLE stg_jde.ext_f01151_init
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
	,[eaan8] [DECIMAL](38, 4) 
	,[eaidln] [DECIMAL](38, 4) 
	,[earck7] [DECIMAL](38, 4) 
	,[eaetp] [NVARCHAR](4) 
	,[eaemal] [NVARCHAR](256) 
	,[eauser] [NVARCHAR](10) 
	,[eapid] [NVARCHAR](10) 
	,[eaupmj] [VARCHAR](20) 
	,[eajobn] [NVARCHAR](10) 
	,[eaupmt] [DECIMAL](38, 4) 
	,[eaehier] [DECIMAL](38, 4) 
	,[eaefor] [NVARCHAR](15) 
	,[eaeclass] [NVARCHAR](3) 
	,[eacfno1] [DECIMAL](38, 4) 
	,[eagen1] [NVARCHAR](10) 
	,[eafalge] [NVARCHAR](1) 
	,[easyncs] [DECIMAL](38, 4) 
	,[eacaad] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f01151/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
