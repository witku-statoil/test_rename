--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4906_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4906_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4906_init
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
	,[cmcars] [DECIMAL](38, 4) 
	,[cmscac] [NVARCHAR](4) 
	,[cmcamd] [NVARCHAR](1) 
	,[cmstbf] [NVARCHAR](32) 
	,[cmstft] [NVARCHAR](3) 
	,[cmrfq1] [NVARCHAR](2) 
	,[cmrfq2] [NVARCHAR](2) 
	,[cmrndn] [NVARCHAR](1) 
	,[cmdwfc] [DECIMAL](38, 4) 
	,[cmrsla] [NVARCHAR](1) 
	,[cmpfsd] [NVARCHAR](1) 
	,[cmprfm] [DECIMAL](38, 4) 
	,[cmuser] [NVARCHAR](10) 
	,[cmpid] [NVARCHAR](10) 
	,[cmjobn] [NVARCHAR](10) 
	,[cmupmj] [VARCHAR](20) 
	,[cmtday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4906/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
