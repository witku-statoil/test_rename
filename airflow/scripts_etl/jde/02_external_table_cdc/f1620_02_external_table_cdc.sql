--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f1620_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f1620_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f1620_cdc
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
	,[ctabt1] [NVARCHAR](1) 
	,[ctdl01] [NVARCHAR](30) 
	,[ctcmer] [NVARCHAR](1) 
	,[ctfile] [NVARCHAR](10) 
	,[ctdtai] [NVARCHAR](10) 
	,[ctvalr] [NVARCHAR](2) 
	,[ctcmvl] [NVARCHAR](10) 
	,[ctsy] [NVARCHAR](4) 
	,[ctrt] [NVARCHAR](2) 
	,[ctuser] [NVARCHAR](10) 
	,[ctpid] [NVARCHAR](10) 
	,[ctupmj] [VARCHAR](20) 
	,[ctupmt] [DECIMAL](38, 4) 
	,[ctjobn] [NVARCHAR](10) 
)
WITH
(
    LOCATION='processing-clean/jde/f1620/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
