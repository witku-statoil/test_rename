--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f4006_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4006_init

CREATE EXTERNAL TABLE stg_jde.ext_f4006_init
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
	,[oadoco] [DECIMAL](38, 4) 
	,[oadcto] [NVARCHAR](2) 
	,[oakcoo] [NVARCHAR](5) 
	,[oaanty] [NVARCHAR](1) 
	,[oamlnm] [NVARCHAR](40) 
	,[oaadd1] [NVARCHAR](40) 
	,[oaadd2] [NVARCHAR](40) 
	,[oaadd3] [NVARCHAR](40) 
	,[oaadd4] [NVARCHAR](40) 
	,[oaaddz] [NVARCHAR](12) 
	,[oacty1] [NVARCHAR](25) 
	,[oacoun] [NVARCHAR](25) 
	,[oaadds] [NVARCHAR](3) 
	,[oacrte] [NVARCHAR](4) 
	,[oabkml] [NVARCHAR](2) 
	,[oactr] [NVARCHAR](3) 
	,[oauser] [NVARCHAR](10) 
	,[oapid] [NVARCHAR](10) 
	,[oaupmj] [VARCHAR](20) 
	,[oajobn] [NVARCHAR](10) 
	,[oaupmt] [DECIMAL](38, 4) 
	,[oalnid] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f4006/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
