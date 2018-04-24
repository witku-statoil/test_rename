--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f41002_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f41002_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f41002_cdc
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
	,[ummcu] [NVARCHAR](12) 
	,[umitm] [DECIMAL](38, 4) 
	,[umum] [NVARCHAR](2) 
	,[umrum] [NVARCHAR](2) 
	,[umustr] [NVARCHAR](1) 
	,[umconv] [DECIMAL](38, 4) 
	,[umcnv1] [DECIMAL](38, 4) 
	,[umuser] [NVARCHAR](10) 
	,[umpid] [NVARCHAR](10) 
	,[umjobn] [NVARCHAR](10) 
	,[umupmj] [VARCHAR](20) 
	,[umtday] [DECIMAL](38, 4) 
	,[umexpo] [NVARCHAR](1) 
	,[umexso] [NVARCHAR](1) 
	,[umpupc] [VARCHAR](20) 
	,[umsepc] [VARCHAR](20) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f41002/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
