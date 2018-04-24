--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f0150_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0150_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f0150_cdc
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
	,[maostp] [NVARCHAR](3) 
	,[mapa8] [DECIMAL](38, 4) 
	,[maan8] [DECIMAL](38, 4) 
	,[madss7] [DECIMAL](38, 4) 
	,[mabefd] [VARCHAR](20) 
	,[maeefd] [VARCHAR](20) 
	,[marmk] [NVARCHAR](30) 
	,[mauser] [NVARCHAR](10) 
	,[maupmj] [VARCHAR](20) 
	,[mapid] [NVARCHAR](10) 
	,[majobn] [NVARCHAR](10) 
	,[maupmt] [DECIMAL](38, 4) 
	,[masyncs] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0150/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
