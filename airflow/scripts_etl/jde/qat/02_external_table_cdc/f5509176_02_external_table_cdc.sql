--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f5509176_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f5509176_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f5509176_cdc
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
	,[qsmcu] [NVARCHAR](12) 
	,[qsy55fypn2] [DECIMAL](38, 4) 
	,[qsrp04] [NVARCHAR](3) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f5509176/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
