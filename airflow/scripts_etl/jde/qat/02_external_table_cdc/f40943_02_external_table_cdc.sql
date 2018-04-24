--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f40943_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f40943_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f40943_cdc
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
	,[oksdgr] [NVARCHAR](8) 
	,[oksdv1] [NVARCHAR](12) 
	,[oksdv2] [NVARCHAR](12) 
	,[oksdv3] [NVARCHAR](12) 
	,[oksdv4] [NVARCHAR](12) 
	,[oksdv5] [NVARCHAR](12) 
	,[oksdv6] [NVARCHAR](12) 
	,[oksdv7] [NVARCHAR](12) 
	,[oksdv8] [NVARCHAR](12) 
	,[okogid] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f40943/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
