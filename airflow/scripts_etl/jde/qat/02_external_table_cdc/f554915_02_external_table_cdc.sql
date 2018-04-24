--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f554915_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f554915_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f554915_cdc
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
	,[vgy55vg] [NVARCHAR](10) 
	,[vgvehi] [NVARCHAR](12) 
	,[vgdsc1] [NVARCHAR](30) 
	,[vgdsc2] [NVARCHAR](30) 
	,[vg98iddesc] [NVARCHAR](500) 
	,[vgdl011] [NVARCHAR](100) 
	,[vgy55char1] [NVARCHAR](1) 
	,[vgy55char2] [NVARCHAR](1) 
	,[vgy55strg1] [NVARCHAR](30) 
	,[vgy55strg2] [NVARCHAR](30) 
	,[vgy55strg3] [NVARCHAR](30) 
	,[vgy55strg4] [NVARCHAR](30) 
	,[vgy55strg5] [NVARCHAR](30) 
	,[vgy55strg6] [NVARCHAR](12) 
	,[vgy55strg7] [NVARCHAR](12) 
	,[vgy55strg8] [NVARCHAR](12) 
	,[vgy55strg9] [NVARCHAR](12) 
	,[vgy55strg10] [NVARCHAR](12) 
	,[vgy55num1] [DECIMAL](38, 4) 
	,[vgy55num2] [DECIMAL](38, 4) 
	,[vgy55num3] [DECIMAL](38, 4) 
	,[vgy55num4] [DECIMAL](38, 4) 
	,[vgy55date1] [VARCHAR](20) 
	,[vgy55date2] [VARCHAR](20) 
	,[vgy55amnt1] [DECIMAL](38, 4) 
	,[vgy55amnt2] [DECIMAL](38, 4) 
	,[vgurrf] [NVARCHAR](15) 
	,[vgurcd] [NVARCHAR](2) 
	,[vgurat] [DECIMAL](38, 4) 
	,[vgurab] [DECIMAL](38, 4) 
	,[vguser] [NVARCHAR](10) 
	,[vgpid] [NVARCHAR](10) 
	,[vgjobn] [NVARCHAR](10) 
	,[vgupmj] [VARCHAR](20) 
	,[vgupmt] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f554915/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
