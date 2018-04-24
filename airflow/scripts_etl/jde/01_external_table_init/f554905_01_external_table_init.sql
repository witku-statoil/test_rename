--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f554905_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f554905_init

CREATE EXTERNAL TABLE stg_jde.ext_f554905_init
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
	,[rdmcu] [NVARCHAR](12) 
	,[rdmot] [NVARCHAR](3) 
	,[rdan8] [DECIMAL](38, 4) 
	,[rdvehi] [NVARCHAR](12) 
	,[rdrtnm] [NVARCHAR](10) 
	,[rdcgc1] [NVARCHAR](3) 
	,[rdcgc2] [NVARCHAR](3) 
	,[rdfrtp] [NVARCHAR](1) 
	,[rdrtgb] [NVARCHAR](1) 
	,[rdrtum] [NVARCHAR](2) 
	,[rdfrcg] [DECIMAL](38, 4) 
	,[rdeftj] [VARCHAR](20) 
	,[rdexdj] [VARCHAR](20) 
	,[rdcrcd] [NVARCHAR](3) 
	,[rduser] [NVARCHAR](10) 
	,[rdpid] [NVARCHAR](10) 
	,[rdjobn] [NVARCHAR](10) 
	,[rdupmj] [VARCHAR](20) 
	,[rdupmt] [DECIMAL](38, 4) 
	,[rdurcd] [NVARCHAR](2) 
	,[rdurdt] [VARCHAR](20) 
	,[rdurat] [DECIMAL](38, 4) 
	,[rdurab] [DECIMAL](38, 4) 
	,[rdurrf] [NVARCHAR](15) 
	,[rdy55char1] [NVARCHAR](1) 
	,[rdy55char2] [NVARCHAR](1) 
	,[rdy55date1] [VARCHAR](20) 
	,[rdy55date2] [VARCHAR](20) 
	,[rdy55strg1] [NVARCHAR](30) 
	,[rdy55strg2] [NVARCHAR](30) 
	,[rdy55time1] [DECIMAL](38, 4) 
	,[rdy55time2] [DECIMAL](38, 4) 
	,[rdy55amnt1] [DECIMAL](38, 4) 
	,[rdy55amnt2] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f554905/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
