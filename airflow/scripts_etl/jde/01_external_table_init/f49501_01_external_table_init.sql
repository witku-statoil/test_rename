--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f49501_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f49501_init

CREATE EXTERNAL TABLE stg_jde.ext_f49501_init
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
	,[rdprnb] [DECIMAL](38, 4) 
	,[rdlnmb] [DECIMAL](38, 4) 
	,[rdorgn] [DECIMAL](38, 4) 
	,[rdancc] [DECIMAL](38, 4) 
	,[rdcars] [DECIMAL](38, 4) 
	,[rdmot] [NVARCHAR](3) 
	,[rdfrsc] [NVARCHAR](8) 
	,[rdfrcg] [DECIMAL](38, 4) 
	,[rdcrcd] [NVARCHAR](3) 
	,[rdfrtp] [NVARCHAR](1) 
	,[rdrtgb] [NVARCHAR](1) 
	,[rdrtum] [NVARCHAR](2) 
	,[rddstn] [DECIMAL](38, 4) 
	,[rdumd1] [NVARCHAR](2) 
	,[rdrtn] [DECIMAL](38, 4) 
	,[rdcnmr] [NVARCHAR](24) 
	,[rdltdt] [DECIMAL](38, 4) 
	,[rdeftj] [VARCHAR](20) 
	,[rdexdj] [VARCHAR](20) 
	,[rdurcd] [NVARCHAR](2) 
	,[rdurdt] [VARCHAR](20) 
	,[rdurat] [DECIMAL](38, 4) 
	,[rdurab] [DECIMAL](38, 4) 
	,[rdurrf] [NVARCHAR](15) 
	,[rduser] [NVARCHAR](10) 
	,[rdpid] [NVARCHAR](10) 
	,[rdjobn] [NVARCHAR](10) 
	,[rdupmj] [VARCHAR](20) 
	,[rdtday] [DECIMAL](38, 4) 
	,[rdtx] [NVARCHAR](1) 
	,[rdtxa1] [NVARCHAR](10) 
	,[rdexr1] [NVARCHAR](2) 
	,[rdtax1] [NVARCHAR](1) 
	,[rdtxa2] [NVARCHAR](10) 
	,[rdexr2] [NVARCHAR](2) 
)
WITH
(
    LOCATION='processing-clean/jde/f49501/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
