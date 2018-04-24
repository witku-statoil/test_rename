--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f554922_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f554922_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f554922_init
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
	,[scmcu] [NVARCHAR](12) 
	,[scctr] [NVARCHAR](3) 
	,[scaddz] [NVARCHAR](12) 
	,[sceftj] [VARCHAR](20) 
	,[scexdj] [VARCHAR](20) 
	,[scdstn] [DECIMAL](38, 4) 
	,[scum] [NVARCHAR](2) 
	,[scy55char1] [NVARCHAR](1) 
	,[scy55char2] [NVARCHAR](1) 
	,[scy55strg1] [NVARCHAR](30) 
	,[scy55strg2] [NVARCHAR](30) 
	,[scy55amnt1] [DECIMAL](38, 4) 
	,[scy55amnt2] [DECIMAL](38, 4) 
	,[scy55time1] [DECIMAL](38, 4) 
	,[scy55time2] [DECIMAL](38, 4) 
	,[scy55date1] [VARCHAR](20) 
	,[scy55date2] [VARCHAR](20) 
	,[scurrf] [NVARCHAR](15) 
	,[scurdt] [VARCHAR](20) 
	,[scurcd] [NVARCHAR](2) 
	,[scurab] [DECIMAL](38, 4) 
	,[scurat] [DECIMAL](38, 4) 
	,[scuser] [NVARCHAR](10) 
	,[scpid] [NVARCHAR](10) 
	,[scjobn] [NVARCHAR](10) 
	,[scupmj] [VARCHAR](20) 
	,[scupmt] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f554922/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
