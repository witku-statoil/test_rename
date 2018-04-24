--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f554503_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f554503_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f554503_cdc
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
	,[pry55qn] [NVARCHAR](100) 
	,[pry55qt] [NVARCHAR](2) 
	,[pry55qdte] [VARCHAR](20) 
	,[pry55qs] [NVARCHAR](2) 
	,[pry55quty] [NVARCHAR](1) 
	,[pruncs] [DECIMAL](38, 4) 
	,[prcrcd] [NVARCHAR](3) 
	,[pruom] [NVARCHAR](2) 
	,[pry55avin] [NVARCHAR](2) 
	,[pry55jdqn] [NVARCHAR](50) 
	,[prflag] [NVARCHAR](1) 
	,[prurrf] [NVARCHAR](15) 
	,[prurcd] [NVARCHAR](2) 
	,[prurab] [DECIMAL](38, 4) 
	,[prurat] [DECIMAL](38, 4) 
	,[prurdt] [VARCHAR](20) 
	,[pruser] [NVARCHAR](10) 
	,[prpid] [NVARCHAR](10) 
	,[prjobn] [NVARCHAR](10) 
	,[prupmt] [DECIMAL](38, 4) 
	,[prupmj] [VARCHAR](20) 
	,[pry55char1] [NVARCHAR](1) 
	,[pry55char2] [NVARCHAR](1) 
	,[pry55date1] [VARCHAR](20) 
	,[pry55date2] [VARCHAR](20) 
	,[pry55strg1] [NVARCHAR](30) 
	,[pry55strg2] [NVARCHAR](30) 
	,[pry55time1] [DECIMAL](38, 4) 
	,[pry55time2] [DECIMAL](38, 4) 
	,[pry55amnt1] [DECIMAL](38, 4) 
	,[pry55amnt2] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f554503/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
