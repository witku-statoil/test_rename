--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f554501_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f554501_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f554501_cdc
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
	,[qmukid] [DECIMAL](38, 4) 
	,[qmy55qn] [NVARCHAR](100) 
	,[qmdsc1] [NVARCHAR](30) 
	,[qmy55qs] [NVARCHAR](2) 
	,[qmy55qt] [NVARCHAR](2) 
	,[qmy55jdqn] [NVARCHAR](50) 
	,[qmdsc2] [NVARCHAR](30) 
	,[qmcrcd] [NVARCHAR](3) 
	,[qmuom] [NVARCHAR](2) 
	,[qmdend] [DECIMAL](38, 4) 
	,[qmdntp] [NVARCHAR](1) 
	,[qmdte] [VARCHAR](20) 
	,[qmy55avin] [NVARCHAR](2) 
	,[qmco] [NVARCHAR](5) 
	,[qmmcu] [NVARCHAR](12) 
	,[qmprp4] [NVARCHAR](3) 
	,[qmcomments] [NVARCHAR](256) 
	,[qmflag] [NVARCHAR](1) 
	,[qmurrf] [NVARCHAR](15) 
	,[qmurcd] [NVARCHAR](2) 
	,[qmurab] [DECIMAL](38, 4) 
	,[qmurat] [DECIMAL](38, 4) 
	,[qmurdt] [VARCHAR](20) 
	,[qmuser] [NVARCHAR](10) 
	,[qmpid] [NVARCHAR](10) 
	,[qmjobn] [NVARCHAR](10) 
	,[qmupmt] [DECIMAL](38, 4) 
	,[qmupmj] [VARCHAR](20) 
	,[qmev01] [NVARCHAR](1) 
	,[qmev02] [NVARCHAR](1) 
	,[qmev03] [NVARCHAR](1) 
	,[qmev04] [NVARCHAR](1) 
	,[qmy55char1] [NVARCHAR](1) 
	,[qmy55char2] [NVARCHAR](1) 
	,[qmy55date1] [VARCHAR](20) 
	,[qmy55date2] [VARCHAR](20) 
	,[qmy55strg1] [NVARCHAR](30) 
	,[qmy55strg2] [NVARCHAR](30) 
	,[qmy55time1] [DECIMAL](38, 4) 
	,[qmy55time2] [DECIMAL](38, 4) 
	,[qmy55amnt1] [DECIMAL](38, 4) 
	,[qmy55amnt2] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f554501/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
