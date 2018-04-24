--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f38012_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f38012_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f38012_init
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
	,[dpdmct] [NVARCHAR](12) 
	,[dpdmcs] [DECIMAL](38, 4) 
	,[dpitm] [DECIMAL](38, 4) 
	,[dpdto] [NVARCHAR](1) 
	,[dpdes] [NVARCHAR](12) 
	,[dpdesy] [NVARCHAR](2) 
	,[dpseq] [DECIMAL](38, 4) 
	,[dppsr] [NVARCHAR](12) 
	,[dppsry] [NVARCHAR](2) 
	,[dpcnqy] [NVARCHAR](1) 
	,[dpcnqt] [DECIMAL](38, 4) 
	,[dpaa] [DECIMAL](38, 4) 
	,[dpcrcd] [NVARCHAR](3) 
	,[dpuom] [NVARCHAR](2) 
	,[dpminq] [DECIMAL](38, 4) 
	,[dpmaxq] [DECIMAL](38, 4) 
	,[dpqbal] [DECIMAL](38, 4) 
	,[dpqcom] [DECIMAL](38, 4) 
	,[dpeftj] [VARCHAR](20) 
	,[dpexdj] [VARCHAR](20) 
	,[dptv01] [NVARCHAR](1) 
	,[dptv02] [NVARCHAR](1) 
	,[dptv03] [NVARCHAR](1) 
	,[dpqty1] [DECIMAL](38, 4) 
	,[dptvum] [NVARCHAR](2) 
	,[dpurcd] [NVARCHAR](2) 
	,[dpurdt] [VARCHAR](20) 
	,[dpurab] [DECIMAL](38, 4) 
	,[dpurrf] [NVARCHAR](15) 
	,[dpuser] [NVARCHAR](10) 
	,[dppid] [NVARCHAR](10) 
	,[dpjobn] [NVARCHAR](10) 
	,[dpupmj] [VARCHAR](20) 
	,[dptday] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f38012/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
