--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f38011_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f38011_init

CREATE EXTERNAL TABLE stg_jde.ext_f38011_init
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
	,[dfdmct] [NVARCHAR](12) 
	,[dfdmcs] [DECIMAL](38, 4) 
	,[dfcnqy] [NVARCHAR](1) 
	,[dfdto] [NVARCHAR](1) 
	,[dfdes] [NVARCHAR](12) 
	,[dfdesy] [NVARCHAR](2) 
	,[dfitm] [DECIMAL](38, 4) 
	,[dfseq] [DECIMAL](38, 4) 
	,[dfcnqt] [DECIMAL](38, 4) 
	,[dfaa] [DECIMAL](38, 4) 
	,[dfuom] [NVARCHAR](2) 
	,[dfminq] [DECIMAL](38, 4) 
	,[dfmaxq] [DECIMAL](38, 4) 
	,[dfeftj] [VARCHAR](20) 
	,[dfexdj] [VARCHAR](20) 
	,[dfuprc] [DECIMAL](38, 4) 
	,[dfasn] [NVARCHAR](8) 
	,[dfpasn] [NVARCHAR](8) 
	,[dfmcu] [NVARCHAR](12) 
	,[dfcrcd] [NVARCHAR](3) 
	,[dftv01] [NVARCHAR](1) 
	,[dftv02] [NVARCHAR](1) 
	,[dftv03] [NVARCHAR](1) 
	,[dfqty1] [DECIMAL](38, 4) 
	,[dftvum] [NVARCHAR](2) 
	,[dfurcd] [NVARCHAR](2) 
	,[dfurdt] [VARCHAR](20) 
	,[dfurat] [DECIMAL](38, 4) 
	,[dfurab] [DECIMAL](38, 4) 
	,[dfurrf] [NVARCHAR](15) 
	,[dfuser] [NVARCHAR](10) 
	,[dfpid] [NVARCHAR](10) 
	,[dfjobn] [NVARCHAR](10) 
	,[dfupmj] [VARCHAR](20) 
	,[dftday] [DECIMAL](38, 4) 
	,[dfrpqt] [DECIMAL](38, 4) 
	,[dfqedt] [NVARCHAR](1) 
	,[dfqed3] [NVARCHAR](1) 
	,[dfqed2] [NVARCHAR](1) 
	,[dfpdp5] [NVARCHAR](3) 
)
WITH
(
    LOCATION='processing-clean/jde/f38011/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
