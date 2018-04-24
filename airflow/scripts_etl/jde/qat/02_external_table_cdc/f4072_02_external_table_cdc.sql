--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4072_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4072_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4072_cdc
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
	,[adast] [NVARCHAR](8) 
	,[aditm] [DECIMAL](38, 4) 
	,[adlitm] [NVARCHAR](25) 
	,[adaitm] [NVARCHAR](25) 
	,[adan8] [DECIMAL](38, 4) 
	,[adigid] [DECIMAL](38, 4) 
	,[adcgid] [DECIMAL](38, 4) 
	,[adogid] [DECIMAL](38, 4) 
	,[adcrcd] [NVARCHAR](3) 
	,[aduom] [NVARCHAR](2) 
	,[admnq] [DECIMAL](38, 4) 
	,[adeftj] [VARCHAR](20) 
	,[adexdj] [VARCHAR](20) 
	,[adbscd] [NVARCHAR](1) 
	,[adledg] [NVARCHAR](2) 
	,[adfrmn] [NVARCHAR](10) 
	,[adfvtr] [DECIMAL](38, 4) 
	,[adfgy] [NVARCHAR](1) 
	,[adatid] [DECIMAL](38, 4) 
	,[adurcd] [NVARCHAR](2) 
	,[adurdt] [VARCHAR](20) 
	,[adurat] [DECIMAL](38, 4) 
	,[adurab] [DECIMAL](38, 4) 
	,[adurrf] [NVARCHAR](15) 
	,[adnbrord] [DECIMAL](38, 4) 
	,[aduomvid] [NVARCHAR](2) 
	,[adfvum] [NVARCHAR](2) 
	,[adpartfg] [NVARCHAR](1) 
	,[adaprs] [NVARCHAR](1) 
	,[aduser] [NVARCHAR](10) 
	,[adpid] [NVARCHAR](10) 
	,[adjobn] [NVARCHAR](10) 
	,[adupmj] [VARCHAR](20) 
	,[adtday] [DECIMAL](38, 4) 
	,[adbktpid] [DECIMAL](38, 4) 
	,[adcrcdvid] [NVARCHAR](3) 
	,[adrulename] [NVARCHAR](10) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4072/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
