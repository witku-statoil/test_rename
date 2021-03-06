--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f40205_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f40205_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f40205_cdc
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
	,[lflnty] [NVARCHAR](2) 
	,[lflnds] [NVARCHAR](30) 
	,[lflnd2] [NVARCHAR](30) 
	,[lfgli] [NVARCHAR](1) 
	,[lfivi] [NVARCHAR](1) 
	,[lfari] [NVARCHAR](1) 
	,[lfapi] [NVARCHAR](1) 
	,[lfrsgn] [NVARCHAR](1) 
	,[lftxyn] [NVARCHAR](1) 
	,[lfprft] [NVARCHAR](1) 
	,[lfcdsc] [NVARCHAR](1) 
	,[lftx01] [NVARCHAR](1) 
	,[lftx02] [NVARCHAR](1) 
	,[lfglc] [NVARCHAR](4) 
	,[lfpdc1] [NVARCHAR](1) 
	,[lfpdc2] [NVARCHAR](1) 
	,[lfpdc3] [NVARCHAR](1) 
	,[lfpdc4] [NVARCHAR](1) 
	,[lfidc1] [NVARCHAR](1) 
	,[lfidc2] [NVARCHAR](1) 
	,[lfidc3] [NVARCHAR](1) 
	,[lfidc4] [NVARCHAR](1) 
	,[lfcsj] [NVARCHAR](1) 
	,[lfdcto] [NVARCHAR](2) 
	,[lfart] [NVARCHAR](1) 
	,[lfaft] [NVARCHAR](1) 
	,[lfgwo] [NVARCHAR](1) 
	,[lfexvr] [NVARCHAR](1) 
	,[lfcmi] [NVARCHAR](1) 
	,[lfprrq] [NVARCHAR](1) 
	,[lfev01] [NVARCHAR](1) 
	,[lfev02] [NVARCHAR](1) 
	,[lfev03] [NVARCHAR](1) 
	,[lfev04] [NVARCHAR](1) 
	,[lfev05] [NVARCHAR](1) 
	,[lfnewr] [NVARCHAR](1) 
	,[lfsruf] [NVARCHAR](1) 
	,[lfnbru] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f40205/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
