--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f41011_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f41011_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f41011_cdc
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
	,[pkitm] [DECIMAL](38, 4) 
	,[pkpdgr] [NVARCHAR](3) 
	,[pkdsgp] [NVARCHAR](3) 
	,[pkdntb] [NVARCHAR](4) 
	,[pkstcn] [NVARCHAR](4) 
	,[pkrptm] [NVARCHAR](4) 
	,[pkrqtc] [NVARCHAR](1) 
	,[pklpgp] [NVARCHAR](1) 
	,[pkcavp] [NVARCHAR](1) 
	,[pkdnty] [DECIMAL](38, 4) 
	,[pkdntp] [NVARCHAR](1) 
	,[pkdetp] [DECIMAL](38, 4) 
	,[pkdtpu] [NVARCHAR](1) 
	,[pkcoex] [DECIMAL](38, 4) 
	,[pktmmn] [DECIMAL](38, 4) 
	,[pktpum] [NVARCHAR](1) 
	,[pktmmx] [DECIMAL](38, 4) 
	,[pktpux] [NVARCHAR](1) 
	,[pkdsmn] [DECIMAL](38, 4) 
	,[pkdntm] [NVARCHAR](1) 
	,[pkdnmx] [DECIMAL](38, 4) 
	,[pkdntx] [NVARCHAR](1) 
	,[pklpgv] [DECIMAL](38, 4) 
	,[pktpu1] [NVARCHAR](1) 
	,[pkmnvc] [DECIMAL](38, 4) 
	,[pkmxvc] [DECIMAL](38, 4) 
	,[pkurcd] [NVARCHAR](2) 
	,[pkurat] [DECIMAL](38, 4) 
	,[pkurab] [DECIMAL](38, 4) 
	,[pkurrf] [NVARCHAR](15) 
	,[pkurdt] [VARCHAR](20) 
	,[pkuser] [NVARCHAR](10) 
	,[pkpid] [NVARCHAR](10) 
	,[pkjobn] [NVARCHAR](10) 
	,[pkupmj] [VARCHAR](20) 
	,[pktday] [DECIMAL](38, 4) 
	,[pkrtob] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f41011/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
