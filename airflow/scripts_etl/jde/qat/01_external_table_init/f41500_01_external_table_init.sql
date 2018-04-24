--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f41500_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f41500_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f41500_init
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
	,[ppmcu] [NVARCHAR](12) 
	,[pptkid] [NVARCHAR](8) 
	,[ppdl01] [NVARCHAR](30) 
	,[pptklo] [NVARCHAR](3) 
	,[pptuse] [NVARCHAR](3) 
	,[pptkty] [NVARCHAR](3) 
	,[pptkcp] [DECIMAL](38, 4) 
	,[ppbum1] [NVARCHAR](2) 
	,[pphttk] [NVARCHAR](1) 
	,[ppprez] [NVARCHAR](1) 
	,[ppidte] [VARCHAR](20) 
	,[ppdtcl] [VARCHAR](20) 
	,[ppstrp] [NVARCHAR](2) 
	,[ppdipt] [NVARCHAR](1) 
	,[ppgamt] [NVARCHAR](1) 
	,[ppfltr] [NVARCHAR](1) 
	,[pptexp] [DECIMAL](38, 4) 
	,[pptstu] [NVARCHAR](1) 
	,[ppcutk] [NVARCHAR](1) 
	,[ppscom] [NVARCHAR](1) 
	,[pptdia] [DECIMAL](38, 4) 
	,[ppbum2] [NVARCHAR](2) 
	,[ppthgt] [DECIMAL](38, 4) 
	,[ppbum3] [NVARCHAR](2) 
	,[ppstem] [DECIMAL](38, 4) 
	,[ppstpu] [NVARCHAR](1) 
	,[pprefh] [DECIMAL](38, 4) 
	,[ppbum4] [NVARCHAR](2) 
	,[pprwgh] [DECIMAL](38, 4) 
	,[ppbum5] [NVARCHAR](2) 
	,[ppflht] [DECIMAL](38, 4) 
	,[ppbum6] [NVARCHAR](2) 
	,[ppunpv] [DECIMAL](38, 4) 
	,[ppbum7] [NVARCHAR](2) 
	,[pppipv] [DECIMAL](38, 4) 
	,[ppbum8] [NVARCHAR](2) 
	,[ppdisv] [DECIMAL](38, 4) 
	,[ppbum9] [NVARCHAR](2) 
	,[ppdihr] [DECIMAL](38, 4) 
	,[ppdhru] [NVARCHAR](2) 
	,[ppfirh] [DECIMAL](38, 4) 
	,[ppfrhu] [NVARCHAR](2) 
	,[pplswn] [DECIMAL](38, 4) 
	,[ppbum0] [NVARCHAR](2) 
	,[pplosc] [DECIMAL](38, 4) 
	,[pphisc] [DECIMAL](38, 4) 
	,[ppitm] [DECIMAL](38, 4) 
	,[ppitml] [DECIMAL](38, 4) 
	,[pppcsd] [NVARCHAR](10) 
	,[pprtob] [NVARCHAR](1) 
	,[ppurcd] [NVARCHAR](2) 
	,[ppurat] [DECIMAL](38, 4) 
	,[ppurab] [DECIMAL](38, 4) 
	,[ppurrf] [NVARCHAR](15) 
	,[ppurdt] [VARCHAR](20) 
	,[ppuser] [NVARCHAR](10) 
	,[pppid] [NVARCHAR](10) 
	,[ppjobn] [NVARCHAR](10) 
	,[ppupmj] [VARCHAR](20) 
	,[pptday] [DECIMAL](38, 4) 
	,[pptnkn] [DECIMAL](38, 4) 
	,[ppexpf] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f41500/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)