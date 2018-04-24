--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f550005_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f550005_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f550005_cdc
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
	,[cmy55cmpcd] [NVARCHAR](16) 
	,[cmcmpid] [DECIMAL](38, 4) 
	,[cmy55cmpty] [NVARCHAR](8) 
	,[cmtxky] [NVARCHAR](254) 
	,[cmsdca] [VARCHAR](20) 
	,[cmedca] [VARCHAR](20) 
	,[cmctr] [NVARCHAR](3) 
	,[cmy55theme] [NVARCHAR](10) 
	,[cmcrcd] [NVARCHAR](3) 
	,[cmexdt0] [NVARCHAR](11) 
	,[cmexdt1] [NVARCHAR](11) 
	,[cmurab] [DECIMAL](38, 4) 
	,[cmurrf] [NVARCHAR](15) 
	,[cmurdt] [VARCHAR](20) 
	,[cmurat] [DECIMAL](38, 4) 
	,[cmurcd] [NVARCHAR](2) 
	,[cmcrtu] [NVARCHAR](10) 
	,[cmcrdj] [VARCHAR](20) 
	,[cmc9crp] [NVARCHAR](10) 
	,[cmupmj] [VARCHAR](20) 
	,[cmupmt] [DECIMAL](38, 4) 
	,[cmjobn] [NVARCHAR](10) 
	,[cmpid] [NVARCHAR](10) 
	,[cmuser] [NVARCHAR](10) 
	,[cmy55lckdt] [VARCHAR](20) 
	,[cmy55alapr] [NVARCHAR](1) 
	,[cmy55strg1] [NVARCHAR](30) 
	,[cmy55strg2] [NVARCHAR](30) 
	,[cmy55amnt1] [DECIMAL](38, 4) 
	,[cmy55amnt2] [DECIMAL](38, 4) 
	,[cmy55char1] [NVARCHAR](1) 
	,[cmy55char2] [NVARCHAR](1) 
	,[cmy55date1] [VARCHAR](20) 
	,[cmy55date2] [VARCHAR](20) 
	,[cmy55time1] [DECIMAL](38, 4) 
	,[cmy55time2] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f550005/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
