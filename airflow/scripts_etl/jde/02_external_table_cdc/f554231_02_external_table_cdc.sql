--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f554231_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f554231_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f554231_cdc
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
	,[szan83] [DECIMAL](38, 4) 
	,[szdoc] [DECIMAL](38, 4) 
	,[szaddj] [VARCHAR](20) 
	,[szcmtb] [VARCHAR](20) 
	,[szptmb] [DECIMAL](38, 4) 
	,[szcmte] [VARCHAR](20) 
	,[szptme] [DECIMAL](38, 4) 
	,[sztx1] [NVARCHAR](16) 
	,[szlnid] [DECIMAL](38, 4) 
	,[szaexp] [DECIMAL](38, 4) 
	,[szuprc] [DECIMAL](38, 4) 
	,[szecst] [DECIMAL](38, 4) 
	,[szlprc] [DECIMAL](38, 4) 
	,[sztcst] [DECIMAL](38, 4) 
	,[szaopn] [DECIMAL](38, 4) 
	,[szdl02] [NVARCHAR](30) 
	,[szdods] [NVARCHAR](30) 
	,[szcrcd] [NVARCHAR](3) 
	,[sztdty] [NVARCHAR](4) 
	,[sztx2] [NVARCHAR](20) 
	,[szan81] [DECIMAL](38, 4) 
	,[szan82] [DECIMAL](38, 4) 
	,[szafit] [NVARCHAR](1) 
	,[szgfl3] [NVARCHAR](1) 
	,[szlitm] [NVARCHAR](25) 
	,[szcsht] [NVARCHAR](5) 
	,[sztkid] [NVARCHAR](8) 
	,[szuorg] [DECIMAL](38, 4) 
	,[szukid] [DECIMAL](38, 4) 
	,[szctr] [NVARCHAR](3) 
	,[szaccd21] [NVARCHAR](10) 
	,[szshan] [DECIMAL](38, 4) 
	,[szan8] [DECIMAL](38, 4) 
	,[szvend] [DECIMAL](38, 4) 
	,[szemcu] [NVARCHAR](12) 
	,[szmcu] [NVARCHAR](12) 
	,[sza801] [NVARCHAR](8) 
	,[szlocn] [NVARCHAR](20) 
	,[szttem] [DECIMAL](38, 4) 
	,[szstpu] [NVARCHAR](1) 
	,[szdend] [DECIMAL](38, 4) 
	,[szdntp] [NVARCHAR](1) 
	,[szlnty] [NVARCHAR](2) 
	,[szgfl5] [NVARCHAR](1) 
	,[szgfl4] [NVARCHAR](1) 
	,[szgfl6] [NVARCHAR](1) 
	,[szekco] [NVARCHAR](5) 
	,[szedct] [NVARCHAR](2) 
	,[szedoc] [DECIMAL](38, 4) 
	,[szedln] [DECIMAL](38, 4) 
	,[szedsp] [NVARCHAR](1) 
	,[szokco] [NVARCHAR](5) 
	,[szoorn] [NVARCHAR](8) 
	,[szocto] [NVARCHAR](2) 
	,[szogno] [DECIMAL](38, 4) 
	,[szgfl7] [NVARCHAR](1) 
	,[szkcoo] [NVARCHAR](5) 
	,[szdoco] [DECIMAL](38, 4) 
	,[szcitm] [NVARCHAR](25) 
	,[szdcto] [NVARCHAR](2) 
	,[szlnic] [DECIMAL](38, 4) 
	,[szgfl8] [NVARCHAR](1) 
	,[szrkco] [NVARCHAR](5) 
	,[szrorn] [NVARCHAR](8) 
	,[szrcto] [NVARCHAR](2) 
	,[szrlln] [DECIMAL](38, 4) 
	,[szgfl2] [NVARCHAR](1) 
	,[szgflag1] [NVARCHAR](1) 
	,[szgflag2] [NVARCHAR](1) 
	,[szgflg] [NVARCHAR](1) 
	,[szdtbs] [NVARCHAR](1) 
	,[szbbdj] [VARCHAR](20) 
	,[szecod] [VARCHAR](20) 
	,[szedck] [NVARCHAR](1) 
	,[szurat] [DECIMAL](38, 4) 
	,[szurab] [DECIMAL](38, 4) 
	,[szurnum12] [DECIMAL](38, 4) 
	,[szurnum13] [DECIMAL](38, 4) 
	,[szaa] [DECIMAL](38, 4) 
	,[szaa1] [DECIMAL](38, 4) 
	,[szaa2] [DECIMAL](38, 4) 
	,[szaa3] [DECIMAL](38, 4) 
	,[szdsc] [NVARCHAR](30) 
	,[szdsc1] [NVARCHAR](30) 
	,[szdsc2] [NVARCHAR](30) 
	,[szdsc3] [NVARCHAR](30) 
	,[szdsc4] [NVARCHAR](30) 
	,[szdate01] [VARCHAR](20) 
	,[szdate02] [VARCHAR](20) 
	,[szdate03] [VARCHAR](20) 
	,[szdate04] [VARCHAR](20) 
	,[szcm01] [NVARCHAR](1) 
	,[szcm02] [NVARCHAR](1) 
	,[szcm03] [NVARCHAR](1) 
	,[szcm04] [NVARCHAR](1) 
	,[szcm05] [NVARCHAR](1) 
	,[szedtn] [NVARCHAR](22) 
	,[szeffutime] [NVARCHAR](11) 
	,[szeetm] [NVARCHAR](11) 
	,[szedbt] [NVARCHAR](15) 
	,[szgfl1] [NVARCHAR](1) 
	,[szaitm] [NVARCHAR](25) 
	,[szy55char1] [NVARCHAR](1) 
	,[szy55char2] [NVARCHAR](1) 
	,[szy55strg1] [NVARCHAR](30) 
	,[szy55strg2] [NVARCHAR](30) 
	,[szy55strg3] [NVARCHAR](30) 
	,[szy55strg4] [NVARCHAR](30) 
	,[szy55strg5] [NVARCHAR](30) 
	,[szy55amnt1] [DECIMAL](38, 4) 
	,[szy55amnt2] [DECIMAL](38, 4) 
	,[szaaq1] [DECIMAL](38, 4) 
	,[szaaq2] [DECIMAL](38, 4) 
	,[szaaq3] [DECIMAL](38, 4) 
	,[szaaq4] [DECIMAL](38, 4) 
	,[szy55time1] [DECIMAL](38, 4) 
	,[szy55time2] [DECIMAL](38, 4) 
	,[szy55date1] [VARCHAR](20) 
	,[szy55date2] [VARCHAR](20) 
	,[szuser] [NVARCHAR](10) 
	,[szupmj] [VARCHAR](20) 
	,[szupmt] [DECIMAL](38, 4) 
	,[szjobn] [NVARCHAR](10) 
	,[szpid] [NVARCHAR](10) 
)
WITH
(
    LOCATION='processing-clean/jde/f554231/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
