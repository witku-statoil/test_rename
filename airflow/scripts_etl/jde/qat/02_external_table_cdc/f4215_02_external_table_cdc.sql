--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4215_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4215_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4215_cdc
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
	,[xhshpn] [DECIMAL](38, 4) 
	,[xhssts] [NVARCHAR](2) 
	,[xhpnid] [NVARCHAR](15) 
	,[xhhlcf] [NVARCHAR](18) 
	,[xhpkcd] [NVARCHAR](5) 
	,[xhwgq] [NVARCHAR](2) 
	,[xhwgts] [DECIMAL](38, 4) 
	,[xhwgtu] [NVARCHAR](2) 
	,[xhidq1] [NVARCHAR](2) 
	,[xhid1] [NVARCHAR](17) 
	,[xhidq2] [NVARCHAR](2) 
	,[xhid2] [NVARCHAR](17) 
	,[xhmot] [NVARCHAR](3) 
	,[xhrote] [NVARCHAR](35) 
	,[xheqcd] [NVARCHAR](2) 
	,[xheqin] [NVARCHAR](4) 
	,[xheqnb] [NVARCHAR](10) 
	,[xhasna] [NVARCHAR](2) 
	,[xhasnd] [VARCHAR](20) 
	,[xhasnt] [DECIMAL](38, 4) 
	,[xhmcu] [NVARCHAR](12) 
	,[xhnmcu] [NVARCHAR](12) 
	,[xhorgn] [DECIMAL](38, 4) 
	,[xhsrco] [NVARCHAR](1) 
	,[xhbpfg] [NVARCHAR](1) 
	,[xhaexp] [DECIMAL](38, 4) 
	,[xhecst] [DECIMAL](38, 4) 
	,[xhdrqj] [VARCHAR](20) 
	,[xhdrqt] [DECIMAL](38, 4) 
	,[xhrsdj] [VARCHAR](20) 
	,[xhrsdt] [DECIMAL](38, 4) 
	,[xhpdov] [NVARCHAR](1) 
	,[xhan8] [DECIMAL](38, 4) 
	,[xhshan] [DECIMAL](38, 4) 
	,[xhcty1] [NVARCHAR](25) 
	,[xhadds] [NVARCHAR](3) 
	,[xhaddz] [NVARCHAR](12) 
	,[xhctr] [NVARCHAR](3) 
	,[xhzon] [NVARCHAR](3) 
	,[xhsct1] [NVARCHAR](3) 
	,[xhsct2] [NVARCHAR](3) 
	,[xhsct3] [NVARCHAR](3) 
	,[xhcar1] [DECIMAL](38, 4) 
	,[xhcar2] [DECIMAL](38, 4) 
	,[xhcar3] [DECIMAL](38, 4) 
	,[xhilel] [NVARCHAR](1) 
	,[xhfrth] [NVARCHAR](3) 
	,[xhfrsc] [NVARCHAR](8) 
	,[xhdllv] [NVARCHAR](1) 
	,[xhrslt] [NVARCHAR](1) 
	,[xhbfsd] [NVARCHAR](1) 
	,[xhdstn] [DECIMAL](38, 4) 
	,[xhumd1] [NVARCHAR](2) 
	,[xhnrts] [DECIMAL](38, 4) 
	,[xhuser] [NVARCHAR](10) 
	,[xhpid] [NVARCHAR](10) 
	,[xhjobn] [NVARCHAR](10) 
	,[xhupmj] [VARCHAR](20) 
	,[xhtday] [DECIMAL](38, 4) 
	,[xhctyo] [NVARCHAR](25) 
	,[xhadso] [NVARCHAR](3) 
	,[xhadzo] [NVARCHAR](12) 
	,[xhctro] [NVARCHAR](3) 
	,[xhsc1o] [NVARCHAR](3) 
	,[xhsc2o] [NVARCHAR](3) 
	,[xhsc3o] [NVARCHAR](3) 
	,[xhaetc] [NVARCHAR](10) 
	,[xhetrsc] [NVARCHAR](3) 
	,[xhetrc] [NVARCHAR](3) 
	,[xhurcd] [NVARCHAR](2) 
	,[xhurdt] [VARCHAR](20) 
	,[xhurat] [DECIMAL](38, 4) 
	,[xhurab] [DECIMAL](38, 4) 
	,[xhurrf] [NVARCHAR](15) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4215/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
