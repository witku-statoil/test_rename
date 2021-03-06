--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4801t_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4801t_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4801t_init
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
	,[wadoco] [DECIMAL](38, 4) 
	,[waline] [NVARCHAR](12) 
	,[wamwdh] [NVARCHAR](1) 
	,[wascsp] [DECIMAL](38, 4) 
	,[washft] [NVARCHAR](1) 
	,[wasrcn] [NVARCHAR](1) 
	,[waledg] [NVARCHAR](2) 
	,[wacts4] [DECIMAL](38, 4) 
	,[wacts7] [DECIMAL](38, 4) 
	,[wacts8] [DECIMAL](38, 4) 
	,[waaid] [NVARCHAR](8) 
	,[waalse] [NVARCHAR](1) 
	,[waasid] [NVARCHAR](25) 
	,[waatst] [VARCHAR](20) 
	,[wabseq] [DECIMAL](38, 4) 
	,[wachpr] [NVARCHAR](1) 
	,[wacrcd] [NVARCHAR](3) 
	,[wacrce] [NVARCHAR](3) 
	,[wacrcf] [NVARCHAR](3) 
	,[wad5j] [VARCHAR](20) 
	,[wad6j] [VARCHAR](20) 
	,[wadraw] [NVARCHAR](20) 
	,[wadual] [NVARCHAR](1) 
	,[wampce] [NVARCHAR](1) 
	,[wamprc] [NVARCHAR](1) 
	,[waobj] [NVARCHAR](6) 
	,[waotam] [DECIMAL](38, 4) 
	,[waprjm] [DECIMAL](38, 4) 
	,[waprrp] [NVARCHAR](1) 
	,[washpp] [NVARCHAR](1) 
	,[wasqor] [DECIMAL](38, 4) 
	,[wasrkf] [NVARCHAR](1) 
	,[wasrnk] [DECIMAL](38, 4) 
	,[wassoq] [DECIMAL](38, 4) 
	,[watraf] [NVARCHAR](1) 
	,[wauom2] [NVARCHAR](2) 
	,[wawr11] [NVARCHAR](3) 
	,[wawr12] [NVARCHAR](3) 
	,[wawr13] [NVARCHAR](3) 
	,[wawr14] [NVARCHAR](3) 
	,[wawr15] [NVARCHAR](3) 
	,[wawr16] [NVARCHAR](3) 
	,[wawr17] [NVARCHAR](3) 
	,[wawr18] [NVARCHAR](3) 
	,[wawr19] [NVARCHAR](3) 
	,[wawr20] [NVARCHAR](3) 
	,[wajbcd] [NVARCHAR](6) 
	,[wavfwo] [NVARCHAR](1) 
	,[wapmtn] [NVARCHAR](12) 
	,[waissue] [NVARCHAR](80) 
	,[waprodm] [NVARCHAR](8) 
	,[wawho2] [NVARCHAR](40) 
	,[waar1] [NVARCHAR](6) 
	,[waphn1] [NVARCHAR](20) 
	,[watmco] [DECIMAL](38, 4) 
	,[wamthpr] [NVARCHAR](1) 
	,[waentck] [NVARCHAR](3) 
	,[waryin] [NVARCHAR](1) 
	,[warstm] [DECIMAL](38, 4) 
	,[wactr] [NVARCHAR](3) 
	,[waregion] [NVARCHAR](3) 
	,[watxa1] [NVARCHAR](10) 
	,[waexr1] [NVARCHAR](2) 
	,[walngp] [NVARCHAR](2) 
	,[waglccv] [NVARCHAR](4) 
	,[waglcnc] [NVARCHAR](4) 
	,[wacovgr] [NVARCHAR](8) 
	,[waasn4] [NVARCHAR](8) 
	,[waasn2] [NVARCHAR](8) 
	,[wasest] [DECIMAL](38, 4) 
	,[waseet] [DECIMAL](38, 4) 
	,[wadsavname] [NVARCHAR](10) 
	,[watimezones] [NVARCHAR](2) 
	,[waprodf] [NVARCHAR](8) 
	,[wacslprt] [DECIMAL](38, 4) 
	,[wamcucsl] [NVARCHAR](12) 
	,[warlot] [NVARCHAR](30) 
	,[wafailcd] [NVARCHAR](8) 
	,[wafaildt] [VARCHAR](20) 
	,[wafailtm] [DECIMAL](38, 4) 
	,[warepdt] [VARCHAR](20) 
	,[wareptm] [DECIMAL](38, 4) 
	,[wavend] [DECIMAL](38, 4) 
	,[waan8as] [DECIMAL](38, 4) 
	,[waan8srm] [DECIMAL](38, 4) 
	,[wasryn] [NVARCHAR](1) 
	,[waentcks] [NVARCHAR](3) 
	,[wacurbalm1] [DECIMAL](38, 4) 
	,[wacurbalm2] [DECIMAL](38, 4) 
	,[wacurbalm3] [DECIMAL](38, 4) 
	,[wacrdc] [NVARCHAR](3) 
	,[wacrrm] [NVARCHAR](1) 
	,[wacrr] [DECIMAL](38, 4) 
	,[wavmrs31] [NVARCHAR](2) 
	,[wavmrs32] [NVARCHAR](8) 
	,[waseqn] [DECIMAL](38, 4) 
	,[waplmr] [DECIMAL](38, 4) 
	,[wapllb] [DECIMAL](38, 4) 
	,[waplos] [DECIMAL](38, 4) 
	,[wabgtc] [DECIMAL](38, 4) 
	,[watoem] [DECIMAL](38, 4) 
	,[watopl] [DECIMAL](38, 4) 
	,[waplsa] [DECIMAL](38, 4) 
	,[waplsu] [DECIMAL](38, 4) 
	,[waessa] [DECIMAL](38, 4) 
	,[waessu] [DECIMAL](38, 4) 
	,[waacsa] [DECIMAL](38, 4) 
	,[waacsu] [DECIMAL](38, 4) 
	,[waoacm] [DECIMAL](38, 4) 
	,[waracm] [DECIMAL](38, 4) 
	,[wahplf] [NVARCHAR](1) 
	,[wavmrs33] [NVARCHAR](10) 
	,[wascall] [NVARCHAR](1) 
	,[waisno] [DECIMAL](38, 4) 
	,[warmthd] [NVARCHAR](1) 
	,[wadoc] [DECIMAL](38, 4) 
	,[wadct] [NVARCHAR](2) 
	,[wakco] [NVARCHAR](5) 
	,[wacoch] [NVARCHAR](3) 
	,[walnid] [DECIMAL](38, 4) 
	,[wacrtu] [NVARCHAR](10) 
	,[waxevt] [NVARCHAR](1) 
	,[wamcult] [NVARCHAR](12) 
	,[wawschf] [NVARCHAR](1) 
	,[wadfmdp] [NVARCHAR](1) 
	,[wapmpn] [NVARCHAR](30) 
	,[wapns] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4801t/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
