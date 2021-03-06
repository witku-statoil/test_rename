--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f4102_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f4102_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f4102_cdc
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
	,[ibitm] [DECIMAL](38, 4) 
	,[iblitm] [NVARCHAR](25) 
	,[ibaitm] [NVARCHAR](25) 
	,[ibmcu] [NVARCHAR](12) 
	,[ibsrp1] [NVARCHAR](3) 
	,[ibsrp2] [NVARCHAR](3) 
	,[ibsrp3] [NVARCHAR](3) 
	,[ibsrp4] [NVARCHAR](3) 
	,[ibsrp5] [NVARCHAR](3) 
	,[ibsrp6] [NVARCHAR](6) 
	,[ibsrp7] [NVARCHAR](6) 
	,[ibsrp8] [NVARCHAR](6) 
	,[ibsrp9] [NVARCHAR](6) 
	,[ibsrp0] [NVARCHAR](6) 
	,[ibprp1] [NVARCHAR](3) 
	,[ibprp2] [NVARCHAR](3) 
	,[ibprp3] [NVARCHAR](3) 
	,[ibprp4] [NVARCHAR](3) 
	,[ibprp5] [NVARCHAR](3) 
	,[ibprp6] [NVARCHAR](6) 
	,[ibprp7] [NVARCHAR](6) 
	,[ibprp8] [NVARCHAR](6) 
	,[ibprp9] [NVARCHAR](6) 
	,[ibprp0] [NVARCHAR](6) 
	,[ibcdcd] [NVARCHAR](15) 
	,[ibpdgr] [NVARCHAR](3) 
	,[ibdsgp] [NVARCHAR](3) 
	,[ibvend] [DECIMAL](38, 4) 
	,[ibanpl] [DECIMAL](38, 4) 
	,[ibbuyr] [DECIMAL](38, 4) 
	,[ibglpt] [NVARCHAR](4) 
	,[iborig] [NVARCHAR](3) 
	,[ibropi] [DECIMAL](38, 4) 
	,[ibroqi] [DECIMAL](38, 4) 
	,[ibrqmx] [DECIMAL](38, 4) 
	,[ibrqmn] [DECIMAL](38, 4) 
	,[ibwomo] [DECIMAL](38, 4) 
	,[ibserv] [DECIMAL](38, 4) 
	,[ibsafe] [DECIMAL](38, 4) 
	,[ibsld] [DECIMAL](38, 4) 
	,[ibckav] [NVARCHAR](1) 
	,[ibsrce] [NVARCHAR](1) 
	,[iblots] [NVARCHAR](1) 
	,[ibot1y] [NVARCHAR](1) 
	,[ibot2y] [NVARCHAR](1) 
	,[ibstdp] [DECIMAL](38, 4) 
	,[ibfrmp] [DECIMAL](38, 4) 
	,[ibthrp] [DECIMAL](38, 4) 
	,[ibstdg] [NVARCHAR](3) 
	,[ibfrgd] [NVARCHAR](3) 
	,[ibthgd] [NVARCHAR](3) 
	,[ibcoty] [NVARCHAR](1) 
	,[ibmmpc] [DECIMAL](38, 4) 
	,[ibprgr] [NVARCHAR](8) 
	,[ibrprc] [NVARCHAR](8) 
	,[iborpr] [NVARCHAR](8) 
	,[ibback] [NVARCHAR](1) 
	,[ibifla] [NVARCHAR](2) 
	,[ibabcs] [NVARCHAR](1) 
	,[ibabcm] [NVARCHAR](1) 
	,[ibabci] [NVARCHAR](1) 
	,[ibovr] [NVARCHAR](1) 
	,[ibshcm] [NVARCHAR](3) 
	,[ibcars] [DECIMAL](38, 4) 
	,[ibcarp] [DECIMAL](38, 4) 
	,[ibshcn] [NVARCHAR](3) 
	,[ibstkt] [NVARCHAR](1) 
	,[iblnty] [NVARCHAR](2) 
	,[ibfifo] [NVARCHAR](1) 
	,[ibcycl] [NVARCHAR](3) 
	,[ibinmg] [NVARCHAR](10) 
	,[ibwarr] [NVARCHAR](8) 
	,[ibsrnr] [NVARCHAR](1) 
	,[ibpctm] [DECIMAL](38, 4) 
	,[ibcmcg] [NVARCHAR](8) 
	,[ibfuf1] [NVARCHAR](1) 
	,[ibtx] [NVARCHAR](1) 
	,[ibtax1] [NVARCHAR](1) 
	,[ibmpst] [NVARCHAR](1) 
	,[ibmrpd] [NVARCHAR](1) 
	,[ibmrpc] [NVARCHAR](1) 
	,[ibupc] [DECIMAL](38, 4) 
	,[ibsns] [NVARCHAR](1) 
	,[ibmerl] [NVARCHAR](3) 
	,[ibltlv] [DECIMAL](38, 4) 
	,[ibltmf] [DECIMAL](38, 4) 
	,[ibltcm] [DECIMAL](38, 4) 
	,[ibopc] [NVARCHAR](1) 
	,[ibopv] [DECIMAL](38, 4) 
	,[ibacq] [DECIMAL](38, 4) 
	,[ibmlq] [DECIMAL](38, 4) 
	,[ibltpu] [DECIMAL](38, 4) 
	,[ibmpsp] [NVARCHAR](1) 
	,[ibmrpp] [NVARCHAR](1) 
	,[ibitc] [NVARCHAR](1) 
	,[ibeco] [NVARCHAR](12) 
	,[ibecty] [NVARCHAR](2) 
	,[ibecod] [VARCHAR](20) 
	,[ibmtf1] [DECIMAL](38, 4) 
	,[ibmtf2] [DECIMAL](38, 4) 
	,[ibmtf3] [DECIMAL](38, 4) 
	,[ibmtf4] [DECIMAL](38, 4) 
	,[ibmtf5] [DECIMAL](38, 4) 
	,[ibmovd] [DECIMAL](38, 4) 
	,[ibqued] [DECIMAL](38, 4) 
	,[ibsetl] [DECIMAL](38, 4) 
	,[ibsrnk] [DECIMAL](38, 4) 
	,[ibsrkf] [NVARCHAR](1) 
	,[ibtimb] [NVARCHAR](1) 
	,[ibbqty] [DECIMAL](38, 4) 
	,[ibordw] [NVARCHAR](1) 
	,[ibexpd] [DECIMAL](38, 4) 
	,[ibdefd] [DECIMAL](38, 4) 
	,[ibmult] [DECIMAL](38, 4) 
	,[ibsflt] [DECIMAL](38, 4) 
	,[ibmake] [NVARCHAR](1) 
	,[iblfdj] [VARCHAR](20) 
	,[ibllx] [DECIMAL](38, 4) 
	,[ibcmgl] [NVARCHAR](1) 
	,[iburcd] [NVARCHAR](2) 
	,[iburdt] [VARCHAR](20) 
	,[iburat] [DECIMAL](38, 4) 
	,[iburab] [DECIMAL](38, 4) 
	,[iburrf] [NVARCHAR](15) 
	,[ibuser] [NVARCHAR](10) 
	,[ibpid] [NVARCHAR](10) 
	,[ibjobn] [NVARCHAR](10) 
	,[ibupmj] [VARCHAR](20) 
	,[ibtday] [DECIMAL](38, 4) 
	,[ibtfla] [NVARCHAR](2) 
	,[ibcomh] [DECIMAL](38, 4) 
	,[ibavrt] [DECIMAL](38, 4) 
	,[ibpoc] [NVARCHAR](1) 
	,[ibaing] [NVARCHAR](1) 
	,[ibbbdd] [DECIMAL](38, 4) 
	,[ibcmdm] [NVARCHAR](1) 
	,[iblecm] [NVARCHAR](1) 
	,[ibledd] [DECIMAL](38, 4) 
	,[ibmlot] [NVARCHAR](1) 
	,[ibpefd] [DECIMAL](38, 4) 
	,[ibsbdd] [DECIMAL](38, 4) 
	,[ibu1dd] [DECIMAL](38, 4) 
	,[ibu2dd] [DECIMAL](38, 4) 
	,[ibu3dd] [DECIMAL](38, 4) 
	,[ibu4dd] [DECIMAL](38, 4) 
	,[ibu5dd] [DECIMAL](38, 4) 
	,[ibxdck] [NVARCHAR](1) 
	,[iblaf] [NVARCHAR](1) 
	,[ibltfm] [NVARCHAR](1) 
	,[ibrwla] [NVARCHAR](1) 
	,[iblnpa] [NVARCHAR](1) 
	,[iblotc] [NVARCHAR](3) 
	,[ibapsc] [NVARCHAR](1) 
	,[ibpri1] [DECIMAL](38, 4) 
	,[ibpri2] [DECIMAL](38, 4) 
	,[ibltcv] [DECIMAL](38, 4) 
	,[ibashl] [NVARCHAR](1) 
	,[ibopth] [DECIMAL](38, 4) 
	,[ibcuth] [DECIMAL](38, 4) 
	,[ibumth] [NVARCHAR](3) 
	,[iblmfg] [NVARCHAR](1) 
	,[ibline] [NVARCHAR](12) 
	,[ibdftpct] [DECIMAL](38, 4) 
	,[ibkbit] [NVARCHAR](1) 
	,[ibdfenditm] [NVARCHAR](1) 
	,[ibkanexll] [NVARCHAR](1) 
	,[ibscpsell] [NVARCHAR](1) 
	,[ibmopth] [DECIMAL](38, 4) 
	,[ibmcuth] [DECIMAL](38, 4) 
	,[ibcumth] [NVARCHAR](3) 
	,[ibatprn] [NVARCHAR](80) 
	,[ibatpca] [NVARCHAR](1) 
	,[ibatpac] [NVARCHAR](5) 
	,[ibcoore] [NVARCHAR](1) 
	,[ibvcpfc] [NVARCHAR](20) 
	,[ibpnyn] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f4102/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
