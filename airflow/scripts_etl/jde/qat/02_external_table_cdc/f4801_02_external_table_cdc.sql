--Create ext table for cdc
IF OBJECT_ID('stg_jde_qat.ext_f4801_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4801_cdc


CREATE EXTERNAL TABLE stg_jde_qat.ext_f4801_cdc
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
	,[wadcto] [NVARCHAR](2) 
	,[wadoco] [DECIMAL](38, 4) 
	,[wasfxo] [NVARCHAR](3) 
	,[warcto] [NVARCHAR](2) 
	,[warorn] [NVARCHAR](8) 
	,[walnid] [DECIMAL](38, 4) 
	,[waptwo] [DECIMAL](38, 4) 
	,[wapars] [NVARCHAR](8) 
	,[watyps] [NVARCHAR](1) 
	,[waprts] [NVARCHAR](1) 
	,[wadl01] [NVARCHAR](30) 
	,[wastcm] [NVARCHAR](30) 
	,[waco] [NVARCHAR](5) 
	,[wamcu] [NVARCHAR](12) 
	,[wammcu] [NVARCHAR](12) 
	,[walocn] [NVARCHAR](20) 
	,[waaisl] [NVARCHAR](8) 
	,[wabin] [NVARCHAR](8) 
	,[wasrst] [NVARCHAR](2) 
	,[wadcg] [VARCHAR](20) 
	,[wasub] [NVARCHAR](8) 
	,[waan8] [DECIMAL](38, 4) 
	,[waano] [DECIMAL](38, 4) 
	,[waansa] [DECIMAL](38, 4) 
	,[waanpa] [DECIMAL](38, 4) 
	,[waanp] [DECIMAL](38, 4) 
	,[wadpl] [VARCHAR](20) 
	,[waant] [DECIMAL](38, 4) 
	,[wanan8] [DECIMAL](38, 4) 
	,[watrdj] [VARCHAR](20) 
	,[wastrt] [VARCHAR](20) 
	,[wadrqj] [VARCHAR](20) 
	,[wastrx] [VARCHAR](20) 
	,[wadap] [VARCHAR](20) 
	,[wadat] [VARCHAR](20) 
	,[wappdt] [VARCHAR](20) 
	,[wawr01] [NVARCHAR](4) 
	,[wawr02] [NVARCHAR](3) 
	,[wawr03] [NVARCHAR](3) 
	,[wawr04] [NVARCHAR](3) 
	,[wawr05] [NVARCHAR](3) 
	,[wawr06] [NVARCHAR](3) 
	,[wawr07] [NVARCHAR](3) 
	,[wawr08] [NVARCHAR](3) 
	,[wawr09] [NVARCHAR](3) 
	,[wawr10] [NVARCHAR](3) 
	,[wavr01] [NVARCHAR](25) 
	,[wavr02] [NVARCHAR](25) 
	,[waamto] [DECIMAL](38, 4) 
	,[wasetc] [DECIMAL](38, 4) 
	,[wabrt] [DECIMAL](38, 4) 
	,[wapayt] [NVARCHAR](5) 
	,[waamtc] [DECIMAL](38, 4) 
	,[wahrso] [DECIMAL](38, 4) 
	,[wahrsc] [DECIMAL](38, 4) 
	,[waamta] [DECIMAL](38, 4) 
	,[wahrsa] [DECIMAL](38, 4) 
	,[waitm] [DECIMAL](38, 4) 
	,[waaitm] [NVARCHAR](25) 
	,[walitm] [NVARCHAR](25) 
	,[wanumb] [DECIMAL](38, 4) 
	,[waapid] [NVARCHAR](12) 
	,[wauorg] [DECIMAL](38, 4) 
	,[wasobk] [DECIMAL](38, 4) 
	,[wasocn] [DECIMAL](38, 4) 
	,[wasoqs] [DECIMAL](38, 4) 
	,[waqtyt] [DECIMAL](38, 4) 
	,[wauom] [NVARCHAR](2) 
	,[washno] [NVARCHAR](10) 
	,[wapbtm] [DECIMAL](38, 4) 
	,[watbm] [NVARCHAR](3) 
	,[watrt] [NVARCHAR](3) 
	,[washty] [NVARCHAR](1) 
	,[wapec] [NVARCHAR](1) 
	,[wappfg] [NVARCHAR](1) 
	,[wabm] [NVARCHAR](1) 
	,[wartg] [NVARCHAR](1) 
	,[wasprt] [NVARCHAR](1) 
	,[wauncd] [NVARCHAR](1) 
	,[waindc] [NVARCHAR](1) 
	,[waresc] [DECIMAL](38, 4) 
	,[wamoh] [DECIMAL](38, 4) 
	,[watdt] [VARCHAR](20) 
	,[wapou] [DECIMAL](38, 4) 
	,[wapc] [DECIMAL](38, 4) 
	,[waltlv] [DECIMAL](38, 4) 
	,[waltcm] [DECIMAL](38, 4) 
	,[wacts1] [DECIMAL](38, 4) 
	,[walotn] [NVARCHAR](30) 
	,[walotp] [DECIMAL](38, 4) 
	,[walotg] [NVARCHAR](3) 
	,[warat1] [DECIMAL](38, 4) 
	,[warat2] [DECIMAL](38, 4) 
	,[wadct] [NVARCHAR](2) 
	,[wasbli] [NVARCHAR](1) 
	,[warkco] [NVARCHAR](5) 
	,[wabrev] [NVARCHAR](3) 
	,[warrev] [NVARCHAR](3) 
	,[wadrwc] [NVARCHAR](1) 
	,[wartch] [NVARCHAR](1) 
	,[wapnrq] [NVARCHAR](1) 
	,[wareas] [NVARCHAR](2) 
	,[waphse] [NVARCHAR](3) 
	,[waxdsp] [NVARCHAR](3) 
	,[wabomc] [NVARCHAR](1) 
	,[waurcd] [NVARCHAR](2) 
	,[waurdt] [VARCHAR](20) 
	,[waurat] [DECIMAL](38, 4) 
	,[waurab] [DECIMAL](38, 4) 
	,[waurrf] [NVARCHAR](15) 
	,[wauser] [NVARCHAR](10) 
	,[wapid] [NVARCHAR](10) 
	,[wajobn] [NVARCHAR](10) 
	,[waupmj] [VARCHAR](20) 
	,[watday] [DECIMAL](38, 4) 
	,[waaaid] [DECIMAL](38, 4) 
	,[wantst] [NVARCHAR](2) 
	,[waxrto] [DECIMAL](38, 4) 
	,[waesdn] [DECIMAL](38, 4) 
	,[waacdn] [DECIMAL](38, 4) 
	,[wasaid] [DECIMAL](38, 4) 
	,[wampos] [DECIMAL](38, 4) 
	,[waaprt] [NVARCHAR](3) 
	,[waamlc] [DECIMAL](38, 4) 
	,[waammc] [DECIMAL](38, 4) 
	,[waamot] [DECIMAL](38, 4) 
	,[walbam] [DECIMAL](38, 4) 
	,[wamtam] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4801/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
