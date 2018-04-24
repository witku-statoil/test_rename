--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f3002_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f3002_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f3002_cdc
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
	,[ixtbm] [NVARCHAR](3) 
	,[ixkit] [DECIMAL](38, 4) 
	,[ixkitl] [NVARCHAR](25) 
	,[ixkita] [NVARCHAR](25) 
	,[ixmmcu] [NVARCHAR](12) 
	,[ixitm] [DECIMAL](38, 4) 
	,[ixlitm] [NVARCHAR](25) 
	,[ixaitm] [NVARCHAR](25) 
	,[ixcmcu] [NVARCHAR](12) 
	,[ixcpnt] [DECIMAL](38, 4) 
	,[ixsbnt] [DECIMAL](38, 4) 
	,[ixprta] [NVARCHAR](1) 
	,[ixqnty] [DECIMAL](38, 4) 
	,[ixum] [NVARCHAR](2) 
	,[ixbqty] [DECIMAL](38, 4) 
	,[ixuom] [NVARCHAR](2) 
	,[ixfvbt] [NVARCHAR](1) 
	,[ixefff] [VARCHAR](20) 
	,[ixefft] [VARCHAR](20) 
	,[ixfser] [NVARCHAR](25) 
	,[ixtser] [NVARCHAR](25) 
	,[ixitc] [NVARCHAR](1) 
	,[ixftrc] [NVARCHAR](1) 
	,[ixoptk] [NVARCHAR](1) 
	,[ixforv] [NVARCHAR](1) 
	,[ixcstm] [NVARCHAR](1) 
	,[ixcsmp] [NVARCHAR](1) 
	,[ixordw] [NVARCHAR](1) 
	,[ixforq] [NVARCHAR](1) 
	,[ixcoby] [NVARCHAR](1) 
	,[ixcoty] [NVARCHAR](1) 
	,[ixfrmp] [DECIMAL](38, 4) 
	,[ixthrp] [DECIMAL](38, 4) 
	,[ixfrgd] [NVARCHAR](3) 
	,[ixthgd] [NVARCHAR](3) 
	,[ixopsq] [DECIMAL](38, 4) 
	,[ixbseq] [DECIMAL](38, 4) 
	,[ixftrp] [DECIMAL](38, 4) 
	,[ixf_2rp] [DECIMAL](38, 4) 
	,[ixrscp] [DECIMAL](38, 4) 
	,[ixscrp] [DECIMAL](38, 4) 
	,[ixrewp] [DECIMAL](38, 4) 
	,[ixasip] [DECIMAL](38, 4) 
	,[ixcpyp] [DECIMAL](38, 4) 
	,[ixstpp] [DECIMAL](38, 4) 
	,[ixlovd] [DECIMAL](38, 4) 
	,[ixeco] [NVARCHAR](12) 
	,[ixecty] [NVARCHAR](2) 
	,[ixecod] [VARCHAR](20) 
	,[ixdsc1] [NVARCHAR](30) 
	,[ixlnty] [NVARCHAR](2) 
	,[ixpric] [DECIMAL](38, 4) 
	,[ixuncs] [DECIMAL](38, 4) 
	,[ixpctk] [DECIMAL](38, 4) 
	,[ixshno] [NVARCHAR](10) 
	,[ixomcu] [NVARCHAR](12) 
	,[ixobj] [NVARCHAR](6) 
	,[ixsub] [NVARCHAR](8) 
	,[ixbrev] [NVARCHAR](3) 
	,[ixcmrv] [NVARCHAR](3) 
	,[ixrvno] [NVARCHAR](2) 
	,[ixuupg] [DECIMAL](38, 4) 
	,[ixurcd] [NVARCHAR](2) 
	,[ixurdt] [VARCHAR](20) 
	,[ixurat] [DECIMAL](38, 4) 
	,[ixurrf] [NVARCHAR](15) 
	,[ixurab] [DECIMAL](38, 4) 
	,[ixuser] [NVARCHAR](10) 
	,[ixpid] [NVARCHAR](10) 
	,[ixjobn] [NVARCHAR](10) 
	,[ixupmj] [VARCHAR](20) 
	,[ixtday] [DECIMAL](38, 4) 
	,[ixaing] [NVARCHAR](1) 
	,[ixsuco] [VARCHAR](20) 
	,[ixstrc] [DECIMAL](38, 4) 
	,[ixendc] [DECIMAL](38, 4) 
	,[ixapsc] [NVARCHAR](1) 
	,[ixcpnb] [DECIMAL](38, 4) 
	,[ixbseqan] [NVARCHAR](5) 
	,[ixbchar] [NVARCHAR](1) 
	,[ixbostr] [NVARCHAR](4) 
)
WITH
(
    LOCATION='processing-clean/jde/f3002/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
