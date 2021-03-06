--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f03b11_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f03b11_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f03b11_cdc
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
	,[rpdoc] [DECIMAL](38, 4) 
	,[rpdct] [NVARCHAR](2) 
	,[rpkco] [NVARCHAR](5) 
	,[rpsfx] [NVARCHAR](3) 
	,[rpan8] [DECIMAL](38, 4) 
	,[rpdgj] [VARCHAR](20) 
	,[rpdivj] [VARCHAR](20) 
	,[rpicut] [NVARCHAR](2) 
	,[rpicu] [DECIMAL](38, 4) 
	,[rpdicj] [VARCHAR](20) 
	,[rpfy] [DECIMAL](38, 4) 
	,[rpctry] [DECIMAL](38, 4) 
	,[rppn] [DECIMAL](38, 4) 
	,[rpco] [NVARCHAR](5) 
	,[rpglc] [NVARCHAR](4) 
	,[rpaid] [NVARCHAR](8) 
	,[rppa8] [DECIMAL](38, 4) 
	,[rpan8j] [DECIMAL](38, 4) 
	,[rppyr] [DECIMAL](38, 4) 
	,[rppost] [NVARCHAR](1) 
	,[rpistr] [NVARCHAR](1) 
	,[rpbalj] [NVARCHAR](1) 
	,[rppst] [NVARCHAR](1) 
	,[rpag] [DECIMAL](38, 4) 
	,[rpaap] [DECIMAL](38, 4) 
	,[rpadsc] [DECIMAL](38, 4) 
	,[rpadsa] [DECIMAL](38, 4) 
	,[rpatxa] [DECIMAL](38, 4) 
	,[rpatxn] [DECIMAL](38, 4) 
	,[rpstam] [DECIMAL](38, 4) 
	,[rpbcrc] [NVARCHAR](3) 
	,[rpcrrm] [NVARCHAR](1) 
	,[rpcrcd] [NVARCHAR](3) 
	,[rpcrr] [DECIMAL](38, 4) 
	,[rpdmcd] [NVARCHAR](1) 
	,[rpacr] [DECIMAL](38, 4) 
	,[rpfap] [DECIMAL](38, 4) 
	,[rpcds] [DECIMAL](38, 4) 
	,[rpcdsa] [DECIMAL](38, 4) 
	,[rpctxa] [DECIMAL](38, 4) 
	,[rpctxn] [DECIMAL](38, 4) 
	,[rpctam] [DECIMAL](38, 4) 
	,[rptxa1] [NVARCHAR](10) 
	,[rpexr1] [NVARCHAR](2) 
	,[rpdsvj] [VARCHAR](20) 
	,[rpglba] [NVARCHAR](8) 
	,[rpam] [NVARCHAR](1) 
	,[rpaid2] [NVARCHAR](8) 
	,[rpam2] [NVARCHAR](1) 
	,[rpmcu] [NVARCHAR](12) 
	,[rpobj] [NVARCHAR](6) 
	,[rpsub] [NVARCHAR](8) 
	,[rpsblt] [NVARCHAR](1) 
	,[rpsbl] [NVARCHAR](8) 
	,[rpptc] [NVARCHAR](3) 
	,[rpddj] [VARCHAR](20) 
	,[rpddnj] [VARCHAR](20) 
	,[rprddj] [VARCHAR](20) 
	,[rprdsj] [VARCHAR](20) 
	,[rplfcj] [VARCHAR](20) 
	,[rpsmtj] [VARCHAR](20) 
	,[rpnbrr] [NVARCHAR](1) 
	,[rprdrl] [NVARCHAR](1) 
	,[rprmds] [DECIMAL](38, 4) 
	,[rpcoll] [NVARCHAR](1) 
	,[rpcorc] [NVARCHAR](2) 
	,[rpafc] [NVARCHAR](1) 
	,[rpdnlt] [NVARCHAR](1) 
	,[rprsco] [NVARCHAR](2) 
	,[rpodoc] [DECIMAL](38, 4) 
	,[rpodct] [NVARCHAR](2) 
	,[rpokco] [NVARCHAR](5) 
	,[rposfx] [NVARCHAR](3) 
	,[rpvinv] [NVARCHAR](25) 
	,[rppo] [NVARCHAR](8) 
	,[rppdct] [NVARCHAR](2) 
	,[rppkco] [NVARCHAR](5) 
	,[rpdcto] [NVARCHAR](2) 
	,[rplnid] [DECIMAL](38, 4) 
	,[rpsdoc] [DECIMAL](38, 4) 
	,[rpsdct] [NVARCHAR](2) 
	,[rpskco] [NVARCHAR](5) 
	,[rpsfxo] [NVARCHAR](3) 
	,[rpvldt] [VARCHAR](20) 
	,[rpcmc1] [DECIMAL](38, 4) 
	,[rpvr01] [NVARCHAR](25) 
	,[rpunit] [NVARCHAR](8) 
	,[rpmcu2] [NVARCHAR](12) 
	,[rprmk] [NVARCHAR](30) 
	,[rpalph] [NVARCHAR](40) 
	,[rprf] [NVARCHAR](2) 
	,[rpdrf] [DECIMAL](38, 4) 
	,[rpctl] [NVARCHAR](13) 
	,[rpfnlp] [NVARCHAR](1) 
	,[rpitm] [DECIMAL](38, 4) 
	,[rpu] [DECIMAL](38, 4) 
	,[rpum] [NVARCHAR](2) 
	,[rpalt6] [NVARCHAR](1) 
	,[rpryin] [NVARCHAR](1) 
	,[rpvdgj] [VARCHAR](20) 
	,[rpvod] [NVARCHAR](1) 
	,[rprp1] [NVARCHAR](1) 
	,[rprp2] [NVARCHAR](1) 
	,[rprp3] [NVARCHAR](1) 
	,[rpar01] [NVARCHAR](3) 
	,[rpar02] [NVARCHAR](3) 
	,[rpar03] [NVARCHAR](3) 
	,[rpar04] [NVARCHAR](3) 
	,[rpar05] [NVARCHAR](3) 
	,[rpar06] [NVARCHAR](3) 
	,[rpar07] [NVARCHAR](3) 
	,[rpar08] [NVARCHAR](3) 
	,[rpar09] [NVARCHAR](3) 
	,[rpar10] [NVARCHAR](3) 
	,[rpistc] [NVARCHAR](1) 
	,[rppyid] [DECIMAL](38, 4) 
	,[rpurc1] [NVARCHAR](3) 
	,[rpurdt] [VARCHAR](20) 
	,[rpurat] [DECIMAL](38, 4) 
	,[rpurab] [DECIMAL](38, 4) 
	,[rpurrf] [NVARCHAR](15) 
	,[rprnid] [DECIMAL](38, 4) 
	,[rpppdi] [VARCHAR](20) 
	,[rptorg] [NVARCHAR](10) 
	,[rpuser] [NVARCHAR](10) 
	,[rpjcl] [VARCHAR](20) 
	,[rppid] [NVARCHAR](10) 
	,[rpupmj] [VARCHAR](20) 
	,[rpupmt] [DECIMAL](38, 4) 
	,[rpddex] [NVARCHAR](2) 
	,[rpjobn] [NVARCHAR](10) 
	,[rphcrr] [DECIMAL](38, 4) 
	,[rphdgj] [VARCHAR](20) 
	,[rpshpn] [DECIMAL](38, 4) 
	,[rpdtxs] [NVARCHAR](1) 
	,[rpomod] [NVARCHAR](1) 
	,[rpclmg] [NVARCHAR](10) 
	,[rpcmgr] [NVARCHAR](10) 
	,[rpatad] [DECIMAL](38, 4) 
	,[rpctad] [DECIMAL](38, 4) 
	,[rpnrta] [DECIMAL](38, 4) 
	,[rpfnrt] [DECIMAL](38, 4) 
	,[rpprgf] [NVARCHAR](1) 
	,[rpgfl1] [NVARCHAR](1) 
	,[rpgfl2] [NVARCHAR](1) 
	,[rpdoco] [DECIMAL](38, 4) 
	,[rpkcoo] [NVARCHAR](5) 
	,[rpsotf] [NVARCHAR](1) 
	,[rpdtpb] [VARCHAR](20) 
	,[rperdj] [VARCHAR](20) 
	,[rppwpg] [DECIMAL](38, 4) 
	,[rpnettcid] [DECIMAL](38, 4) 
	,[rpnetdoc] [DECIMAL](38, 4) 
	,[rpnetrc5] [DECIMAL](38, 4) 
	,[rpnetst] [NVARCHAR](1) 
	,[rpajcl] [VARCHAR](20) 
	,[rprmr1] [NVARCHAR](50) 
)
WITH
(
    LOCATION='processing-clean/jde/f03b11/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
