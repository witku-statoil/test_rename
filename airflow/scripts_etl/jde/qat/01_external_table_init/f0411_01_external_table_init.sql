--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f0411_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f0411_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f0411_init
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
	,[rpkco] [NVARCHAR](5) 
	,[rpdoc] [DECIMAL](38, 4) 
	,[rpdct] [NVARCHAR](2) 
	,[rpsfx] [NVARCHAR](3) 
	,[rpsfxe] [DECIMAL](38, 4) 
	,[rpdcta] [NVARCHAR](2) 
	,[rpan8] [DECIMAL](38, 4) 
	,[rppye] [DECIMAL](38, 4) 
	,[rpsnto] [DECIMAL](38, 4) 
	,[rpdivj] [VARCHAR](20) 
	,[rpdsvj] [VARCHAR](20) 
	,[rpddj] [VARCHAR](20) 
	,[rpddnj] [VARCHAR](20) 
	,[rpdgj] [VARCHAR](20) 
	,[rpfy] [DECIMAL](38, 4) 
	,[rpctry] [DECIMAL](38, 4) 
	,[rppn] [DECIMAL](38, 4) 
	,[rpco] [NVARCHAR](5) 
	,[rpicu] [DECIMAL](38, 4) 
	,[rpicut] [NVARCHAR](2) 
	,[rpdicj] [VARCHAR](20) 
	,[rpbalj] [NVARCHAR](1) 
	,[rppst] [NVARCHAR](1) 
	,[rpag] [DECIMAL](38, 4) 
	,[rpaap] [DECIMAL](38, 4) 
	,[rpadsc] [DECIMAL](38, 4) 
	,[rpadsa] [DECIMAL](38, 4) 
	,[rpatxa] [DECIMAL](38, 4) 
	,[rpatxn] [DECIMAL](38, 4) 
	,[rpstam] [DECIMAL](38, 4) 
	,[rptxa1] [NVARCHAR](10) 
	,[rpexr1] [NVARCHAR](2) 
	,[rpcrrm] [NVARCHAR](1) 
	,[rpcrcd] [NVARCHAR](3) 
	,[rpcrr] [DECIMAL](38, 4) 
	,[rpacr] [DECIMAL](38, 4) 
	,[rpfap] [DECIMAL](38, 4) 
	,[rpcds] [DECIMAL](38, 4) 
	,[rpcdsa] [DECIMAL](38, 4) 
	,[rpctxa] [DECIMAL](38, 4) 
	,[rpctxn] [DECIMAL](38, 4) 
	,[rpctam] [DECIMAL](38, 4) 
	,[rpglc] [NVARCHAR](4) 
	,[rpglba] [NVARCHAR](8) 
	,[rppost] [NVARCHAR](1) 
	,[rpam] [NVARCHAR](1) 
	,[rpaid2] [NVARCHAR](8) 
	,[rpmcu] [NVARCHAR](12) 
	,[rpobj] [NVARCHAR](6) 
	,[rpsub] [NVARCHAR](8) 
	,[rpsblt] [NVARCHAR](1) 
	,[rpsbl] [NVARCHAR](8) 
	,[rpbaid] [NVARCHAR](8) 
	,[rpptc] [NVARCHAR](3) 
	,[rpvod] [NVARCHAR](1) 
	,[rpokco] [NVARCHAR](5) 
	,[rpodct] [NVARCHAR](2) 
	,[rpodoc] [DECIMAL](38, 4) 
	,[rposfx] [NVARCHAR](3) 
	,[rpcrc] [NVARCHAR](3) 
	,[rpvinv] [NVARCHAR](25) 
	,[rppkco] [NVARCHAR](5) 
	,[rppo] [NVARCHAR](8) 
	,[rppdct] [NVARCHAR](2) 
	,[rplnid] [DECIMAL](38, 4) 
	,[rpsfxo] [NVARCHAR](3) 
	,[rpopsq] [DECIMAL](38, 4) 
	,[rpvr01] [NVARCHAR](25) 
	,[rpunit] [NVARCHAR](8) 
	,[rpmcu2] [NVARCHAR](12) 
	,[rprmk] [NVARCHAR](30) 
	,[rprf] [NVARCHAR](2) 
	,[rpdrf] [DECIMAL](38, 4) 
	,[rpctl] [NVARCHAR](13) 
	,[rpfnlp] [NVARCHAR](1) 
	,[rpu] [DECIMAL](38, 4) 
	,[rpum] [NVARCHAR](2) 
	,[rppyin] [NVARCHAR](1) 
	,[rptxa3] [NVARCHAR](10) 
	,[rpexr3] [NVARCHAR](2) 
	,[rprp1] [NVARCHAR](1) 
	,[rprp2] [NVARCHAR](1) 
	,[rprp3] [NVARCHAR](1) 
	,[rpac07] [NVARCHAR](3) 
	,[rptnn] [NVARCHAR](1) 
	,[rpdmcd] [NVARCHAR](1) 
	,[rpitm] [DECIMAL](38, 4) 
	,[rphcrr] [DECIMAL](38, 4) 
	,[rphdgj] [VARCHAR](20) 
	,[rpurc1] [NVARCHAR](3) 
	,[rpurdt] [VARCHAR](20) 
	,[rpurat] [DECIMAL](38, 4) 
	,[rpurab] [DECIMAL](38, 4) 
	,[rpurrf] [NVARCHAR](15) 
	,[rptorg] [NVARCHAR](10) 
	,[rpuser] [NVARCHAR](10) 
	,[rppid] [NVARCHAR](10) 
	,[rpupmj] [VARCHAR](20) 
	,[rpupmt] [DECIMAL](38, 4) 
	,[rpjobn] [NVARCHAR](10) 
	,[rptnst] [NVARCHAR](20) 
	,[rpyc01] [NVARCHAR](3) 
	,[rpyc02] [NVARCHAR](3) 
	,[rpyc03] [NVARCHAR](3) 
	,[rpyc04] [NVARCHAR](3) 
	,[rpyc05] [NVARCHAR](3) 
	,[rpyc06] [NVARCHAR](3) 
	,[rpyc07] [NVARCHAR](3) 
	,[rpyc08] [NVARCHAR](3) 
	,[rpyc09] [NVARCHAR](3) 
	,[rpyc10] [NVARCHAR](3) 
	,[rpdtxs] [NVARCHAR](1) 
	,[rpbcrc] [NVARCHAR](3) 
	,[rpatad] [DECIMAL](38, 4) 
	,[rpctad] [DECIMAL](38, 4) 
	,[rpnrta] [DECIMAL](38, 4) 
	,[rpfnrt] [DECIMAL](38, 4) 
	,[rptaxp] [NVARCHAR](1) 
	,[rpprgf] [NVARCHAR](1) 
	,[rpgfl5] [NVARCHAR](1) 
	,[rpgfl6] [NVARCHAR](1) 
	,[rpgam1] [DECIMAL](38, 4) 
	,[rpgam2] [DECIMAL](38, 4) 
	,[rpgen4] [NVARCHAR](25) 
	,[rpgen5] [NVARCHAR](25) 
	,[rpwtad] [DECIMAL](38, 4) 
	,[rpwtaf] [DECIMAL](38, 4) 
	,[rpsmmf] [NVARCHAR](1) 
	,[rppywp] [NVARCHAR](1) 
	,[rppwpg] [DECIMAL](38, 4) 
	,[rpnettcid] [DECIMAL](38, 4) 
	,[rpnetdoc] [DECIMAL](38, 4) 
	,[rpnetrc5] [DECIMAL](38, 4) 
	,[rpnetst] [NVARCHAR](1) 
	,[rpcntrtid] [DECIMAL](38, 4) 
	,[rpcntrtcd] [NVARCHAR](12) 
	,[rpwvid] [DECIMAL](38, 4) 
	,[rpblscd2] [NVARCHAR](10) 
	,[rpharper] [NVARCHAR](6) 
	,[rpharsfx] [NVARCHAR](10) 
	,[rpddrl] [NVARCHAR](5) 
	,[rpseqn] [DECIMAL](38, 4) 
	,[rpclass01] [NVARCHAR](3) 
	,[rpclass02] [NVARCHAR](3) 
	,[rpclass03] [NVARCHAR](3) 
	,[rpclass04] [NVARCHAR](3) 
	,[rpclass05] [NVARCHAR](3) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f0411/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
