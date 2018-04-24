--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f03012_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f03012_init

CREATE EXTERNAL TABLE stg_jde.ext_f03012_init
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
	,[aian8] [DECIMAL](38, 4) 
	,[aico] [NVARCHAR](5) 
	,[aiarc] [NVARCHAR](4) 
	,[aimcur] [NVARCHAR](12) 
	,[aiobar] [NVARCHAR](6) 
	,[aiaidr] [NVARCHAR](8) 
	,[aikcor] [NVARCHAR](5) 
	,[aidcar] [DECIMAL](38, 4) 
	,[aidtar] [NVARCHAR](2) 
	,[aicrcd] [NVARCHAR](3) 
	,[aitxa1] [NVARCHAR](10) 
	,[aiexr1] [NVARCHAR](2) 
	,[aiacl] [DECIMAL](38, 4) 
	,[aihdar] [NVARCHAR](1) 
	,[aitrar] [NVARCHAR](3) 
	,[aistto] [NVARCHAR](1) 
	,[airyin] [NVARCHAR](1) 
	,[aistmt] [NVARCHAR](1) 
	,[aiarpy] [DECIMAL](38, 4) 
	,[aiatcs] [NVARCHAR](1) 
	,[aisito] [NVARCHAR](1) 
	,[aisqnl] [NVARCHAR](1) 
	,[aialgm] [NVARCHAR](2) 
	,[aicycn] [NVARCHAR](2) 
	,[aibo] [NVARCHAR](1) 
	,[aitsta] [NVARCHAR](2) 
	,[aickhc] [NVARCHAR](1) 
	,[aidlc] [VARCHAR](20) 
	,[aidnlt] [NVARCHAR](1) 
	,[aiplcr] [NVARCHAR](10) 
	,[airvdj] [VARCHAR](20) 
	,[aidso] [DECIMAL](38, 4) 
	,[aicmgr] [NVARCHAR](10) 
	,[aiclmg] [NVARCHAR](10) 
	,[aidlqt] [DECIMAL](38, 4) 
	,[aidlqj] [VARCHAR](20) 
	,[ainbrr] [NVARCHAR](1) 
	,[aicoll] [NVARCHAR](1) 
	,[ainbr1] [DECIMAL](38, 4) 
	,[ainbr2] [DECIMAL](38, 4) 
	,[ainbr3] [DECIMAL](38, 4) 
	,[ainbcl] [DECIMAL](38, 4) 
	,[aiafc] [NVARCHAR](1) 
	,[aifd] [DECIMAL](38, 4) 
	,[aifp] [DECIMAL](38, 4) 
	,[aicfce] [NVARCHAR](1) 
	,[aiab2] [NVARCHAR](1) 
	,[aidt1j] [VARCHAR](20) 
	,[aidfij] [VARCHAR](20) 
	,[aidlij] [VARCHAR](20) 
	,[aiabc1] [NVARCHAR](1) 
	,[aiabc2] [NVARCHAR](1) 
	,[aiabc3] [NVARCHAR](1) 
	,[aifndj] [VARCHAR](20) 
	,[aidlp] [VARCHAR](20) 
	,[aidb] [NVARCHAR](3) 
	,[aidnbj] [VARCHAR](20) 
	,[aitrw] [NVARCHAR](3) 
	,[aitwdj] [VARCHAR](20) 
	,[aiavd] [DECIMAL](38, 4) 
	,[aicrca] [NVARCHAR](3) 
	,[aiad] [DECIMAL](38, 4) 
	,[aiafcp] [DECIMAL](38, 4) 
	,[aiafcy] [DECIMAL](38, 4) 
	,[aiasty] [DECIMAL](38, 4) 
	,[aispye] [DECIMAL](38, 4) 
	,[aiahb] [DECIMAL](38, 4) 
	,[aialp] [DECIMAL](38, 4) 
	,[aiabam] [DECIMAL](38, 4) 
	,[aiaba1] [DECIMAL](38, 4) 
	,[aiaprc] [DECIMAL](38, 4) 
	,[aimaxo] [DECIMAL](38, 4) 
	,[aimino] [DECIMAL](38, 4) 
	,[aioytd] [DECIMAL](38, 4) 
	,[aiopy] [DECIMAL](38, 4) 
	,[aipopn] [NVARCHAR](10) 
	,[aidaoj] [VARCHAR](20) 
	,[aian8r] [DECIMAL](38, 4) 
	,[aibadt] [NVARCHAR](1) 
	,[aicpgp] [NVARCHAR](8) 
	,[aiortp] [NVARCHAR](8) 
	,[aitrdc] [DECIMAL](38, 4) 
	,[aiinmg] [NVARCHAR](10) 
	,[aiexhd] [NVARCHAR](1) 
	,[aihold] [NVARCHAR](2) 
	,[airout] [NVARCHAR](3) 
	,[aistop] [NVARCHAR](3) 
	,[aizon] [NVARCHAR](3) 
	,[aicars] [DECIMAL](38, 4) 
	,[aidel1] [NVARCHAR](30) 
	,[aidel2] [NVARCHAR](30) 
	,[ailtdt] [DECIMAL](38, 4) 
	,[aifrth] [NVARCHAR](3) 
	,[aiaft] [NVARCHAR](1) 
	,[aiapts] [NVARCHAR](1) 
	,[aisbal] [NVARCHAR](1) 
	,[aiback] [NVARCHAR](1) 
	,[aiporq] [NVARCHAR](1) 
	,[aiprio] [NVARCHAR](1) 
	,[aiarto] [NVARCHAR](1) 
	,[aiinvc] [DECIMAL](38, 4) 
	,[aiicon] [NVARCHAR](1) 
	,[aiblfr] [NVARCHAR](1) 
	,[ainivd] [VARCHAR](20) 
	,[ailedj] [VARCHAR](20) 
	,[aiplst] [NVARCHAR](1) 
	,[aimord] [NVARCHAR](1) 
	,[aicmc1] [DECIMAL](38, 4) 
	,[aicmr1] [DECIMAL](38, 4) 
	,[aicmc2] [DECIMAL](38, 4) 
	,[aicmr2] [DECIMAL](38, 4) 
	,[aipalc] [NVARCHAR](1) 
	,[aivumd] [NVARCHAR](2) 
	,[aiwumd] [NVARCHAR](2) 
	,[aiedpm] [NVARCHAR](1) 
	,[aiedii] [NVARCHAR](1) 
	,[aiedci] [NVARCHAR](1) 
	,[aiedqd] [DECIMAL](38, 4) 
	,[aiedad] [DECIMAL](38, 4) 
	,[aiedf1] [NVARCHAR](1) 
	,[aiedf2] [NVARCHAR](1) 
	,[aisi01] [NVARCHAR](1) 
	,[aisi02] [NVARCHAR](1) 
	,[aisi03] [NVARCHAR](1) 
	,[aisi04] [NVARCHAR](1) 
	,[aisi05] [NVARCHAR](1) 
	,[aiurcd] [NVARCHAR](2) 
	,[aiurat] [DECIMAL](38, 4) 
	,[aiurab] [DECIMAL](38, 4) 
	,[aiurdt] [VARCHAR](20) 
	,[aiurrf] [NVARCHAR](15) 
	,[aicp01] [NVARCHAR](1) 
	,[aiasn] [NVARCHAR](8) 
	,[aidspa] [NVARCHAR](1) 
	,[aicrmd] [NVARCHAR](1) 
	,[aiply] [DECIMAL](38, 4) 
	,[aiman8] [DECIMAL](38, 4) 
	,[aiarl] [NVARCHAR](10) 
	,[aiamcr] [DECIMAL](38, 4) 
	,[aiac01] [NVARCHAR](3) 
	,[aiac02] [NVARCHAR](3) 
	,[aiac03] [NVARCHAR](3) 
	,[aiac04] [NVARCHAR](3) 
	,[aiac05] [NVARCHAR](3) 
	,[aiac06] [NVARCHAR](3) 
	,[aiac07] [NVARCHAR](3) 
	,[aiac08] [NVARCHAR](3) 
	,[aiac09] [NVARCHAR](3) 
	,[aiac10] [NVARCHAR](3) 
	,[aiac11] [NVARCHAR](3) 
	,[aiac12] [NVARCHAR](3) 
	,[aiac13] [NVARCHAR](3) 
	,[aiac14] [NVARCHAR](3) 
	,[aiac15] [NVARCHAR](3) 
	,[aiac16] [NVARCHAR](3) 
	,[aiac17] [NVARCHAR](3) 
	,[aiac18] [NVARCHAR](3) 
	,[aiac19] [NVARCHAR](3) 
	,[aiac20] [NVARCHAR](3) 
	,[aiac21] [NVARCHAR](3) 
	,[aiac22] [NVARCHAR](3) 
	,[aiac23] [NVARCHAR](3) 
	,[aiac24] [NVARCHAR](3) 
	,[aiac25] [NVARCHAR](3) 
	,[aiac26] [NVARCHAR](3) 
	,[aiac27] [NVARCHAR](3) 
	,[aiac28] [NVARCHAR](3) 
	,[aiac29] [NVARCHAR](3) 
	,[aiac30] [NVARCHAR](3) 
	,[aislpg] [NVARCHAR](10) 
	,[aisldw] [NVARCHAR](10) 
	,[aicfpp] [NVARCHAR](18) 
	,[aicfsp] [NVARCHAR](18) 
	,[aicfdf] [NVARCHAR](1) 
	,[airq01] [NVARCHAR](1) 
	,[airq02] [NVARCHAR](1) 
	,[aidr03] [NVARCHAR](2) 
	,[aidr04] [NVARCHAR](2) 
	,[airq03] [NVARCHAR](1) 
	,[airq04] [NVARCHAR](1) 
	,[airq05] [NVARCHAR](1) 
	,[airq06] [NVARCHAR](1) 
	,[airq07] [NVARCHAR](1) 
	,[airq08] [NVARCHAR](1) 
	,[aidr08] [NVARCHAR](2) 
	,[aidr09] [NVARCHAR](2) 
	,[airq09] [NVARCHAR](1) 
	,[aiuser] [NVARCHAR](10) 
	,[aipid] [NVARCHAR](10) 
	,[aiupmj] [VARCHAR](20) 
	,[aiupmt] [DECIMAL](38, 4) 
	,[aijobn] [NVARCHAR](10) 
	,[aiprgf] [NVARCHAR](1) 
	,[aibyal] [NVARCHAR](1) 
	,[aibsc] [NVARCHAR](10) 
	,[aiashl] [NVARCHAR](1) 
	,[aiprsn] [NVARCHAR](8) 
	,[aiopbo] [NVARCHAR](30) 
	,[aiapsb] [NVARCHAR](1) 
	,[aitier1] [NVARCHAR](5) 
	,[aipwpcp] [DECIMAL](38, 4) 
	,[aicusts] [NVARCHAR](1) 
	,[aistof] [NVARCHAR](1) 
	,[aiterrid] [DECIMAL](38, 4) 
	,[aicig] [DECIMAL](38, 4) 
	,[aitorg] [NVARCHAR](10) 
	,[aidtee] [NVARCHAR](11) 
	,[aisyncs] [DECIMAL](38, 4) 
	,[aicaad] [DECIMAL](38, 4) 
	,[aigopasf] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f03012/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
