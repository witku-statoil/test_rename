--Create ext table for init
IF OBJECT_ID('stg_jde_qat.ext_f4311_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde_qat.ext_f4311_init

CREATE EXTERNAL TABLE stg_jde_qat.ext_f4311_init
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
	,[pdkcoo] [NVARCHAR](5) 
	,[pddoco] [DECIMAL](38, 4) 
	,[pddcto] [NVARCHAR](2) 
	,[pdsfxo] [NVARCHAR](3) 
	,[pdlnid] [DECIMAL](38, 4) 
	,[pdmcu] [NVARCHAR](12) 
	,[pdco] [NVARCHAR](5) 
	,[pdokco] [NVARCHAR](5) 
	,[pdoorn] [NVARCHAR](8) 
	,[pdocto] [NVARCHAR](2) 
	,[pdogno] [DECIMAL](38, 4) 
	,[pdrkco] [NVARCHAR](5) 
	,[pdrorn] [NVARCHAR](8) 
	,[pdrcto] [NVARCHAR](2) 
	,[pdrlln] [DECIMAL](38, 4) 
	,[pddmct] [NVARCHAR](12) 
	,[pddmcs] [DECIMAL](38, 4) 
	,[pdbalu] [NVARCHAR](1) 
	,[pdan8] [DECIMAL](38, 4) 
	,[pdshan] [DECIMAL](38, 4) 
	,[pddrqj] [VARCHAR](20) 
	,[pdtrdj] [VARCHAR](20) 
	,[pdpddj] [VARCHAR](20) 
	,[pdopdj] [VARCHAR](20) 
	,[pdaddj] [VARCHAR](20) 
	,[pdcndj] [VARCHAR](20) 
	,[pdpefj] [VARCHAR](20) 
	,[pdppdj] [VARCHAR](20) 
	,[pdpsdj] [VARCHAR](20) 
	,[pddsvj] [VARCHAR](20) 
	,[pddgl] [VARCHAR](20) 
	,[pdpn] [DECIMAL](38, 4) 
	,[pdvr01] [NVARCHAR](25) 
	,[pdvr02] [NVARCHAR](25) 
	,[pditm] [DECIMAL](38, 4) 
	,[pdlitm] [NVARCHAR](25) 
	,[pdaitm] [NVARCHAR](25) 
	,[pdlocn] [NVARCHAR](20) 
	,[pdlotn] [NVARCHAR](30) 
	,[pdfrgd] [NVARCHAR](3) 
	,[pdthgd] [NVARCHAR](3) 
	,[pdfrmp] [DECIMAL](38, 4) 
	,[pdthrp] [DECIMAL](38, 4) 
	,[pddsc1] [NVARCHAR](30) 
	,[pddsc2] [NVARCHAR](30) 
	,[pdlnty] [NVARCHAR](2) 
	,[pdnxtr] [NVARCHAR](3) 
	,[pdlttr] [NVARCHAR](3) 
	,[pdrlit] [NVARCHAR](8) 
	,[pdpds1] [NVARCHAR](3) 
	,[pdpds2] [NVARCHAR](3) 
	,[pdpds3] [NVARCHAR](3) 
	,[pdpds4] [NVARCHAR](3) 
	,[pdpds5] [NVARCHAR](3) 
	,[pdpdp1] [NVARCHAR](3) 
	,[pdpdp2] [NVARCHAR](3) 
	,[pdpdp3] [NVARCHAR](3) 
	,[pdpdp4] [NVARCHAR](3) 
	,[pdpdp5] [NVARCHAR](3) 
	,[pduom] [NVARCHAR](2) 
	,[pduorg] [DECIMAL](38, 4) 
	,[pduchg] [DECIMAL](38, 4) 
	,[pduopn] [DECIMAL](38, 4) 
	,[pdurec] [DECIMAL](38, 4) 
	,[pdcrec] [DECIMAL](38, 4) 
	,[pdurlv] [DECIMAL](38, 4) 
	,[pdotqy] [NVARCHAR](1) 
	,[pdprrc] [DECIMAL](38, 4) 
	,[pdaexp] [DECIMAL](38, 4) 
	,[pdachg] [DECIMAL](38, 4) 
	,[pdaopn] [DECIMAL](38, 4) 
	,[pdarec] [DECIMAL](38, 4) 
	,[pdarlv] [DECIMAL](38, 4) 
	,[pdftn1] [DECIMAL](38, 4) 
	,[pdtrlv] [DECIMAL](38, 4) 
	,[pdprov] [NVARCHAR](1) 
	,[pdamc3] [DECIMAL](38, 4) 
	,[pdecst] [DECIMAL](38, 4) 
	,[pdcsto] [NVARCHAR](1) 
	,[pdcsmp] [NVARCHAR](1) 
	,[pdinmg] [NVARCHAR](10) 
	,[pdasn] [NVARCHAR](8) 
	,[pdprgr] [NVARCHAR](8) 
	,[pdclvl] [NVARCHAR](3) 
	,[pdcatn] [NVARCHAR](8) 
	,[pddspr] [DECIMAL](38, 4) 
	,[pdptc] [NVARCHAR](3) 
	,[pdtx] [NVARCHAR](1) 
	,[pdexr1] [NVARCHAR](2) 
	,[pdtxa1] [NVARCHAR](10) 
	,[pdatxt] [NVARCHAR](1) 
	,[pdcnid] [NVARCHAR](20) 
	,[pdcdcd] [NVARCHAR](15) 
	,[pdntr] [NVARCHAR](2) 
	,[pdfrth] [NVARCHAR](3) 
	,[pdfrtc] [NVARCHAR](1) 
	,[pdzon] [NVARCHAR](3) 
	,[pdfrat] [NVARCHAR](10) 
	,[pdratt] [NVARCHAR](1) 
	,[pdanby] [DECIMAL](38, 4) 
	,[pdancr] [DECIMAL](38, 4) 
	,[pdmot] [NVARCHAR](3) 
	,[pdcot] [NVARCHAR](3) 
	,[pdshcm] [NVARCHAR](3) 
	,[pdshcn] [NVARCHAR](3) 
	,[pduom1] [NVARCHAR](2) 
	,[pdpqor] [DECIMAL](38, 4) 
	,[pduom2] [NVARCHAR](2) 
	,[pdsqor] [DECIMAL](38, 4) 
	,[pduom3] [NVARCHAR](2) 
	,[pditwt] [DECIMAL](38, 4) 
	,[pdwtum] [NVARCHAR](2) 
	,[pditvl] [DECIMAL](38, 4) 
	,[pdvlum] [NVARCHAR](2) 
	,[pdglc] [NVARCHAR](4) 
	,[pdctry] [DECIMAL](38, 4) 
	,[pdfy] [DECIMAL](38, 4) 
	,[pdstts] [NVARCHAR](2) 
	,[pdrcd] [NVARCHAR](3) 
	,[pdfuf1] [NVARCHAR](1) 
	,[pdfuf2] [NVARCHAR](1) 
	,[pdgrwt] [DECIMAL](38, 4) 
	,[pdgwum] [NVARCHAR](2) 
	,[pdlt] [NVARCHAR](2) 
	,[pdani] [NVARCHAR](29) 
	,[pdaid] [NVARCHAR](8) 
	,[pdomcu] [NVARCHAR](12) 
	,[pdobj] [NVARCHAR](6) 
	,[pdsub] [NVARCHAR](8) 
	,[pdsblt] [NVARCHAR](1) 
	,[pdsbl] [NVARCHAR](8) 
	,[pdasid] [NVARCHAR](25) 
	,[pdccmp] [DECIMAL](38, 4) 
	,[pdtag] [NVARCHAR](8) 
	,[pdwr01] [NVARCHAR](4) 
	,[pdpl] [NVARCHAR](4) 
	,[pdelev] [NVARCHAR](3) 
	,[pdr001] [NVARCHAR](3) 
	,[pdrtnr] [NVARCHAR](3) 
	,[pdlcod] [NVARCHAR](2) 
	,[pdpurg] [NVARCHAR](1) 
	,[pdprom] [NVARCHAR](1) 
	,[pdfnlp] [NVARCHAR](1) 
	,[pdavch] [NVARCHAR](1) 
	,[pdprpy] [NVARCHAR](1) 
	,[pduncd] [NVARCHAR](1) 
	,[pdmaty] [NVARCHAR](1) 
	,[pdrtgc] [NVARCHAR](1) 
	,[pdrcpf] [NVARCHAR](1) 
	,[pdps01] [NVARCHAR](1) 
	,[pdps02] [NVARCHAR](1) 
	,[pdps03] [NVARCHAR](1) 
	,[pdps04] [NVARCHAR](1) 
	,[pdps05] [NVARCHAR](1) 
	,[pdps06] [NVARCHAR](1) 
	,[pdps07] [NVARCHAR](1) 
	,[pdps08] [NVARCHAR](1) 
	,[pdps09] [NVARCHAR](1) 
	,[pdps10] [NVARCHAR](1) 
	,[pdcrmd] [NVARCHAR](1) 
	,[pdartg] [NVARCHAR](12) 
	,[pdcord] [DECIMAL](38, 4) 
	,[pdchdt] [NVARCHAR](2) 
	,[pddocc] [DECIMAL](38, 4) 
	,[pdchln] [DECIMAL](38, 4) 
	,[pdcrcd] [NVARCHAR](3) 
	,[pdcrr] [DECIMAL](38, 4) 
	,[pdfrrc] [DECIMAL](38, 4) 
	,[pdfea] [DECIMAL](38, 4) 
	,[pdfuc] [DECIMAL](38, 4) 
	,[pdfec] [DECIMAL](38, 4) 
	,[pdfchg] [DECIMAL](38, 4) 
	,[pdfap] [DECIMAL](38, 4) 
	,[pdfrec] [DECIMAL](38, 4) 
	,[pdurcd] [NVARCHAR](2) 
	,[pdurdt] [VARCHAR](20) 
	,[pdurat] [DECIMAL](38, 4) 
	,[pdurab] [DECIMAL](38, 4) 
	,[pdurrf] [NVARCHAR](15) 
	,[pdtorg] [NVARCHAR](10) 
	,[pduser] [NVARCHAR](10) 
	,[pdpid] [NVARCHAR](10) 
	,[pdjobn] [NVARCHAR](10) 
	,[pdupmj] [VARCHAR](20) 
	,[pdtday] [DECIMAL](38, 4) 
	,[pdvr05] [NVARCHAR](25) 
	,[pdvr04] [NVARCHAR](25) 
	,[pdshpn] [DECIMAL](38, 4) 
	,[pdrsht] [DECIMAL](38, 4) 
	,[pdprjm] [DECIMAL](38, 4) 
	,[pdosfx] [NVARCHAR](3) 
	,[pdmerl] [NVARCHAR](3) 
	,[pdmcln] [DECIMAL](38, 4) 
	,[pdmact] [NVARCHAR](1) 
	,[pdktln] [DECIMAL](38, 4) 
	,[pdftrl] [DECIMAL](38, 4) 
	,[pddual] [NVARCHAR](1) 
	,[pddrqt] [DECIMAL](38, 4) 
	,[pddlej] [VARCHAR](20) 
	,[pdctam] [DECIMAL](38, 4) 
	,[pdcpnt] [DECIMAL](38, 4) 
	,[pdcht] [DECIMAL](38, 4) 
	,[pdchrt] [DECIMAL](38, 4) 
	,[pdchrs] [NVARCHAR](1) 
	,[pdchmj] [VARCHAR](20) 
	,[pdbcrc] [NVARCHAR](3) 
	,[pdvr03] [NVARCHAR](25) 
	,[pdldnm] [DECIMAL](38, 4) 
	,[pdmkfr] [DECIMAL](38, 4) 
	,[pdpmtn] [NVARCHAR](12) 
	,[pdukid] [DECIMAL](38, 4) 
	,[pdunspsc] [NVARCHAR](8) 
	,[pdcmdcde] [NVARCHAR](15) 
	,[pdrsfx] [NVARCHAR](3) 
	,[pdwvid] [DECIMAL](38, 4) 
	,[pdcntrtid] [DECIMAL](38, 4) 
	,[pdcntrtdid] [DECIMAL](38, 4) 
	,[pdmoadj] [NVARCHAR](1) 
	,[pdpodc01] [NVARCHAR](3) 
	,[pdpodc02] [NVARCHAR](3) 
	,[pdpodc03] [NVARCHAR](10) 
	,[pdpodc04] [NVARCHAR](10) 
	,[pdjbcd] [NVARCHAR](6) 
	,[pdsrqty] [DECIMAL](38, 4) 
	,[pdsruom] [NVARCHAR](2) 
	,[pdcfgfl] [NVARCHAR](1) 
	,[pdpmpn] [NVARCHAR](30) 
	,[pdpns] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde_qat/f4311/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
