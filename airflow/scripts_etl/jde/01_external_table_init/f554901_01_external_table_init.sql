--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f554901_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f554901_init

CREATE EXTERNAL TABLE stg_jde.ext_f554901_init
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
	,[ddvr01] [NVARCHAR](25) 
	,[dddoco] [DECIMAL](38, 4) 
	,[ddlnid] [DECIMAL](38, 4) 
	,[ddlitm] [NVARCHAR](25) 
	,[ddy55doco] [NVARCHAR](10) 
	,[ddy55msgno] [DECIMAL](38, 4) 
	,[ddy55insdj] [VARCHAR](20) 
	,[ddy55insdt] [DECIMAL](38, 4) 
	,[ddshpn] [DECIMAL](38, 4) 
	,[ddssts] [NVARCHAR](2) 
	,[ddkcoo] [NVARCHAR](5) 
	,[dddcto] [NVARCHAR](2) 
	,[ddy55dlqs] [DECIMAL](38, 4) 
	,[ddums] [NVARCHAR](5) 
	,[ddtemp] [DECIMAL](38, 4) 
	,[ddy55tnfl] [NVARCHAR](1) 
	,[ddy55orfg] [NVARCHAR](1) 
	,[dddcdt] [VARCHAR](20) 
	,[dddldl] [VARCHAR](20) 
	,[ddy55aclt] [DECIMAL](38, 4) 
	,[dddisn] [DECIMAL](38, 4) 
	,[ddy55xcrd] [DECIMAL](38, 4) 
	,[ddy55ycrd] [DECIMAL](38, 4) 
	,[ddcmqt] [DECIMAL](38, 4) 
	,[ddy55drcd] [DECIMAL](38, 4) 
	,[dddlda] [VARCHAR](20) 
	,[ddtdlf] [DECIMAL](38, 4) 
	,[ddy55tpdt] [DECIMAL](38, 4) 
	,[ddy55gpsf] [DECIMAL](38, 4) 
	,[ddy55pdflg] [NVARCHAR](1) 
	,[ddy55disn1] [DECIMAL](38, 4) 
	,[ddy55cttm] [DECIMAL](38, 4) 
	,[ddmcux] [NVARCHAR](12) 
	,[ddshan] [DECIMAL](38, 4) 
	,[ddcrcp] [NVARCHAR](3) 
	,[ddfrvc] [DECIMAL](38, 4) 
	,[dddoc] [DECIMAL](38, 4) 
	,[ddgnrt] [NVARCHAR](10) 
	,[ddy55gnno] [NVARCHAR](26) 
	,[ddtkid] [NVARCHAR](8) 
	,[dddend] [DECIMAL](38, 4) 
	,[ddqtyl] [DECIMAL](38, 4) 
	,[ddcert] [NVARCHAR](1) 
	,[ddy55govid] [NVARCHAR](20) 
	,[ddedbt] [NVARCHAR](15) 
	,[ddedtn] [NVARCHAR](22) 
	,[ddflag] [NVARCHAR](1) 
	,[dduser] [NVARCHAR](10) 
	,[ddpid] [NVARCHAR](10) 
	,[ddjobn] [NVARCHAR](10) 
	,[ddupmj] [VARCHAR](20) 
	,[ddupmt] [DECIMAL](38, 4) 
	,[ddurcd] [NVARCHAR](2) 
	,[ddurdt] [VARCHAR](20) 
	,[ddurat] [DECIMAL](38, 4) 
	,[ddurrf] [NVARCHAR](15) 
	,[ddurab] [DECIMAL](38, 4) 
	,[ddrcd] [NVARCHAR](3) 
	,[ddy55char1] [NVARCHAR](1) 
	,[ddy55char2] [NVARCHAR](1) 
	,[ddy55date1] [VARCHAR](20) 
	,[ddy55date2] [VARCHAR](20) 
	,[ddy55strg1] [NVARCHAR](30) 
	,[ddy55strg2] [NVARCHAR](30) 
	,[ddy55strg3] [NVARCHAR](30) 
	,[ddy55strg4] [NVARCHAR](30) 
	,[ddy55strg5] [NVARCHAR](30) 
	,[ddy55strg6] [NVARCHAR](12) 
	,[ddy55strg7] [NVARCHAR](12) 
	,[ddy55strg8] [NVARCHAR](12) 
	,[ddy55strg9] [NVARCHAR](12) 
	,[ddy55strg10] [NVARCHAR](12) 
	,[ddy55flg1] [NVARCHAR](1) 
	,[ddy55flg2] [NVARCHAR](1) 
	,[ddy55num3] [DECIMAL](38, 4) 
	,[ddy55num4] [DECIMAL](38, 4) 
	,[ddy55time1] [DECIMAL](38, 4) 
	,[ddy55time2] [DECIMAL](38, 4) 
	,[ddy55amnt1] [DECIMAL](38, 4) 
	,[ddy55amnt2] [DECIMAL](38, 4) 
	,[ddordnumid] [NVARCHAR](30) 
	,[ddrornumid] [NVARCHAR](30) 
	,[ddvmcu] [NVARCHAR](12) 
	,[ddflg2] [NVARCHAR](1) 
	,[ddflg3] [NVARCHAR](1) 
	,[ddflg4] [NVARCHAR](1) 
	,[ddukid] [DECIMAL](38, 4) 
	,[ddboolean] [NVARCHAR](10) 
	,[ddppdj] [VARCHAR](20) 
	,[ddcflg] [NVARCHAR](1) 
	,[ddgovid1] [NVARCHAR](25) 
	,[ddot1] [DECIMAL](38, 4) 
	,[ddef01] [DECIMAL](38, 4) 
	,[ddflg1] [NVARCHAR](1) 
	,[ddfmrund] [NVARCHAR](1) 
	,[ddsclq] [DECIMAL](38, 4) 
	,[ddtme0] [DECIMAL](38, 4) 
	,[dddte] [VARCHAR](20) 
	,[ddancc] [DECIMAL](38, 4) 
	,[ddorgn] [DECIMAL](38, 4) 
	,[ddtdlt] [DECIMAL](38, 4) 
	,[dditm] [DECIMAL](38, 4) 
	,[ddxpitraid] [DECIMAL](38, 4) 
	,[ddy55cert] [NVARCHAR](30) 
	,[ddy55qlty] [NVARCHAR](30) 
	,[ddy55zdrcd] [NVARCHAR](3) 
)
WITH
(
    LOCATION='processing-clean/jde/f554901/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
