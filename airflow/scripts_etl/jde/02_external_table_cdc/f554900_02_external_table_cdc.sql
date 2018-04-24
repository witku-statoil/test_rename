--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f554900_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f554900_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f554900_cdc
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
	,[dhvr01] [NVARCHAR](25) 
	,[dhy55msgno] [DECIMAL](38, 4) 
	,[dhy55insdj] [VARCHAR](20) 
	,[dhy55insdt] [DECIMAL](38, 4) 
	,[dhnstp] [DECIMAL](38, 4) 
	,[dhpveh] [NVARCHAR](12) 
	,[dhnbrord] [DECIMAL](38, 4) 
	,[dhy55dstt] [DECIMAL](38, 4) 
	,[dhy55dsnt] [DECIMAL](38, 4) 
	,[dhumd1] [NVARCHAR](2) 
	,[dhy55eltm] [DECIMAL](38, 4) 
	,[dhy55odrtm] [DECIMAL](38, 4) 
	,[dhy55odltm] [DECIMAL](38, 4) 
	,[dhy55oldtm] [DECIMAL](38, 4) 
	,[dhy55tsdj] [VARCHAR](20) 
	,[dhy55tsdm] [DECIMAL](38, 4) 
	,[dhdlda] [VARCHAR](20) 
	,[dhtdlf] [DECIMAL](38, 4) 
	,[dhy55tdlf] [DECIMAL](38, 4) 
	,[dhdldl] [VARCHAR](20) 
	,[dhtdlt] [DECIMAL](38, 4) 
	,[dhy55tdlt] [DECIMAL](38, 4) 
	,[dhy55tedj] [VARCHAR](20) 
	,[dhy55tetm] [DECIMAL](38, 4) 
	,[dhldnm] [DECIMAL](38, 4) 
	,[dhldls] [NVARCHAR](2) 
	,[dhy55ercod] [NVARCHAR](10) 
	,[dhy55eflg] [NVARCHAR](1) 
	,[dhy55tdlq] [DECIMAL](38, 4) 
	,[dhuom] [NVARCHAR](2) 
	,[dhy55ldno] [DECIMAL](38, 4) 
	,[dhy55mnor] [DECIMAL](38, 4) 
	,[dhy55mnstp] [NVARCHAR](1) 
	,[dhy55pdno] [DECIMAL](38, 4) 
	,[dhy55zdno] [DECIMAL](38, 4) 
	,[dhy55rdno] [DECIMAL](38, 4) 
	,[dhy55ntat] [DECIMAL](38, 4) 
	,[dhy55wttm] [DECIMAL](38, 4) 
	,[dhy55odstm] [DECIMAL](38, 4) 
	,[dhy55oumds] [NVARCHAR](2) 
	,[dhy55disn1] [DECIMAL](38, 4) 
	,[dhy55stmcu] [DECIMAL](38, 4) 
	,[dhy55etmcu] [DECIMAL](38, 4) 
	,[dhy55bflg] [NVARCHAR](1) 
	,[dhy55drtm] [DECIMAL](38, 4) 
	,[dhy55mctm] [DECIMAL](38, 4) 
	,[dhstfn] [DECIMAL](38, 4) 
	,[dhy55wktm] [DECIMAL](38, 4) 
	,[dhwactime] [DECIMAL](38, 4) 
	,[dhappflg] [NVARCHAR](1) 
	,[dhbhuser] [NVARCHAR](10) 
	,[dhapdj] [VARCHAR](20) 
	,[dhatim] [DECIMAL](38, 4) 
	,[dhy55ackm] [DECIMAL](38, 4) 
	,[dhy55gpskm] [DECIMAL](38, 4) 
	,[dhrlno] [NVARCHAR](13) 
	,[dhtlnum] [NVARCHAR](12) 
	,[dhy55ldtm] [DECIMAL](38, 4) 
	,[dhy55drcd] [DECIMAL](38, 4) 
	,[dhy55rbflg] [NVARCHAR](1) 
	,[dhy55glfg] [NVARCHAR](1) 
	,[dhy55glap] [NVARCHAR](1) 
	,[dhy55srce] [NVARCHAR](12) 
	,[dhedbt] [NVARCHAR](15) 
	,[dhedtn] [NVARCHAR](22) 
	,[dhy55spdl] [DECIMAL](38, 4) 
	,[dhcars] [DECIMAL](38, 4) 
	,[dhflag] [NVARCHAR](1) 
	,[dhuser] [NVARCHAR](10) 
	,[dhpid] [NVARCHAR](10) 
	,[dhjobn] [NVARCHAR](10) 
	,[dhupmj] [VARCHAR](20) 
	,[dhupmt] [DECIMAL](38, 4) 
	,[dhurcd] [NVARCHAR](2) 
	,[dhurdt] [VARCHAR](20) 
	,[dhurat] [DECIMAL](38, 4) 
	,[dhurab] [DECIMAL](38, 4) 
	,[dhurrf] [NVARCHAR](15) 
	,[dhrcd] [NVARCHAR](3) 
	,[dhy55char1] [NVARCHAR](1) 
	,[dhy55char2] [NVARCHAR](1) 
	,[dhy55date1] [VARCHAR](20) 
	,[dhy55date2] [VARCHAR](20) 
	,[dhy55strg1] [NVARCHAR](30) 
	,[dhy55strg2] [NVARCHAR](30) 
	,[dhy55strg3] [NVARCHAR](30) 
	,[dhy55strg4] [NVARCHAR](30) 
	,[dhy55strg5] [NVARCHAR](30) 
	,[dhy55strg6] [NVARCHAR](12) 
	,[dhy55strg7] [NVARCHAR](12) 
	,[dhy55strg8] [NVARCHAR](12) 
	,[dhy55strg9] [NVARCHAR](12) 
	,[dhy55strg10] [NVARCHAR](12) 
	,[dhy55flg1] [NVARCHAR](1) 
	,[dhy55flg2] [NVARCHAR](1) 
	,[dhy55num3] [DECIMAL](38, 4) 
	,[dhy55num4] [DECIMAL](38, 4) 
	,[dhy55time1] [DECIMAL](38, 4) 
	,[dhy55time2] [DECIMAL](38, 4) 
	,[dhy55amnt1] [DECIMAL](38, 4) 
	,[dhy55amnt2] [DECIMAL](38, 4) 
	,[dhum] [NVARCHAR](2) 
	,[dhhrsact] [DECIMAL](38, 4) 
	,[dhflg2] [NVARCHAR](1) 
	,[dhflg3] [NVARCHAR](1) 
	,[dhflg4] [NVARCHAR](1) 
	,[dhvmcu] [NVARCHAR](12) 
	,[dhy55disn] [DECIMAL](38, 4) 
	,[dhefr] [DECIMAL](38, 4) 
	,[dhhrwt] [DECIMAL](38, 4) 
	,[dhaltd] [DECIMAL](38, 4) 
	,[dhdrtm] [DECIMAL](38, 4) 
	,[dhflg1] [NVARCHAR](1) 
	,[dhapsfact] [DECIMAL](38, 4) 
	,[dhprqu] [DECIMAL](38, 4) 
	,[dhuom1] [NVARCHAR](2) 
	,[dhmc1] [DECIMAL](38, 4) 
	,[dhtcap] [DECIMAL](38, 4) 
	,[dhot1] [DECIMAL](38, 4) 
	,[dhcrcp] [NVARCHAR](3) 
	,[dhpraa] [DECIMAL](38, 4) 
	,[dhaoth] [DECIMAL](38, 4) 
	,[dhchgamt] [DECIMAL](38, 4) 
	,[dhnbr3] [DECIMAL](38, 4) 
	,[dhnbr2] [DECIMAL](38, 4) 
	,[dhnbr1] [DECIMAL](38, 4) 
	,[dhnocm] [DECIMAL](38, 4) 
	,[dhnbrsld] [DECIMAL](38, 4) 
	,[dhtold] [DECIMAL](38, 4) 
	,[dhodst] [DECIMAL](38, 4) 
	,[dhsclq] [DECIMAL](38, 4) 
	,[dhy55trtm] [DECIMAL](38, 4) 
	,[dhorgn] [DECIMAL](38, 4) 
	,[dhtrdt] [VARCHAR](20) 
	,[dhsttme] [DECIMAL](38, 4) 
	,[dhstdate] [VARCHAR](20) 
	,[dhdisn] [DECIMAL](38, 4) 
	,[dhdtai] [NVARCHAR](10) 
	,[dhy55elts2] [DECIMAL](38, 4) 
	,[dhy55elts1] [DECIMAL](38, 4) 
	,[dhy55elts] [DECIMAL](38, 4) 
	,[dhtme0] [DECIMAL](38, 4) 
	,[dhacttime] [DECIMAL](38, 4) 
	,[dhbstn] [DECIMAL](38, 4) 
	,[dhancc] [DECIMAL](38, 4) 
	,[dhdte] [VARCHAR](20) 
	,[dhukid] [DECIMAL](38, 4) 
	,[dhxpitraid] [DECIMAL](38, 4) 
	,[dhy55vg] [NVARCHAR](10) 
	,[dhvtyp] [NVARCHAR](12) 
	,[dhy55rm1] [NVARCHAR](60) 
	,[dhy55rm2] [NVARCHAR](60) 
	,[dhy55stter] [NVARCHAR](12) 
	,[dhy55edter] [NVARCHAR](12) 
	,[dhy55wrcd] [NVARCHAR](3) 
	,[dhy55sapflg] [NVARCHAR](1) 
	,[dhy55mnobsp] [NVARCHAR](1) 
	,[dhy55mcth] [DECIMAL](38, 4) 
	,[dhy55wttmh] [DECIMAL](38, 4) 
	,[dhy55wktmh] [DECIMAL](38, 4) 
	,[dhy55nstp] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f554900/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
