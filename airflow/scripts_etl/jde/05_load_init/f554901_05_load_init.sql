-- Create rep new table for init
IF OBJECT_ID('rep_jde.f554901_new') IS NOT NULL
    DROP TABLE rep_jde.f554901_new


CREATE TABLE rep_jde.f554901_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
) AS 
SELECT
    	cast(sys_file_name as [NVARCHAR](100)) as sys_file_name
	,cast(sys_file_ln as [INT]) as sys_file_ln
	,cast(sys_integration_id as [NVARCHAR](200)) as sys_integration_id
	,cast(sys_extract_dt as [NVARCHAR](40)) as sys_extract_dt
	,cast(sys_cdc_dt as [NVARCHAR](40)) as sys_cdc_dt
	,cast(sys_cdc_scn as [NVARCHAR](14)) as sys_cdc_scn
	,cast(sys_cdc_operation_type as [NVARCHAR](15)) as sys_cdc_operation_type
	,cast(sys_cdc_before_after as [NVARCHAR](10)) as sys_cdc_before_after
	,cast(sys_line_modified_ind as [INT]) as sys_line_modified_ind
	,ddvr01 as ddvr01
	,ltrim(rtrim(ddvr01)) as ddvr01_conv
	,cast(dddoco as [DECIMAL](38, 4)) as dddoco
	,cast(ddlnid as [DECIMAL](38, 4)) as ddlnid
	,cast(ddlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ddlnid_conv
	,ddlitm as ddlitm
	,ltrim(rtrim(ddlitm)) as ddlitm_conv
	,ddy55doco as ddy55doco
	,ltrim(rtrim(ddy55doco)) as ddy55doco_conv
	,cast(ddy55msgno as [DECIMAL](38, 4)) as ddy55msgno
	,cast(ddy55msgno as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ddy55msgno_conv
	,cast(ddy55insdj as [INT]) as ddy55insdj
	,case when cast(ddy55insdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ddy55insdj as [INT]) %1000, dateadd(year, cast(ddy55insdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ddy55insdj_conv
	,cast(ddy55insdt as [DECIMAL](38, 4)) as ddy55insdt
	,cast(ddshpn as [DECIMAL](38, 4)) as ddshpn
	,cast(ddssts as [NVARCHAR](2)) as ddssts
	,ddkcoo as ddkcoo
	,ltrim(rtrim(ddkcoo)) as ddkcoo_conv
	,cast(dddcto as [NVARCHAR](2)) as dddcto
	,cast(ddy55dlqs as [DECIMAL](38, 4)) as ddy55dlqs
	,cast(ddy55dlqs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddy55dlqs_conv
	,ddums as ddums
	,ltrim(rtrim(ddums)) as ddums_conv
	,cast(ddtemp as [DECIMAL](38, 4)) as ddtemp
	,cast(ddtemp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ddtemp_conv
	,ddy55tnfl as ddy55tnfl
	,ltrim(rtrim(ddy55tnfl)) as ddy55tnfl_conv
	,ddy55orfg as ddy55orfg
	,ltrim(rtrim(ddy55orfg)) as ddy55orfg_conv
	,cast(dddcdt as [INT]) as dddcdt
	,case when cast(dddcdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dddcdt as [INT]) %1000, dateadd(year, cast(dddcdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dddcdt_conv
	,cast(dddldl as [INT]) as dddldl
	,case when cast(dddldl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dddldl as [INT]) %1000, dateadd(year, cast(dddldl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dddldl_conv
	,cast(ddy55aclt as [DECIMAL](38, 4)) as ddy55aclt
	,cast(dddisn as [DECIMAL](38, 4)) as dddisn
	,cast(ddy55xcrd as [DECIMAL](38, 4)) as ddy55xcrd
	,cast(ddy55ycrd as [DECIMAL](38, 4)) as ddy55ycrd
	,cast(ddcmqt as [DECIMAL](38, 4)) as ddcmqt
	,cast(ddcmqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddcmqt_conv
	,cast(ddy55drcd as [DECIMAL](38, 4)) as ddy55drcd
	,cast(ddy55drcd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ddy55drcd_conv
	,cast(dddlda as [INT]) as dddlda
	,case when cast(dddlda as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dddlda as [INT]) %1000, dateadd(year, cast(dddlda as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dddlda_conv
	,cast(ddtdlf as [DECIMAL](38, 4)) as ddtdlf
	,cast(ddy55tpdt as [DECIMAL](38, 4)) as ddy55tpdt
	,cast(ddy55gpsf as [DECIMAL](38, 4)) as ddy55gpsf
	,ddy55pdflg as ddy55pdflg
	,ltrim(rtrim(ddy55pdflg)) as ddy55pdflg_conv
	,cast(ddy55disn1 as [DECIMAL](38, 4)) as ddy55disn1
	,cast(ddy55cttm as [DECIMAL](38, 4)) as ddy55cttm
	,ddmcux as ddmcux
	,ltrim(rtrim(ddmcux)) as ddmcux_conv
	,cast(ddshan as [DECIMAL](38, 4)) as ddshan
	,ddcrcp as ddcrcp
	,ltrim(rtrim(ddcrcp)) as ddcrcp_conv
	,cast(ddfrvc as [DECIMAL](38, 4)) as ddfrvc
	,cast(ddfrvc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddfrvc_conv
	,cast(dddoc as [DECIMAL](38, 4)) as dddoc
	,ddgnrt as ddgnrt
	,ltrim(rtrim(ddgnrt)) as ddgnrt_conv
	,ddy55gnno as ddy55gnno
	,ltrim(rtrim(ddy55gnno)) as ddy55gnno_conv
	,ddtkid as ddtkid
	,ltrim(rtrim(ddtkid)) as ddtkid_conv
	,cast(dddend as [DECIMAL](38, 4)) as dddend
	,cast(dddend as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dddend_conv
	,cast(ddqtyl as [DECIMAL](38, 4)) as ddqtyl
	,cast(ddqtyl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddqtyl_conv
	,cast(ddcert as [NVARCHAR](1)) as ddcert
	,ddy55govid as ddy55govid
	,ltrim(rtrim(ddy55govid)) as ddy55govid_conv
	,ddedbt as ddedbt
	,ltrim(rtrim(ddedbt)) as ddedbt_conv
	,ddedtn as ddedtn
	,ltrim(rtrim(ddedtn)) as ddedtn_conv
	,ddflag as ddflag
	,ltrim(rtrim(ddflag)) as ddflag_conv
	,dduser as dduser
	,ltrim(rtrim(dduser)) as dduser_conv
	,ddpid as ddpid
	,ltrim(rtrim(ddpid)) as ddpid_conv
	,ddjobn as ddjobn
	,ltrim(rtrim(ddjobn)) as ddjobn_conv
	,cast(ddupmj as [INT]) as ddupmj
	,case when cast(ddupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ddupmj as [INT]) %1000, dateadd(year, cast(ddupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ddupmj_conv
	,cast(ddupmt as [DECIMAL](38, 4)) as ddupmt
	,ddurcd as ddurcd
	,ltrim(rtrim(ddurcd)) as ddurcd_conv
	,cast(ddurdt as [INT]) as ddurdt
	,case when cast(ddurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ddurdt as [INT]) %1000, dateadd(year, cast(ddurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ddurdt_conv
	,cast(ddurat as [DECIMAL](38, 4)) as ddurat
	,cast(ddurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ddurat_conv
	,ddurrf as ddurrf
	,ltrim(rtrim(ddurrf)) as ddurrf_conv
	,cast(ddurab as [DECIMAL](38, 4)) as ddurab
	,cast(ddurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ddurab_conv
	,cast(ddrcd as [NVARCHAR](3)) as ddrcd
	,ddy55char1 as ddy55char1
	,ltrim(rtrim(ddy55char1)) as ddy55char1_conv
	,ddy55char2 as ddy55char2
	,ltrim(rtrim(ddy55char2)) as ddy55char2_conv
	,cast(ddy55date1 as [INT]) as ddy55date1
	,case when cast(ddy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ddy55date1 as [INT]) %1000, dateadd(year, cast(ddy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ddy55date1_conv
	,cast(ddy55date2 as [INT]) as ddy55date2
	,case when cast(ddy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ddy55date2 as [INT]) %1000, dateadd(year, cast(ddy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ddy55date2_conv
	,ddy55strg1 as ddy55strg1
	,ltrim(rtrim(ddy55strg1)) as ddy55strg1_conv
	,ddy55strg2 as ddy55strg2
	,ltrim(rtrim(ddy55strg2)) as ddy55strg2_conv
	,ddy55strg3 as ddy55strg3
	,ltrim(rtrim(ddy55strg3)) as ddy55strg3_conv
	,ddy55strg4 as ddy55strg4
	,ltrim(rtrim(ddy55strg4)) as ddy55strg4_conv
	,ddy55strg5 as ddy55strg5
	,ltrim(rtrim(ddy55strg5)) as ddy55strg5_conv
	,ddy55strg6 as ddy55strg6
	,ltrim(rtrim(ddy55strg6)) as ddy55strg6_conv
	,ddy55strg7 as ddy55strg7
	,ltrim(rtrim(ddy55strg7)) as ddy55strg7_conv
	,ddy55strg8 as ddy55strg8
	,ltrim(rtrim(ddy55strg8)) as ddy55strg8_conv
	,ddy55strg9 as ddy55strg9
	,ltrim(rtrim(ddy55strg9)) as ddy55strg9_conv
	,ddy55strg10 as ddy55strg10
	,ltrim(rtrim(ddy55strg10)) as ddy55strg10_conv
	,ddy55flg1 as ddy55flg1
	,ltrim(rtrim(ddy55flg1)) as ddy55flg1_conv
	,ddy55flg2 as ddy55flg2
	,ltrim(rtrim(ddy55flg2)) as ddy55flg2_conv
	,cast(ddy55num3 as [DECIMAL](38, 4)) as ddy55num3
	,cast(ddy55num3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddy55num3_conv
	,cast(ddy55num4 as [DECIMAL](38, 4)) as ddy55num4
	,cast(ddy55num4 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddy55num4_conv
	,cast(ddy55time1 as [DECIMAL](38, 4)) as ddy55time1
	,cast(ddy55time2 as [DECIMAL](38, 4)) as ddy55time2
	,cast(ddy55amnt1 as [DECIMAL](38, 4)) as ddy55amnt1
	,cast(ddy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ddy55amnt1_conv
	,cast(ddy55amnt2 as [DECIMAL](38, 4)) as ddy55amnt2
	,cast(ddy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ddy55amnt2_conv
	,ddordnumid as ddordnumid
	,ltrim(rtrim(ddordnumid)) as ddordnumid_conv
	,ddrornumid as ddrornumid
	,ltrim(rtrim(ddrornumid)) as ddrornumid_conv
	,ddvmcu as ddvmcu
	,ltrim(rtrim(ddvmcu)) as ddvmcu_conv
	,ddflg2 as ddflg2
	,ltrim(rtrim(ddflg2)) as ddflg2_conv
	,ddflg3 as ddflg3
	,ltrim(rtrim(ddflg3)) as ddflg3_conv
	,ddflg4 as ddflg4
	,ltrim(rtrim(ddflg4)) as ddflg4_conv
	,cast(ddukid as [DECIMAL](38, 4)) as ddukid
	,cast(ddboolean as [NVARCHAR](10)) as ddboolean
	,cast(ddppdj as [INT]) as ddppdj
	,case when cast(ddppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ddppdj as [INT]) %1000, dateadd(year, cast(ddppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ddppdj_conv
	,cast(ddcflg as [NVARCHAR](1)) as ddcflg
	,ddgovid1 as ddgovid1
	,ltrim(rtrim(ddgovid1)) as ddgovid1_conv
	,cast(ddot1 as [DECIMAL](38, 4)) as ddot1
	,cast(ddot1 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ddot1_conv
	,cast(ddef01 as [DECIMAL](38, 4)) as ddef01
	,ddflg1 as ddflg1
	,ltrim(rtrim(ddflg1)) as ddflg1_conv
	,ddfmrund as ddfmrund
	,ltrim(rtrim(ddfmrund)) as ddfmrund_conv
	,cast(ddsclq as [DECIMAL](38, 4)) as ddsclq
	,cast(ddsclq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ddsclq_conv
	,cast(ddtme0 as [DECIMAL](38, 4)) as ddtme0
	,cast(dddte as [INT]) as dddte
	,case when cast(dddte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dddte as [INT]) %1000, dateadd(year, cast(dddte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dddte_conv
	,cast(ddancc as [DECIMAL](38, 4)) as ddancc
	,cast(ddorgn as [DECIMAL](38, 4)) as ddorgn
	,cast(ddtdlt as [DECIMAL](38, 4)) as ddtdlt
	,cast(dditm as [DECIMAL](38, 4)) as dditm
	,cast(ddxpitraid as [DECIMAL](38, 4)) as ddxpitraid
	,cast(ddxpitraid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ddxpitraid_conv
	,ddy55cert as ddy55cert
	,ltrim(rtrim(ddy55cert)) as ddy55cert_conv
	,ddy55qlty as ddy55qlty
	,ltrim(rtrim(ddy55qlty)) as ddy55qlty_conv
	,cast(ddy55zdrcd as [NVARCHAR](3)) as ddy55zdrcd
FROM 
    stg_jde.tmp_f554901_init
OPTION ( LABEL = 'CREATE_rep_jde.f554901_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554901_sys_integration_id ON rep_jde.f554901_new(sys_integration_id); 
