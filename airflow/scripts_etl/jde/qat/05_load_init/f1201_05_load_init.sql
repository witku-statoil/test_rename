-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f1201_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f1201_new


CREATE TABLE rep_jde_qat.f1201_new
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
	,faco as faco
	,ltrim(rtrim(faco)) as faco_conv
	,cast(fanumb as [DECIMAL](38, 4)) as fanumb
	,faapid as faapid
	,ltrim(rtrim(faapid)) as faapid_conv
	,cast(faaaid as [DECIMAL](38, 4)) as faaaid
	,faasid as faasid
	,ltrim(rtrim(faasid)) as faasid_conv
	,cast(faseq as [DECIMAL](38, 4)) as faseq
	,cast(faacl1 as [NVARCHAR](3)) as faacl1
	,cast(faacl2 as [NVARCHAR](3)) as faacl2
	,cast(faacl3 as [NVARCHAR](3)) as faacl3
	,cast(faacl4 as [NVARCHAR](3)) as faacl4
	,cast(faacl5 as [NVARCHAR](3)) as faacl5
	,famcu as famcu
	,ltrim(rtrim(famcu)) as famcu_conv
	,fadl01 as fadl01
	,ltrim(rtrim(fadl01)) as fadl01_conv
	,fadl02 as fadl02
	,ltrim(rtrim(fadl02)) as fadl02_conv
	,fadl03 as fadl03
	,ltrim(rtrim(fadl03)) as fadl03_conv
	,fadscc as fadscc
	,ltrim(rtrim(fadscc)) as fadscc_conv
	,cast(fadaj as [INT]) as fadaj
	,case when cast(fadaj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fadaj as [INT]) %1000, dateadd(year, cast(fadaj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fadaj_conv
	,cast(fadsp as [INT]) as fadsp
	,case when cast(fadsp as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fadsp as [INT]) %1000, dateadd(year, cast(fadsp as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fadsp_conv
	,cast(faeqst as [NVARCHAR](2)) as faeqst
	,cast(fanoru as [NVARCHAR](1)) as fanoru
	,cast(faaesv as [DECIMAL](38, 4)) as faaesv
	,cast(faaesv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faaesv_conv
	,cast(faarpc as [DECIMAL](38, 4)) as faarpc
	,cast(faarpc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faarpc_conv
	,cast(faalrc as [DECIMAL](38, 4)) as faalrc
	,cast(faalrc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faalrc_conv
	,faamcu as faamcu
	,ltrim(rtrim(faamcu)) as faamcu_conv
	,faaobj as faaobj
	,ltrim(rtrim(faaobj)) as faaobj_conv
	,faasub as faasub
	,ltrim(rtrim(faasub)) as faasub_conv
	,fadmcu as fadmcu
	,ltrim(rtrim(fadmcu)) as fadmcu_conv
	,fadobj as fadobj
	,ltrim(rtrim(fadobj)) as fadobj_conv
	,fadsub as fadsub
	,ltrim(rtrim(fadsub)) as fadsub_conv
	,faxmcu as faxmcu
	,ltrim(rtrim(faxmcu)) as faxmcu_conv
	,faxobj as faxobj
	,ltrim(rtrim(faxobj)) as faxobj_conv
	,faxsub as faxsub
	,ltrim(rtrim(faxsub)) as faxsub_conv
	,farmcu as farmcu
	,ltrim(rtrim(farmcu)) as farmcu_conv
	,farobj as farobj
	,ltrim(rtrim(farobj)) as farobj_conv
	,farsub as farsub
	,ltrim(rtrim(farsub)) as farsub_conv
	,cast(faarcq as [DECIMAL](38, 4)) as faarcq
	,cast(faarcq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faarcq_conv
	,cast(faaroq as [DECIMAL](38, 4)) as faaroq
	,cast(faaroq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faaroq_conv
	,cast(fatxjs as [DECIMAL](38, 4)) as fatxjs
	,cast(faaity as [DECIMAL](38, 4)) as faaity
	,cast(faaity as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faaity_conv
	,cast(faaitp as [DECIMAL](38, 4)) as faaitp
	,cast(faaitp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faaitp_conv
	,cast(fafinc as [NVARCHAR](1)) as fafinc
	,cast(faitco as [NVARCHAR](1)) as faitco
	,cast(fapuro as [NVARCHAR](1)) as fapuro
	,cast(faapur as [DECIMAL](38, 4)) as faapur
	,cast(faapur as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faapur_conv
	,cast(fapurp as [DECIMAL](38, 4)) as fapurp
	,cast(fapurp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as fapurp_conv
	,cast(faapom as [DECIMAL](38, 4)) as faapom
	,cast(faapom as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faapom_conv
	,cast(falano as [DECIMAL](38, 4)) as falano
	,cast(fajcd as [INT]) as fajcd
	,case when cast(fajcd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fajcd as [INT]) %1000, dateadd(year, cast(fajcd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fajcd_conv
	,cast(fadexj as [INT]) as fadexj
	,case when cast(fadexj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fadexj as [INT]) %1000, dateadd(year, cast(fadexj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fadexj_conv
	,cast(faamf as [DECIMAL](38, 4)) as faamf
	,cast(faamf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faamf_conv
	,farmk as farmk
	,ltrim(rtrim(farmk)) as farmk_conv
	,farmk2 as farmk2
	,ltrim(rtrim(farmk2)) as farmk2_conv
	,fainsp as fainsp
	,ltrim(rtrim(fainsp)) as fainsp_conv
	,fainsc as fainsc
	,ltrim(rtrim(fainsc)) as fainsc_conv
	,cast(fainsm as [DECIMAL](38, 4)) as fainsm
	,cast(fainsa as [DECIMAL](38, 4)) as fainsa
	,cast(fainsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fainsa_conv
	,cast(faaiv as [DECIMAL](38, 4)) as faaiv
	,cast(faaiv as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as faaiv_conv
	,cast(fainsi as [DECIMAL](38, 4)) as fainsi
	,fauser as fauser
	,ltrim(rtrim(fauser)) as fauser_conv
	,cast(falct as [INT]) as falct
	,case when cast(falct as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(falct as [INT]) %1000, dateadd(year, cast(falct as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as falct_conv
	,faloc as faloc
	,ltrim(rtrim(faloc)) as faloc_conv
	,cast(faadds as [NVARCHAR](3)) as faadds
	,fapid as fapid
	,ltrim(rtrim(fapid)) as fapid_conv
	,cast(faeftb as [INT]) as faeftb
	,case when cast(faeftb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(faeftb as [INT]) %1000, dateadd(year, cast(faeftb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as faeftb_conv
	,cast(fader as [INT]) as fader
	,case when cast(fader as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fader as [INT]) %1000, dateadd(year, cast(fader as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fader_conv
	,cast(famsga as [NVARCHAR](1)) as famsga
	,faex as faex
	,ltrim(rtrim(faex)) as faex_conv
	,faexr as faexr
	,ltrim(rtrim(faexr)) as faexr_conv
	,cast(faan8 as [DECIMAL](38, 4)) as faan8
	,cast(faacl6 as [NVARCHAR](3)) as faacl6
	,cast(faacl7 as [NVARCHAR](3)) as faacl7
	,cast(faacl8 as [NVARCHAR](3)) as faacl8
	,cast(faacl9 as [NVARCHAR](3)) as faacl9
	,cast(faacl0 as [NVARCHAR](3)) as faacl0
	,cast(fasfc as [DECIMAL](38, 4)) as fasfc
	,cast(fasfc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fasfc_conv
	,fadadc as fadadc
	,ltrim(rtrim(fadadc)) as fadadc_conv
	,fadado as fadado
	,ltrim(rtrim(fadado)) as fadado_conv
	,fadads as fadads
	,ltrim(rtrim(fadads)) as fadads_conv
	,faunit as faunit
	,ltrim(rtrim(faunit)) as faunit_conv
	,cast(fakit as [DECIMAL](38, 4)) as fakit
	,fakitl as fakitl
	,ltrim(rtrim(fakitl)) as fakitl_conv
	,faafe as faafe
	,ltrim(rtrim(faafe)) as faafe_conv
	,cast(fajbcd as [NVARCHAR](6)) as fajbcd
	,cast(fajbst as [NVARCHAR](4)) as fajbst
	,cast(faun as [NVARCHAR](6)) as faun
	,cast(fasbli as [NVARCHAR](1)) as fasbli
	,cast(faupmj as [INT]) as faupmj
	,case when cast(faupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(faupmj as [INT]) %1000, dateadd(year, cast(faupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as faupmj_conv
	,fajobn as fajobn
	,ltrim(rtrim(fajobn)) as fajobn_conv
	,cast(faupmt as [DECIMAL](38, 4)) as faupmt
	,cast(fafa0 as [NVARCHAR](3)) as fafa0
	,cast(fafa1 as [NVARCHAR](3)) as fafa1
	,cast(fafa2 as [NVARCHAR](3)) as fafa2
	,cast(fafa3 as [NVARCHAR](3)) as fafa3
	,cast(fafa4 as [NVARCHAR](3)) as fafa4
	,cast(fafa5 as [NVARCHAR](3)) as fafa5
	,cast(fafa6 as [NVARCHAR](3)) as fafa6
	,cast(fafa7 as [NVARCHAR](3)) as fafa7
	,cast(fafa8 as [NVARCHAR](3)) as fafa8
	,cast(fafa9 as [NVARCHAR](3)) as fafa9
	,cast(fafa21 as [NVARCHAR](10)) as fafa21
	,cast(fafa22 as [NVARCHAR](10)) as fafa22
	,cast(fafa23 as [NVARCHAR](10)) as fafa23
	,fawoyn as fawoyn
	,ltrim(rtrim(fawoyn)) as fawoyn_conv
	,cast(facrtl as [DECIMAL](38, 4)) as facrtl
	,cast(facrtl as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as facrtl_conv
	,cast(fawrfl as [NVARCHAR](1)) as fawrfl
	,cast(fawarj as [INT]) as fawarj
	,case when cast(fawarj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fawarj as [INT]) %1000, dateadd(year, cast(fawarj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fawarj_conv
FROM 
    stg_jde_qat.tmp_f1201_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f1201_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f1201_sys_integration_id ON rep_jde_qat.f1201_new(sys_integration_id); 
