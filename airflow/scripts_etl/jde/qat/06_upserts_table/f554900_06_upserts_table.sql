-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f554900_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f554900_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f554900_cdc
WITH 
(
    DISTRIBUTION = HASH(sys_integration_id) 
    ,HEAP
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
	,dhvr01 as dhvr01
	,ltrim(rtrim(dhvr01)) as dhvr01_conv
	,cast(dhy55msgno as [DECIMAL](38, 4)) as dhy55msgno
	,cast(dhy55msgno as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhy55msgno_conv
	,cast(dhy55insdj as [INT]) as dhy55insdj
	,case when cast(dhy55insdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhy55insdj as [INT]) %1000, dateadd(year, cast(dhy55insdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhy55insdj_conv
	,cast(dhy55insdt as [DECIMAL](38, 4)) as dhy55insdt
	,cast(dhnstp as [DECIMAL](38, 4)) as dhnstp
	,cast(dhnstp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhnstp_conv
	,dhpveh as dhpveh
	,ltrim(rtrim(dhpveh)) as dhpveh_conv
	,cast(dhnbrord as [DECIMAL](38, 4)) as dhnbrord
	,cast(dhnbrord as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhnbrord_conv
	,cast(dhy55dstt as [DECIMAL](38, 4)) as dhy55dstt
	,cast(dhy55dsnt as [DECIMAL](38, 4)) as dhy55dsnt
	,cast(dhumd1 as [NVARCHAR](2)) as dhumd1
	,cast(dhy55eltm as [DECIMAL](38, 4)) as dhy55eltm
	,cast(dhy55odrtm as [DECIMAL](38, 4)) as dhy55odrtm
	,cast(dhy55odltm as [DECIMAL](38, 4)) as dhy55odltm
	,cast(dhy55oldtm as [DECIMAL](38, 4)) as dhy55oldtm
	,cast(dhy55tsdj as [INT]) as dhy55tsdj
	,case when cast(dhy55tsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhy55tsdj as [INT]) %1000, dateadd(year, cast(dhy55tsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhy55tsdj_conv
	,cast(dhy55tsdm as [DECIMAL](38, 4)) as dhy55tsdm
	,cast(dhdlda as [INT]) as dhdlda
	,case when cast(dhdlda as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhdlda as [INT]) %1000, dateadd(year, cast(dhdlda as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhdlda_conv
	,cast(dhtdlf as [DECIMAL](38, 4)) as dhtdlf
	,cast(dhy55tdlf as [DECIMAL](38, 4)) as dhy55tdlf
	,cast(dhdldl as [INT]) as dhdldl
	,case when cast(dhdldl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhdldl as [INT]) %1000, dateadd(year, cast(dhdldl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhdldl_conv
	,cast(dhtdlt as [DECIMAL](38, 4)) as dhtdlt
	,cast(dhy55tdlt as [DECIMAL](38, 4)) as dhy55tdlt
	,cast(dhy55tedj as [INT]) as dhy55tedj
	,case when cast(dhy55tedj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhy55tedj as [INT]) %1000, dateadd(year, cast(dhy55tedj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhy55tedj_conv
	,cast(dhy55tetm as [DECIMAL](38, 4)) as dhy55tetm
	,cast(dhldnm as [DECIMAL](38, 4)) as dhldnm
	,cast(dhldls as [NVARCHAR](2)) as dhldls
	,dhy55ercod as dhy55ercod
	,ltrim(rtrim(dhy55ercod)) as dhy55ercod_conv
	,dhy55eflg as dhy55eflg
	,ltrim(rtrim(dhy55eflg)) as dhy55eflg_conv
	,cast(dhy55tdlq as [DECIMAL](38, 4)) as dhy55tdlq
	,cast(dhy55tdlq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dhy55tdlq_conv
	,cast(dhuom as [NVARCHAR](2)) as dhuom
	,cast(dhy55ldno as [DECIMAL](38, 4)) as dhy55ldno
	,cast(dhy55ldno as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhy55ldno_conv
	,cast(dhy55mnor as [DECIMAL](38, 4)) as dhy55mnor
	,dhy55mnstp as dhy55mnstp
	,ltrim(rtrim(dhy55mnstp)) as dhy55mnstp_conv
	,cast(dhy55pdno as [DECIMAL](38, 4)) as dhy55pdno
	,cast(dhy55zdno as [DECIMAL](38, 4)) as dhy55zdno
	,cast(dhy55rdno as [DECIMAL](38, 4)) as dhy55rdno
	,cast(dhy55ntat as [DECIMAL](38, 4)) as dhy55ntat
	,cast(dhy55ntat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhy55ntat_conv
	,cast(dhy55wttm as [DECIMAL](38, 4)) as dhy55wttm
	,cast(dhy55odstm as [DECIMAL](38, 4)) as dhy55odstm
	,cast(dhy55oumds as [NVARCHAR](2)) as dhy55oumds
	,cast(dhy55disn1 as [DECIMAL](38, 4)) as dhy55disn1
	,cast(dhy55stmcu as [DECIMAL](38, 4)) as dhy55stmcu
	,cast(dhy55etmcu as [DECIMAL](38, 4)) as dhy55etmcu
	,cast(dhy55bflg as [NVARCHAR](1)) as dhy55bflg
	,cast(dhy55drtm as [DECIMAL](38, 4)) as dhy55drtm
	,cast(dhy55mctm as [DECIMAL](38, 4)) as dhy55mctm
	,cast(dhstfn as [DECIMAL](38, 4)) as dhstfn
	,cast(dhy55wktm as [DECIMAL](38, 4)) as dhy55wktm
	,cast(dhwactime as [DECIMAL](38, 4)) as dhwactime
	,cast(dhwactime as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhwactime_conv
	,dhappflg as dhappflg
	,ltrim(rtrim(dhappflg)) as dhappflg_conv
	,dhbhuser as dhbhuser
	,ltrim(rtrim(dhbhuser)) as dhbhuser_conv
	,cast(dhapdj as [INT]) as dhapdj
	,case when cast(dhapdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhapdj as [INT]) %1000, dateadd(year, cast(dhapdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhapdj_conv
	,cast(dhatim as [DECIMAL](38, 4)) as dhatim
	,cast(dhy55ackm as [DECIMAL](38, 4)) as dhy55ackm
	,cast(dhy55gpskm as [DECIMAL](38, 4)) as dhy55gpskm
	,dhrlno as dhrlno
	,ltrim(rtrim(dhrlno)) as dhrlno_conv
	,dhtlnum as dhtlnum
	,ltrim(rtrim(dhtlnum)) as dhtlnum_conv
	,cast(dhy55ldtm as [DECIMAL](38, 4)) as dhy55ldtm
	,cast(dhy55drcd as [DECIMAL](38, 4)) as dhy55drcd
	,cast(dhy55drcd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhy55drcd_conv
	,dhy55rbflg as dhy55rbflg
	,ltrim(rtrim(dhy55rbflg)) as dhy55rbflg_conv
	,dhy55glfg as dhy55glfg
	,ltrim(rtrim(dhy55glfg)) as dhy55glfg_conv
	,dhy55glap as dhy55glap
	,ltrim(rtrim(dhy55glap)) as dhy55glap_conv
	,cast(dhy55srce as [NVARCHAR](12)) as dhy55srce
	,dhedbt as dhedbt
	,ltrim(rtrim(dhedbt)) as dhedbt_conv
	,dhedtn as dhedtn
	,ltrim(rtrim(dhedtn)) as dhedtn_conv
	,cast(dhy55spdl as [DECIMAL](38, 4)) as dhy55spdl
	,cast(dhy55spdl as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhy55spdl_conv
	,cast(dhcars as [DECIMAL](38, 4)) as dhcars
	,cast(dhcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhcars_conv
	,dhflag as dhflag
	,ltrim(rtrim(dhflag)) as dhflag_conv
	,dhuser as dhuser
	,ltrim(rtrim(dhuser)) as dhuser_conv
	,dhpid as dhpid
	,ltrim(rtrim(dhpid)) as dhpid_conv
	,dhjobn as dhjobn
	,ltrim(rtrim(dhjobn)) as dhjobn_conv
	,cast(dhupmj as [INT]) as dhupmj
	,case when cast(dhupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhupmj as [INT]) %1000, dateadd(year, cast(dhupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhupmj_conv
	,cast(dhupmt as [DECIMAL](38, 4)) as dhupmt
	,dhurcd as dhurcd
	,ltrim(rtrim(dhurcd)) as dhurcd_conv
	,cast(dhurdt as [INT]) as dhurdt
	,case when cast(dhurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhurdt as [INT]) %1000, dateadd(year, cast(dhurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhurdt_conv
	,cast(dhurat as [DECIMAL](38, 4)) as dhurat
	,cast(dhurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhurat_conv
	,cast(dhurab as [DECIMAL](38, 4)) as dhurab
	,cast(dhurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhurab_conv
	,dhurrf as dhurrf
	,ltrim(rtrim(dhurrf)) as dhurrf_conv
	,cast(dhrcd as [NVARCHAR](3)) as dhrcd
	,dhy55char1 as dhy55char1
	,ltrim(rtrim(dhy55char1)) as dhy55char1_conv
	,dhy55char2 as dhy55char2
	,ltrim(rtrim(dhy55char2)) as dhy55char2_conv
	,cast(dhy55date1 as [INT]) as dhy55date1
	,case when cast(dhy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhy55date1 as [INT]) %1000, dateadd(year, cast(dhy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhy55date1_conv
	,cast(dhy55date2 as [INT]) as dhy55date2
	,case when cast(dhy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhy55date2 as [INT]) %1000, dateadd(year, cast(dhy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhy55date2_conv
	,dhy55strg1 as dhy55strg1
	,ltrim(rtrim(dhy55strg1)) as dhy55strg1_conv
	,dhy55strg2 as dhy55strg2
	,ltrim(rtrim(dhy55strg2)) as dhy55strg2_conv
	,dhy55strg3 as dhy55strg3
	,ltrim(rtrim(dhy55strg3)) as dhy55strg3_conv
	,dhy55strg4 as dhy55strg4
	,ltrim(rtrim(dhy55strg4)) as dhy55strg4_conv
	,dhy55strg5 as dhy55strg5
	,ltrim(rtrim(dhy55strg5)) as dhy55strg5_conv
	,dhy55strg6 as dhy55strg6
	,ltrim(rtrim(dhy55strg6)) as dhy55strg6_conv
	,dhy55strg7 as dhy55strg7
	,ltrim(rtrim(dhy55strg7)) as dhy55strg7_conv
	,dhy55strg8 as dhy55strg8
	,ltrim(rtrim(dhy55strg8)) as dhy55strg8_conv
	,dhy55strg9 as dhy55strg9
	,ltrim(rtrim(dhy55strg9)) as dhy55strg9_conv
	,dhy55strg10 as dhy55strg10
	,ltrim(rtrim(dhy55strg10)) as dhy55strg10_conv
	,dhy55flg1 as dhy55flg1
	,ltrim(rtrim(dhy55flg1)) as dhy55flg1_conv
	,dhy55flg2 as dhy55flg2
	,ltrim(rtrim(dhy55flg2)) as dhy55flg2_conv
	,cast(dhy55num3 as [DECIMAL](38, 4)) as dhy55num3
	,cast(dhy55num3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dhy55num3_conv
	,cast(dhy55num4 as [DECIMAL](38, 4)) as dhy55num4
	,cast(dhy55num4 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dhy55num4_conv
	,cast(dhy55time1 as [DECIMAL](38, 4)) as dhy55time1
	,cast(dhy55time2 as [DECIMAL](38, 4)) as dhy55time2
	,cast(dhy55amnt1 as [DECIMAL](38, 4)) as dhy55amnt1
	,cast(dhy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhy55amnt1_conv
	,cast(dhy55amnt2 as [DECIMAL](38, 4)) as dhy55amnt2
	,cast(dhy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhy55amnt2_conv
	,cast(dhum as [NVARCHAR](2)) as dhum
	,cast(dhhrsact as [DECIMAL](38, 4)) as dhhrsact
	,cast(dhhrsact as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhhrsact_conv
	,dhflg2 as dhflg2
	,ltrim(rtrim(dhflg2)) as dhflg2_conv
	,dhflg3 as dhflg3
	,ltrim(rtrim(dhflg3)) as dhflg3_conv
	,dhflg4 as dhflg4
	,ltrim(rtrim(dhflg4)) as dhflg4_conv
	,dhvmcu as dhvmcu
	,ltrim(rtrim(dhvmcu)) as dhvmcu_conv
	,cast(dhy55disn as [DECIMAL](38, 4)) as dhy55disn
	,cast(dhefr as [DECIMAL](38, 4)) as dhefr
	,cast(dhefr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhefr_conv
	,cast(dhhrwt as [DECIMAL](38, 4)) as dhhrwt
	,cast(dhhrwt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhhrwt_conv
	,cast(dhaltd as [DECIMAL](38, 4)) as dhaltd
	,cast(dhaltd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhaltd_conv
	,cast(dhdrtm as [DECIMAL](38, 4)) as dhdrtm
	,cast(dhdrtm as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dhdrtm_conv
	,dhflg1 as dhflg1
	,ltrim(rtrim(dhflg1)) as dhflg1_conv
	,cast(dhapsfact as [DECIMAL](38, 4)) as dhapsfact
	,cast(dhapsfact as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhapsfact_conv
	,cast(dhprqu as [DECIMAL](38, 4)) as dhprqu
	,cast(dhprqu as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dhprqu_conv
	,cast(dhuom1 as [NVARCHAR](2)) as dhuom1
	,cast(dhmc1 as [DECIMAL](38, 4)) as dhmc1
	,cast(dhmc1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhmc1_conv
	,cast(dhtcap as [DECIMAL](38, 4)) as dhtcap
	,cast(dhtcap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhtcap_conv
	,cast(dhot1 as [DECIMAL](38, 4)) as dhot1
	,cast(dhot1 as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as dhot1_conv
	,dhcrcp as dhcrcp
	,ltrim(rtrim(dhcrcp)) as dhcrcp_conv
	,cast(dhpraa as [DECIMAL](38, 4)) as dhpraa
	,cast(dhpraa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhpraa_conv
	,cast(dhaoth as [DECIMAL](38, 4)) as dhaoth
	,cast(dhaoth as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhaoth_conv
	,cast(dhchgamt as [DECIMAL](38, 4)) as dhchgamt
	,cast(dhchgamt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhchgamt_conv
	,cast(dhnbr3 as [DECIMAL](38, 4)) as dhnbr3
	,cast(dhnbr2 as [DECIMAL](38, 4)) as dhnbr2
	,cast(dhnbr1 as [DECIMAL](38, 4)) as dhnbr1
	,cast(dhnocm as [DECIMAL](38, 4)) as dhnocm
	,cast(dhnocm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhnocm_conv
	,cast(dhnbrsld as [DECIMAL](38, 4)) as dhnbrsld
	,cast(dhnbrsld as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhnbrsld_conv
	,cast(dhtold as [DECIMAL](38, 4)) as dhtold
	,cast(dhtold as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhtold_conv
	,cast(dhodst as [DECIMAL](38, 4)) as dhodst
	,cast(dhsclq as [DECIMAL](38, 4)) as dhsclq
	,cast(dhsclq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dhsclq_conv
	,cast(dhy55trtm as [DECIMAL](38, 4)) as dhy55trtm
	,cast(dhorgn as [DECIMAL](38, 4)) as dhorgn
	,cast(dhtrdt as [INT]) as dhtrdt
	,case when cast(dhtrdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhtrdt as [INT]) %1000, dateadd(year, cast(dhtrdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhtrdt_conv
	,cast(dhsttme as [DECIMAL](38, 4)) as dhsttme
	,cast(dhstdate as [INT]) as dhstdate
	,case when cast(dhstdate as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhstdate as [INT]) %1000, dateadd(year, cast(dhstdate as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhstdate_conv
	,cast(dhdisn as [DECIMAL](38, 4)) as dhdisn
	,dhdtai as dhdtai
	,ltrim(rtrim(dhdtai)) as dhdtai_conv
	,cast(dhy55elts2 as [DECIMAL](38, 4)) as dhy55elts2
	,cast(dhy55elts1 as [DECIMAL](38, 4)) as dhy55elts1
	,cast(dhy55elts as [DECIMAL](38, 4)) as dhy55elts
	,cast(dhtme0 as [DECIMAL](38, 4)) as dhtme0
	,cast(dhacttime as [DECIMAL](38, 4)) as dhacttime
	,cast(dhacttime as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhacttime_conv
	,cast(dhbstn as [DECIMAL](38, 4)) as dhbstn
	,cast(dhbstn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhbstn_conv
	,cast(dhancc as [DECIMAL](38, 4)) as dhancc
	,cast(dhdte as [INT]) as dhdte
	,case when cast(dhdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dhdte as [INT]) %1000, dateadd(year, cast(dhdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dhdte_conv
	,cast(dhukid as [DECIMAL](38, 4)) as dhukid
	,cast(dhxpitraid as [DECIMAL](38, 4)) as dhxpitraid
	,cast(dhxpitraid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhxpitraid_conv
	,cast(dhy55vg as [NVARCHAR](10)) as dhy55vg
	,dhvtyp as dhvtyp
	,ltrim(rtrim(dhvtyp)) as dhvtyp_conv
	,dhy55rm1 as dhy55rm1
	,ltrim(rtrim(dhy55rm1)) as dhy55rm1_conv
	,dhy55rm2 as dhy55rm2
	,ltrim(rtrim(dhy55rm2)) as dhy55rm2_conv
	,dhy55stter as dhy55stter
	,ltrim(rtrim(dhy55stter)) as dhy55stter_conv
	,dhy55edter as dhy55edter
	,ltrim(rtrim(dhy55edter)) as dhy55edter_conv
	,cast(dhy55wrcd as [NVARCHAR](3)) as dhy55wrcd
	,dhy55sapflg as dhy55sapflg
	,ltrim(rtrim(dhy55sapflg)) as dhy55sapflg_conv
	,cast(dhy55mnobsp as [NVARCHAR](1)) as dhy55mnobsp
	,cast(dhy55mcth as [DECIMAL](38, 4)) as dhy55mcth
	,cast(dhy55mcth as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhy55mcth_conv
	,cast(dhy55wttmh as [DECIMAL](38, 4)) as dhy55wttmh
	,cast(dhy55wttmh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhy55wttmh_conv
	,cast(dhy55wktmh as [DECIMAL](38, 4)) as dhy55wktmh
	,cast(dhy55wktmh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dhy55wktmh_conv
	,cast(dhy55nstp as [DECIMAL](38, 4)) as dhy55nstp
	,cast(dhy55nstp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dhy55nstp_conv
FROM (
    SELECT 
        	sys_file_name
	,sys_file_ln
	,sys_integration_id
	,sys_extract_dt
	,sys_cdc_dt
	,sys_cdc_scn
	,sys_cdc_operation_type
	,sys_cdc_before_after
	,sys_line_modified_ind
	,dhvr01
	,dhy55msgno
	,dhy55insdj
	,dhy55insdt
	,dhnstp
	,dhpveh
	,dhnbrord
	,dhy55dstt
	,dhy55dsnt
	,dhumd1
	,dhy55eltm
	,dhy55odrtm
	,dhy55odltm
	,dhy55oldtm
	,dhy55tsdj
	,dhy55tsdm
	,dhdlda
	,dhtdlf
	,dhy55tdlf
	,dhdldl
	,dhtdlt
	,dhy55tdlt
	,dhy55tedj
	,dhy55tetm
	,dhldnm
	,dhldls
	,dhy55ercod
	,dhy55eflg
	,dhy55tdlq
	,dhuom
	,dhy55ldno
	,dhy55mnor
	,dhy55mnstp
	,dhy55pdno
	,dhy55zdno
	,dhy55rdno
	,dhy55ntat
	,dhy55wttm
	,dhy55odstm
	,dhy55oumds
	,dhy55disn1
	,dhy55stmcu
	,dhy55etmcu
	,dhy55bflg
	,dhy55drtm
	,dhy55mctm
	,dhstfn
	,dhy55wktm
	,dhwactime
	,dhappflg
	,dhbhuser
	,dhapdj
	,dhatim
	,dhy55ackm
	,dhy55gpskm
	,dhrlno
	,dhtlnum
	,dhy55ldtm
	,dhy55drcd
	,dhy55rbflg
	,dhy55glfg
	,dhy55glap
	,dhy55srce
	,dhedbt
	,dhedtn
	,dhy55spdl
	,dhcars
	,dhflag
	,dhuser
	,dhpid
	,dhjobn
	,dhupmj
	,dhupmt
	,dhurcd
	,dhurdt
	,dhurat
	,dhurab
	,dhurrf
	,dhrcd
	,dhy55char1
	,dhy55char2
	,dhy55date1
	,dhy55date2
	,dhy55strg1
	,dhy55strg2
	,dhy55strg3
	,dhy55strg4
	,dhy55strg5
	,dhy55strg6
	,dhy55strg7
	,dhy55strg8
	,dhy55strg9
	,dhy55strg10
	,dhy55flg1
	,dhy55flg2
	,dhy55num3
	,dhy55num4
	,dhy55time1
	,dhy55time2
	,dhy55amnt1
	,dhy55amnt2
	,dhum
	,dhhrsact
	,dhflg2
	,dhflg3
	,dhflg4
	,dhvmcu
	,dhy55disn
	,dhefr
	,dhhrwt
	,dhaltd
	,dhdrtm
	,dhflg1
	,dhapsfact
	,dhprqu
	,dhuom1
	,dhmc1
	,dhtcap
	,dhot1
	,dhcrcp
	,dhpraa
	,dhaoth
	,dhchgamt
	,dhnbr3
	,dhnbr2
	,dhnbr1
	,dhnocm
	,dhnbrsld
	,dhtold
	,dhodst
	,dhsclq
	,dhy55trtm
	,dhorgn
	,dhtrdt
	,dhsttme
	,dhstdate
	,dhdisn
	,dhdtai
	,dhy55elts2
	,dhy55elts1
	,dhy55elts
	,dhtme0
	,dhacttime
	,dhbstn
	,dhancc
	,dhdte
	,dhukid
	,dhxpitraid
	,dhy55vg
	,dhvtyp
	,dhy55rm1
	,dhy55rm2
	,dhy55stter
	,dhy55edter
	,dhy55wrcd
	,dhy55sapflg
	,dhy55mnobsp
	,dhy55mcth
	,dhy55wttmh
	,dhy55wktmh
	,dhy55nstp,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f554900_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f554900_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554900_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f554900_cdc(sys_integration_id); 
