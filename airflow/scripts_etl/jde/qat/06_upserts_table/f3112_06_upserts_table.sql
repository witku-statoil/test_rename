-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f3112_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f3112_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f3112_cdc
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
	,cast(wldoco as [DECIMAL](38, 4)) as wldoco
	,cast(wldcto as [NVARCHAR](2)) as wldcto
	,wlsfxo as wlsfxo
	,ltrim(rtrim(wlsfxo)) as wlsfxo_conv
	,cast(wltrt as [NVARCHAR](3)) as wltrt
	,cast(wlkit as [DECIMAL](38, 4)) as wlkit
	,wlkitl as wlkitl
	,ltrim(rtrim(wlkitl)) as wlkitl_conv
	,wlkita as wlkita
	,ltrim(rtrim(wlkita)) as wlkita_conv
	,wlmmcu as wlmmcu
	,ltrim(rtrim(wlmmcu)) as wlmmcu_conv
	,wlline as wlline
	,ltrim(rtrim(wlline)) as wlline_conv
	,wlmcu as wlmcu
	,ltrim(rtrim(wlmcu)) as wlmcu_conv
	,cast(wlald as [NVARCHAR](4)) as wlald
	,wldsc1 as wldsc1
	,ltrim(rtrim(wldsc1)) as wldsc1_conv
	,cast(wlopsq as [DECIMAL](38, 4)) as wlopsq
	,cast(wlopsq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlopsq_conv
	,cast(wlopsc as [NVARCHAR](2)) as wlopsc
	,cast(wlopst as [NVARCHAR](2)) as wlopst
	,wlinpe as wlinpe
	,ltrim(rtrim(wlinpe)) as wlinpe_conv
	,cast(wltimb as [NVARCHAR](1)) as wltimb
	,cast(wllamc as [NVARCHAR](1)) as wllamc
	,cast(wlbfpf as [NVARCHAR](1)) as wlbfpf
	,cast(wlpprf as [NVARCHAR](1)) as wlpprf
	,cast(wljbcd as [NVARCHAR](6)) as wljbcd
	,cast(wlan8 as [DECIMAL](38, 4)) as wlan8
	,cast(wlcrtr as [DECIMAL](38, 4)) as wlcrtr
	,cast(wlcrtr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlcrtr_conv
	,cast(wlsltr as [DECIMAL](38, 4)) as wlsltr
	,cast(wlsltr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlsltr_conv
	,cast(wltrdj as [INT]) as wltrdj
	,case when cast(wltrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wltrdj as [INT]) %1000, dateadd(year, cast(wltrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wltrdj_conv
	,cast(wldrqj as [INT]) as wldrqj
	,case when cast(wldrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wldrqj as [INT]) %1000, dateadd(year, cast(wldrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wldrqj_conv
	,cast(wlstrt as [INT]) as wlstrt
	,case when cast(wlstrt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wlstrt as [INT]) %1000, dateadd(year, cast(wlstrt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wlstrt_conv
	,cast(wlstrx as [INT]) as wlstrx
	,case when cast(wlstrx as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wlstrx as [INT]) %1000, dateadd(year, cast(wlstrx as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wlstrx_conv
	,wlrsft as wlrsft
	,ltrim(rtrim(wlrsft)) as wlrsft_conv
	,wlssft as wlssft
	,ltrim(rtrim(wlssft)) as wlssft_conv
	,wlcsft as wlcsft
	,ltrim(rtrim(wlcsft)) as wlcsft_conv
	,cast(wlltpc as [DECIMAL](38, 4)) as wlltpc
	,cast(wlltpc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlltpc_conv
	,cast(wlpovr as [DECIMAL](38, 4)) as wlpovr
	,cast(wlpovr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlpovr_conv
	,cast(wlopyp as [DECIMAL](38, 4)) as wlopyp
	,cast(wlopyp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlopyp_conv
	,cast(wlcpyp as [DECIMAL](38, 4)) as wlcpyp
	,cast(wlcpyp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcpyp_conv
	,cast(wlnxop as [DECIMAL](38, 4)) as wlnxop
	,cast(wlnxop as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlnxop_conv
	,cast(wlsetc as [DECIMAL](38, 4)) as wlsetc
	,cast(wlmovd as [DECIMAL](38, 4)) as wlmovd
	,cast(wlmovd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlmovd_conv
	,cast(wlqued as [DECIMAL](38, 4)) as wlqued
	,cast(wlqued as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlqued_conv
	,cast(wlrunm as [DECIMAL](38, 4)) as wlrunm
	,cast(wlrunm as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlrunm_conv
	,cast(wlrunl as [DECIMAL](38, 4)) as wlrunl
	,cast(wlrunl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlrunl_conv
	,cast(wlsetl as [DECIMAL](38, 4)) as wlsetl
	,cast(wlsetl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlsetl_conv
	,cast(wlmaca as [DECIMAL](38, 4)) as wlmaca
	,cast(wlmaca as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlmaca_conv
	,cast(wllaba as [DECIMAL](38, 4)) as wllaba
	,cast(wllaba as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wllaba_conv
	,cast(wlseta as [DECIMAL](38, 4)) as wlseta
	,cast(wlseta as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlseta_conv
	,cast(wlopsr as [DECIMAL](38, 4)) as wlopsr
	,cast(wlopsr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlopsr_conv
	,cast(wluorg as [DECIMAL](38, 4)) as wluorg
	,cast(wluorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wluorg_conv
	,cast(wlsocn as [DECIMAL](38, 4)) as wlsocn
	,cast(wlsocn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlsocn_conv
	,cast(wlsoqs as [DECIMAL](38, 4)) as wlsoqs
	,cast(wlsoqs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlsoqs_conv
	,cast(wlqmto as [DECIMAL](38, 4)) as wlqmto
	,cast(wlqmto as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlqmto_conv
	,cast(wlpwrt as [DECIMAL](38, 4)) as wlpwrt
	,cast(wlpwrt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wlpwrt_conv
	,cast(wluom as [NVARCHAR](2)) as wluom
	,cast(wlcts2 as [DECIMAL](38, 4)) as wlcts2
	,cast(wlcts2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlcts2_conv
	,cast(wlcts3 as [DECIMAL](38, 4)) as wlcts3
	,cast(wlcts3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlcts3_conv
	,cast(wlcts4 as [DECIMAL](38, 4)) as wlcts4
	,cast(wlcts4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcts4_conv
	,cast(wlcts5 as [DECIMAL](38, 4)) as wlcts5
	,cast(wlcts5 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcts5_conv
	,cast(wlcts6 as [DECIMAL](38, 4)) as wlcts6
	,cast(wlcts6 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcts6_conv
	,wlapid as wlapid
	,ltrim(rtrim(wlapid)) as wlapid_conv
	,wlshno as wlshno
	,ltrim(rtrim(wlshno)) as wlshno_conv
	,wlomcu as wlomcu
	,ltrim(rtrim(wlomcu)) as wlomcu_conv
	,wlobj as wlobj
	,ltrim(rtrim(wlobj)) as wlobj_conv
	,wlsub as wlsub
	,ltrim(rtrim(wlsub)) as wlsub_conv
	,cast(wlvend as [DECIMAL](38, 4)) as wlvend
	,cast(wlvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wlvend_conv
	,wlrkco as wlrkco
	,ltrim(rtrim(wlrkco)) as wlrkco_conv
	,wlrorn as wlrorn
	,ltrim(rtrim(wlrorn)) as wlrorn_conv
	,cast(wlrcto as [NVARCHAR](2)) as wlrcto
	,cast(wlrlln as [DECIMAL](38, 4)) as wlrlln
	,cast(wlrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wlrlln_conv
	,cast(wldct as [NVARCHAR](2)) as wldct
	,wlurcd as wlurcd
	,ltrim(rtrim(wlurcd)) as wlurcd_conv
	,cast(wlurdt as [INT]) as wlurdt
	,case when cast(wlurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wlurdt as [INT]) %1000, dateadd(year, cast(wlurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wlurdt_conv
	,cast(wlurat as [DECIMAL](38, 4)) as wlurat
	,cast(wlurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlurat_conv
	,wlurrf as wlurrf
	,ltrim(rtrim(wlurrf)) as wlurrf_conv
	,cast(wlurab as [DECIMAL](38, 4)) as wlurab
	,cast(wlurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wlurab_conv
	,wluser as wluser
	,ltrim(rtrim(wluser)) as wluser_conv
	,wlpid as wlpid
	,ltrim(rtrim(wlpid)) as wlpid_conv
	,wljobn as wljobn
	,ltrim(rtrim(wljobn)) as wljobn_conv
	,cast(wlupmj as [INT]) as wlupmj
	,case when cast(wlupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wlupmj as [INT]) %1000, dateadd(year, cast(wlupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wlupmj_conv
	,cast(wltday as [DECIMAL](38, 4)) as wltday
	,wllocn as wllocn
	,ltrim(rtrim(wllocn)) as wllocn_conv
	,cast(wlruc as [DECIMAL](38, 4)) as wlruc
	,cast(wlruc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlruc_conv
	,cast(wlcapu as [NVARCHAR](2)) as wlcapu
	,wlwmcu as wlwmcu
	,ltrim(rtrim(wlwmcu)) as wlwmcu_conv
	,cast(wlcmhr as [DECIMAL](38, 4)) as wlcmhr
	,cast(wlcmhr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcmhr_conv
	,cast(wlclhr as [DECIMAL](38, 4)) as wlclhr
	,cast(wlclhr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlclhr_conv
	,cast(wlcshr as [DECIMAL](38, 4)) as wlcshr
	,cast(wlcshr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcshr_conv
	,cast(wlcost as [NVARCHAR](3)) as wlcost
	,wlactb as wlactb
	,ltrim(rtrim(wlactb)) as wlactb_conv
	,cast(wlnumb as [DECIMAL](38, 4)) as wlnumb
	,cast(wlcts7 as [DECIMAL](38, 4)) as wlcts7
	,cast(wlcts7 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlcts7_conv
	,cast(wlcts8 as [DECIMAL](38, 4)) as wlcts8
	,cast(wlcts8 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlcts8_conv
	,cast(wlcts9 as [DECIMAL](38, 4)) as wlcts9
	,cast(wlcts9 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wlcts9_conv
	,cast(wllabp as [DECIMAL](38, 4)) as wllabp
	,cast(wllabp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wllabp_conv
	,cast(wlmacr as [DECIMAL](38, 4)) as wlmacr
	,cast(wlmacr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlmacr_conv
	,cast(wlsetr as [DECIMAL](38, 4)) as wlsetr
	,cast(wlsetr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wlsetr_conv
	,cast(wlapsc as [NVARCHAR](1)) as wlapsc
	,cast(wlsest as [DECIMAL](38, 4)) as wlsest
	,cast(wlseet as [DECIMAL](38, 4)) as wlseet
	,cast(wltmco as [DECIMAL](38, 4)) as wltmco
	,cast(wld2j as [INT]) as wld2j
	,case when cast(wld2j as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wld2j as [INT]) %1000, dateadd(year, cast(wld2j as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wld2j_conv
	,cast(wlstti as [DECIMAL](38, 4)) as wlstti
	,cast(wlcmpe as [NVARCHAR](3)) as wlcmpe
	,wlcmpc as wlcmpc
	,ltrim(rtrim(wlcmpc)) as wlcmpc_conv
	,cast(wlcplvlfr as [DECIMAL](38, 4)) as wlcplvlfr
	,cast(wlcplvlfr as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wlcplvlfr_conv
	,cast(wlcplvlto as [DECIMAL](38, 4)) as wlcplvlto
	,cast(wlcplvlto as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wlcplvlto_conv
	,wlcmrq as wlcmrq
	,ltrim(rtrim(wlcmrq)) as wlcmrq_conv
	,cast(wlansa as [DECIMAL](38, 4)) as wlansa
	,cast(wlanpa as [DECIMAL](38, 4)) as wlanpa
	,cast(wlanp as [DECIMAL](38, 4)) as wlanp
	,cast(wlwschf as [NVARCHAR](1)) as wlwschf
	,cast(wltraf as [NVARCHAR](1)) as wltraf
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
	,wldoco
	,wldcto
	,wlsfxo
	,wltrt
	,wlkit
	,wlkitl
	,wlkita
	,wlmmcu
	,wlline
	,wlmcu
	,wlald
	,wldsc1
	,wlopsq
	,wlopsc
	,wlopst
	,wlinpe
	,wltimb
	,wllamc
	,wlbfpf
	,wlpprf
	,wljbcd
	,wlan8
	,wlcrtr
	,wlsltr
	,wltrdj
	,wldrqj
	,wlstrt
	,wlstrx
	,wlrsft
	,wlssft
	,wlcsft
	,wlltpc
	,wlpovr
	,wlopyp
	,wlcpyp
	,wlnxop
	,wlsetc
	,wlmovd
	,wlqued
	,wlrunm
	,wlrunl
	,wlsetl
	,wlmaca
	,wllaba
	,wlseta
	,wlopsr
	,wluorg
	,wlsocn
	,wlsoqs
	,wlqmto
	,wlpwrt
	,wluom
	,wlcts2
	,wlcts3
	,wlcts4
	,wlcts5
	,wlcts6
	,wlapid
	,wlshno
	,wlomcu
	,wlobj
	,wlsub
	,wlvend
	,wlrkco
	,wlrorn
	,wlrcto
	,wlrlln
	,wldct
	,wlurcd
	,wlurdt
	,wlurat
	,wlurrf
	,wlurab
	,wluser
	,wlpid
	,wljobn
	,wlupmj
	,wltday
	,wllocn
	,wlruc
	,wlcapu
	,wlwmcu
	,wlcmhr
	,wlclhr
	,wlcshr
	,wlcost
	,wlactb
	,wlnumb
	,wlcts7
	,wlcts8
	,wlcts9
	,wllabp
	,wlmacr
	,wlsetr
	,wlapsc
	,wlsest
	,wlseet
	,wltmco
	,wld2j
	,wlstti
	,wlcmpe
	,wlcmpc
	,wlcplvlfr
	,wlcplvlto
	,wlcmrq
	,wlansa
	,wlanpa
	,wlanp
	,wlwschf
	,wltraf,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f3112_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f3112_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f3112_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f3112_cdc(sys_integration_id); 
