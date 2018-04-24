-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f49219_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f49219_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f49219_cdc
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
	,cast(uddoco as [DECIMAL](38, 4)) as uddoco
	,cast(uddcto as [NVARCHAR](2)) as uddcto
	,udkcoo as udkcoo
	,ltrim(rtrim(udkcoo)) as udkcoo_conv
	,cast(udlnid as [DECIMAL](38, 4)) as udlnid
	,cast(udlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as udlnid_conv
	,udvmcu as udvmcu
	,ltrim(rtrim(udvmcu)) as udvmcu_conv
	,cast(udtrp as [DECIMAL](38, 4)) as udtrp
	,cast(udload as [INT]) as udload
	,case when cast(udload as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udload as [INT]) %1000, dateadd(year, cast(udload as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udload_conv
	,cast(uddsgp as [NVARCHAR](3)) as uddsgp
	,cast(udbpfg as [NVARCHAR](1)) as udbpfg
	,cast(uddstn as [DECIMAL](38, 4)) as uddstn
	,cast(udum as [NVARCHAR](2)) as udum
	,uddeff as uddeff
	,ltrim(rtrim(uddeff)) as uddeff_conv
	,cast(uddunc as [DECIMAL](38, 4)) as uddunc
	,cast(uddunc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as uddunc_conv
	,cast(udfduc as [DECIMAL](38, 4)) as udfduc
	,cast(udfduc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udfduc_conv
	,cast(uddrev as [DECIMAL](38, 4)) as uddrev
	,cast(uddrev as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as uddrev_conv
	,cast(udfdrv as [DECIMAL](38, 4)) as udfdrv
	,cast(udfdrv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udfdrv_conv
	,cast(udanum as [DECIMAL](38, 4)) as udanum
	,cast(udanum as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as udanum_conv
	,cast(udsidt as [INT]) as udsidt
	,case when cast(udsidt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udsidt as [INT]) %1000, dateadd(year, cast(udsidt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udsidt_conv
	,cast(udincy as [NVARCHAR](3)) as udincy
	,cast(udlddt as [INT]) as udlddt
	,case when cast(udlddt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udlddt as [INT]) %1000, dateadd(year, cast(udlddt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udlddt_conv
	,cast(udldtm as [DECIMAL](38, 4)) as udldtm
	,cast(uddcdt as [INT]) as uddcdt
	,case when cast(uddcdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(uddcdt as [INT]) %1000, dateadd(year, cast(uddcdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as uddcdt_conv
	,cast(udpcqy as [DECIMAL](38, 4)) as udpcqy
	,cast(udpcqy as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udpcqy_conv
	,cast(uduomc as [NVARCHAR](2)) as uduomc
	,cast(udtemp as [DECIMAL](38, 4)) as udtemp
	,cast(udtemp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as udtemp_conv
	,cast(udstpu as [NVARCHAR](1)) as udstpu
	,cast(uddnty as [DECIMAL](38, 4)) as uddnty
	,cast(uddnty as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as uddnty_conv
	,cast(uddntp as [NVARCHAR](1)) as uddntp
	,cast(uddetp as [DECIMAL](38, 4)) as uddetp
	,cast(uddetp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as uddetp_conv
	,cast(uddtpu as [NVARCHAR](1)) as uddtpu
	,cast(udvcf as [DECIMAL](38, 4)) as udvcf
	,cast(udvcf as [DECIMAL](38, 5)) / cast(100000 as [DECIMAL](38, 5)) as udvcf_conv
	,cast(udpras as [NVARCHAR](1)) as udpras
	,cast(udcp01 as [NVARCHAR](1)) as udcp01
	,cast(udiqty as [DECIMAL](38, 4)) as udiqty
	,cast(udiqty as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udiqty_conv
	,cast(udstum as [DECIMAL](38, 4)) as udstum
	,cast(udstum as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udstum_conv
	,cast(udbum6 as [NVARCHAR](2)) as udbum6
	,cast(udambr as [DECIMAL](38, 4)) as udambr
	,cast(udambr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udambr_conv
	,cast(udbum3 as [NVARCHAR](2)) as udbum3
	,cast(udwgtr as [DECIMAL](38, 4)) as udwgtr
	,cast(udwgtr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udwgtr_conv
	,cast(udbum5 as [NVARCHAR](2)) as udbum5
	,udfrtv as udfrtv
	,ltrim(rtrim(udfrtv)) as udfrtv_conv
	,udfrtd as udfrtd
	,ltrim(rtrim(udfrtd)) as udfrtd_conv
	,cast(udfrcc as [DECIMAL](38, 4)) as udfrcc
	,cast(udfrcc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udfrcc_conv
	,cast(udfrvc as [DECIMAL](38, 4)) as udfrvc
	,cast(udfrvc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udfrvc_conv
	,udpveh as udpveh
	,ltrim(rtrim(udpveh)) as udpveh_conv
	,udrlno as udrlno
	,ltrim(rtrim(udrlno)) as udrlno_conv
	,udmcur as udmcur
	,ltrim(rtrim(udmcur)) as udmcur_conv
	,udfltn as udfltn
	,ltrim(rtrim(udfltn)) as udfltn_conv
	,cast(uddsnn as [NVARCHAR](5)) as uddsnn
	,cast(udarct as [NVARCHAR](5)) as udarct
	,cast(udsorg as [NVARCHAR](15)) as udsorg
	,cast(udeltm as [DECIMAL](38, 4)) as udeltm
	,cast(udptnr as [DECIMAL](38, 4)) as udptnr
	,cast(udian8 as [DECIMAL](38, 4)) as udian8
	,udptc as udptc
	,ltrim(rtrim(udptc)) as udptc_conv
	,cast(uddoc as [DECIMAL](38, 4)) as uddoc
	,cast(uddct as [NVARCHAR](2)) as uddct
	,udkco as udkco
	,ltrim(rtrim(udkco)) as udkco_conv
	,cast(udcrr as [DECIMAL](38, 4)) as udcrr
	,udcrcd as udcrcd
	,ltrim(rtrim(udcrcd)) as udcrcd_conv
	,udtxa1 as udtxa1
	,ltrim(rtrim(udtxa1)) as udtxa1_conv
	,cast(udexr1 as [NVARCHAR](2)) as udexr1
	,udtv01 as udtv01
	,ltrim(rtrim(udtv01)) as udtv01_conv
	,udtv02 as udtv02
	,ltrim(rtrim(udtv02)) as udtv02_conv
	,udtv03 as udtv03
	,ltrim(rtrim(udtv03)) as udtv03_conv
	,udtv04 as udtv04
	,ltrim(rtrim(udtv04)) as udtv04_conv
	,udtv05 as udtv05
	,ltrim(rtrim(udtv05)) as udtv05_conv
	,udtvcd as udtvcd
	,ltrim(rtrim(udtvcd)) as udtvcd_conv
	,cast(udtvqt as [DECIMAL](38, 4)) as udtvqt
	,cast(udtvdt as [INT]) as udtvdt
	,case when cast(udtvdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udtvdt as [INT]) %1000, dateadd(year, cast(udtvdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udtvdt_conv
	,cast(udtvum as [NVARCHAR](2)) as udtvum
	,uduser as uduser
	,ltrim(rtrim(uduser)) as uduser_conv
	,udpid as udpid
	,ltrim(rtrim(udpid)) as udpid_conv
	,udjobn as udjobn
	,ltrim(rtrim(udjobn)) as udjobn_conv
	,cast(udupmj as [INT]) as udupmj
	,case when cast(udupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udupmj as [INT]) %1000, dateadd(year, cast(udupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udupmj_conv
	,cast(udtday as [DECIMAL](38, 4)) as udtday
	,udsbck as udsbck
	,ltrim(rtrim(udsbck)) as udsbck_conv
	,udedck as udedck
	,ltrim(rtrim(udedck)) as udedck_conv
	,cast(udcmdm as [NVARCHAR](1)) as udcmdm
	,udbbck as udbbck
	,ltrim(rtrim(udbbck)) as udbbck_conv
	,cast(udrqsj as [INT]) as udrqsj
	,case when cast(udrqsj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udrqsj as [INT]) %1000, dateadd(year, cast(udrqsj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udrqsj_conv
	,cast(udpsdj as [INT]) as udpsdj
	,case when cast(udpsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udpsdj as [INT]) %1000, dateadd(year, cast(udpsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udpsdj_conv
	,cast(udadlj as [INT]) as udadlj
	,case when cast(udadlj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(udadlj as [INT]) %1000, dateadd(year, cast(udadlj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as udadlj_conv
	,udsub as udsub
	,ltrim(rtrim(udsub)) as udsub_conv
	,udstts as udstts
	,ltrim(rtrim(udstts)) as udstts_conv
	,udratt as udratt
	,ltrim(rtrim(udratt)) as udratt_conv
	,cast(udfuf1 as [NVARCHAR](1)) as udfuf1
	,udfrtc as udfrtc
	,ltrim(rtrim(udfrtc)) as udfrtc_conv
	,cast(udfrat as [NVARCHAR](10)) as udfrat
	,udaft as udaft
	,ltrim(rtrim(udaft)) as udaft_conv
	,udomcu as udomcu
	,ltrim(rtrim(udomcu)) as udomcu_conv
	,udobj as udobj
	,ltrim(rtrim(udobj)) as udobj_conv
	,cast(udlt as [NVARCHAR](2)) as udlt
	,udfapp as udfapp
	,ltrim(rtrim(udfapp)) as udfapp_conv
	,cast(uddspr as [DECIMAL](38, 4)) as uddspr
	,cast(uddspr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as uddspr_conv
	,cast(uddsft as [NVARCHAR](1)) as uddsft
	,uddmt1 as uddmt1
	,ltrim(rtrim(uddmt1)) as uddmt1_conv
	,cast(uddms1 as [DECIMAL](38, 4)) as uddms1
	,cast(udcot as [NVARCHAR](3)) as udcot
	,cast(udcmgl as [NVARCHAR](1)) as udcmgl
	,udbalu as udbalu
	,ltrim(rtrim(udbalu)) as udbalu_conv
	,cast(udapot as [DECIMAL](38, 4)) as udapot
	,cast(udapot as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as udapot_conv
	,cast(udacgd as [NVARCHAR](3)) as udacgd
	,udani as udani
	,ltrim(rtrim(udani)) as udani_conv
	,udaid as udaid
	,ltrim(rtrim(udaid)) as udaid_conv
	,cast(udopol as [DECIMAL](38, 4)) as udopol
	,cast(udopol as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as udopol_conv
	,cast(udopbo as [NVARCHAR](30)) as udopbo
	,udopid as udopid
	,ltrim(rtrim(udopid)) as udopid_conv
	,cast(udopcs as [DECIMAL](38, 4)) as udopcs
	,cast(udopcs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as udopcs_conv
	,udopll as udopll
	,ltrim(rtrim(udopll)) as udopll_conv
	,udopms as udopms
	,ltrim(rtrim(udopms)) as udopms_conv
	,udopss as udopss
	,ltrim(rtrim(udopss)) as udopss_conv
	,udopba as udopba
	,ltrim(rtrim(udopba)) as udopba_conv
	,udfxsr as udfxsr
	,ltrim(rtrim(udfxsr)) as udfxsr_conv
	,udpsjobno as udpsjobno
	,ltrim(rtrim(udpsjobno)) as udpsjobno_conv
	,udjobsq as udjobsq
	,ltrim(rtrim(udjobsq)) as udjobsq_conv
	,cast(udcardno as [NVARCHAR](4)) as udcardno
	,uddelbatch as uddelbatch
	,ltrim(rtrim(uddelbatch)) as uddelbatch_conv
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
	,uddoco
	,uddcto
	,udkcoo
	,udlnid
	,udvmcu
	,udtrp
	,udload
	,uddsgp
	,udbpfg
	,uddstn
	,udum
	,uddeff
	,uddunc
	,udfduc
	,uddrev
	,udfdrv
	,udanum
	,udsidt
	,udincy
	,udlddt
	,udldtm
	,uddcdt
	,udpcqy
	,uduomc
	,udtemp
	,udstpu
	,uddnty
	,uddntp
	,uddetp
	,uddtpu
	,udvcf
	,udpras
	,udcp01
	,udiqty
	,udstum
	,udbum6
	,udambr
	,udbum3
	,udwgtr
	,udbum5
	,udfrtv
	,udfrtd
	,udfrcc
	,udfrvc
	,udpveh
	,udrlno
	,udmcur
	,udfltn
	,uddsnn
	,udarct
	,udsorg
	,udeltm
	,udptnr
	,udian8
	,udptc
	,uddoc
	,uddct
	,udkco
	,udcrr
	,udcrcd
	,udtxa1
	,udexr1
	,udtv01
	,udtv02
	,udtv03
	,udtv04
	,udtv05
	,udtvcd
	,udtvqt
	,udtvdt
	,udtvum
	,uduser
	,udpid
	,udjobn
	,udupmj
	,udtday
	,udsbck
	,udedck
	,udcmdm
	,udbbck
	,udrqsj
	,udpsdj
	,udadlj
	,udsub
	,udstts
	,udratt
	,udfuf1
	,udfrtc
	,udfrat
	,udaft
	,udomcu
	,udobj
	,udlt
	,udfapp
	,uddspr
	,uddsft
	,uddmt1
	,uddms1
	,udcot
	,udcmgl
	,udbalu
	,udapot
	,udacgd
	,udani
	,udaid
	,udopol
	,udopbo
	,udopid
	,udopcs
	,udopll
	,udopms
	,udopss
	,udopba
	,udfxsr
	,udpsjobno
	,udjobsq
	,udcardno
	,uddelbatch,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f49219_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f49219_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f49219_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f49219_cdc(sys_integration_id); 
