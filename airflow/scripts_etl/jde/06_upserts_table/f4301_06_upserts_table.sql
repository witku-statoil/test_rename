-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4301_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4301_cdc


CREATE TABLE stg_jde.tmp_upsert_f4301_cdc
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
	,phkcoo as phkcoo
	,ltrim(rtrim(phkcoo)) as phkcoo_conv
	,cast(phdoco as [DECIMAL](38, 4)) as phdoco
	,cast(phdcto as [NVARCHAR](2)) as phdcto
	,phsfxo as phsfxo
	,ltrim(rtrim(phsfxo)) as phsfxo_conv
	,phmcu as phmcu
	,ltrim(rtrim(phmcu)) as phmcu_conv
	,phokco as phokco
	,ltrim(rtrim(phokco)) as phokco_conv
	,phoorn as phoorn
	,ltrim(rtrim(phoorn)) as phoorn_conv
	,cast(phocto as [NVARCHAR](2)) as phocto
	,phrkco as phrkco
	,ltrim(rtrim(phrkco)) as phrkco_conv
	,phrorn as phrorn
	,ltrim(rtrim(phrorn)) as phrorn_conv
	,cast(phrcto as [NVARCHAR](2)) as phrcto
	,cast(phan8 as [DECIMAL](38, 4)) as phan8
	,cast(phshan as [DECIMAL](38, 4)) as phshan
	,cast(phdrqj as [INT]) as phdrqj
	,case when cast(phdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phdrqj as [INT]) %1000, dateadd(year, cast(phdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phdrqj_conv
	,cast(phtrdj as [INT]) as phtrdj
	,case when cast(phtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phtrdj as [INT]) %1000, dateadd(year, cast(phtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phtrdj_conv
	,cast(phpddj as [INT]) as phpddj
	,case when cast(phpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phpddj as [INT]) %1000, dateadd(year, cast(phpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phpddj_conv
	,cast(phopdj as [INT]) as phopdj
	,case when cast(phopdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phopdj as [INT]) %1000, dateadd(year, cast(phopdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phopdj_conv
	,cast(phaddj as [INT]) as phaddj
	,case when cast(phaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phaddj as [INT]) %1000, dateadd(year, cast(phaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phaddj_conv
	,cast(phcndj as [INT]) as phcndj
	,case when cast(phcndj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phcndj as [INT]) %1000, dateadd(year, cast(phcndj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phcndj_conv
	,cast(phpefj as [INT]) as phpefj
	,case when cast(phpefj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phpefj as [INT]) %1000, dateadd(year, cast(phpefj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phpefj_conv
	,cast(phppdj as [INT]) as phppdj
	,case when cast(phppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phppdj as [INT]) %1000, dateadd(year, cast(phppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phppdj_conv
	,cast(phpsdj as [INT]) as phpsdj
	,case when cast(phpsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phpsdj as [INT]) %1000, dateadd(year, cast(phpsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phpsdj_conv
	,phvr01 as phvr01
	,ltrim(rtrim(phvr01)) as phvr01_conv
	,phvr02 as phvr02
	,ltrim(rtrim(phvr02)) as phvr02_conv
	,phdel1 as phdel1
	,ltrim(rtrim(phdel1)) as phdel1_conv
	,phdel2 as phdel2
	,ltrim(rtrim(phdel2)) as phdel2_conv
	,phrmk as phrmk
	,ltrim(rtrim(phrmk)) as phrmk_conv
	,phdesc as phdesc
	,ltrim(rtrim(phdesc)) as phdesc_conv
	,cast(phinmg as [NVARCHAR](10)) as phinmg
	,cast(phasn as [NVARCHAR](8)) as phasn
	,cast(phprgp as [NVARCHAR](8)) as phprgp
	,phptc as phptc
	,ltrim(rtrim(phptc)) as phptc_conv
	,cast(phexr1 as [NVARCHAR](2)) as phexr1
	,phtxa1 as phtxa1
	,ltrim(rtrim(phtxa1)) as phtxa1_conv
	,phtxct as phtxct
	,ltrim(rtrim(phtxct)) as phtxct_conv
	,cast(phhold as [NVARCHAR](2)) as phhold
	,phatxt as phatxt
	,ltrim(rtrim(phatxt)) as phatxt_conv
	,cast(phinvc as [DECIMAL](38, 4)) as phinvc
	,cast(phinvc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as phinvc_conv
	,cast(phntr as [NVARCHAR](2)) as phntr
	,phcnid as phcnid
	,ltrim(rtrim(phcnid)) as phcnid_conv
	,cast(phfrth as [NVARCHAR](3)) as phfrth
	,cast(phzon as [NVARCHAR](3)) as phzon
	,cast(phanby as [DECIMAL](38, 4)) as phanby
	,cast(phancr as [DECIMAL](38, 4)) as phancr
	,cast(phmot as [NVARCHAR](3)) as phmot
	,cast(phcot as [NVARCHAR](3)) as phcot
	,cast(phrcd as [NVARCHAR](3)) as phrcd
	,phfrtc as phfrtc
	,ltrim(rtrim(phfrtc)) as phfrtc_conv
	,cast(phfuf1 as [NVARCHAR](1)) as phfuf1
	,phfuf2 as phfuf2
	,ltrim(rtrim(phfuf2)) as phfuf2_conv
	,cast(photot as [DECIMAL](38, 4)) as photot
	,cast(photot as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as photot_conv
	,cast(phpcrt as [DECIMAL](38, 4)) as phpcrt
	,cast(phpcrt as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as phpcrt_conv
	,phrtnr as phrtnr
	,ltrim(rtrim(phrtnr)) as phrtnr_conv
	,cast(phwumd as [NVARCHAR](2)) as phwumd
	,cast(phvumd as [NVARCHAR](2)) as phvumd
	,phpurg as phpurg
	,ltrim(rtrim(phpurg)) as phpurg_conv
	,phlgct as phlgct
	,ltrim(rtrim(phlgct)) as phlgct_conv
	,cast(phprom as [NVARCHAR](1)) as phprom
	,cast(phmaty as [NVARCHAR](1)) as phmaty
	,phosts as phosts
	,ltrim(rtrim(phosts)) as phosts_conv
	,cast(phavch as [NVARCHAR](1)) as phavch
	,phprpy as phprpy
	,ltrim(rtrim(phprpy)) as phprpy_conv
	,cast(phcrmd as [NVARCHAR](1)) as phcrmd
	,cast(phprp5 as [NVARCHAR](3)) as phprp5
	,phartg as phartg
	,ltrim(rtrim(phartg)) as phartg_conv
	,cast(phcord as [DECIMAL](38, 4)) as phcord
	,cast(phcrrm as [NVARCHAR](1)) as phcrrm
	,phcrcd as phcrcd
	,ltrim(rtrim(phcrcd)) as phcrcd_conv
	,cast(phcrr as [DECIMAL](38, 4)) as phcrr
	,cast(phlngp as [NVARCHAR](2)) as phlngp
	,cast(phfap as [DECIMAL](38, 4)) as phfap
	,cast(phfap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as phfap_conv
	,phorby as phorby
	,ltrim(rtrim(phorby)) as phorby_conv
	,phtkby as phtkby
	,ltrim(rtrim(phtkby)) as phtkby_conv
	,phurcd as phurcd
	,ltrim(rtrim(phurcd)) as phurcd_conv
	,cast(phurdt as [INT]) as phurdt
	,case when cast(phurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phurdt as [INT]) %1000, dateadd(year, cast(phurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phurdt_conv
	,cast(phurat as [DECIMAL](38, 4)) as phurat
	,cast(phurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as phurat_conv
	,cast(phurab as [DECIMAL](38, 4)) as phurab
	,cast(phurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as phurab_conv
	,phurrf as phurrf
	,ltrim(rtrim(phurrf)) as phurrf_conv
	,phuser as phuser
	,ltrim(rtrim(phuser)) as phuser_conv
	,phpid as phpid
	,ltrim(rtrim(phpid)) as phpid_conv
	,phjobn as phjobn
	,ltrim(rtrim(phjobn)) as phjobn_conv
	,cast(phupmj as [INT]) as phupmj
	,case when cast(phupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phupmj as [INT]) %1000, dateadd(year, cast(phupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phupmj_conv
	,cast(phtday as [DECIMAL](38, 4)) as phtday
	,cast(phrsht as [DECIMAL](38, 4)) as phrsht
	,cast(phrsht as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as phrsht_conv
	,cast(phdrqt as [DECIMAL](38, 4)) as phdrqt
	,cast(phdoc1 as [DECIMAL](38, 4)) as phdoc1
	,cast(phdct4 as [NVARCHAR](2)) as phdct4
	,phbcrc as phbcrc
	,ltrim(rtrim(phbcrc)) as phbcrc_conv
	,cast(phmkfr as [DECIMAL](38, 4)) as phmkfr
	,phpohp01 as phpohp01
	,ltrim(rtrim(phpohp01)) as phpohp01_conv
	,phpohp02 as phpohp02
	,ltrim(rtrim(phpohp02)) as phpohp02_conv
	,phpohp03 as phpohp03
	,ltrim(rtrim(phpohp03)) as phpohp03_conv
	,phpohp04 as phpohp04
	,ltrim(rtrim(phpohp04)) as phpohp04_conv
	,phpohp05 as phpohp05
	,ltrim(rtrim(phpohp05)) as phpohp05_conv
	,phpohp06 as phpohp06
	,ltrim(rtrim(phpohp06)) as phpohp06_conv
	,phpohp07 as phpohp07
	,ltrim(rtrim(phpohp07)) as phpohp07_conv
	,phpohp08 as phpohp08
	,ltrim(rtrim(phpohp08)) as phpohp08_conv
	,phpohp09 as phpohp09
	,ltrim(rtrim(phpohp09)) as phpohp09_conv
	,phpohp10 as phpohp10
	,ltrim(rtrim(phpohp10)) as phpohp10_conv
	,phpohp11 as phpohp11
	,ltrim(rtrim(phpohp11)) as phpohp11_conv
	,phpohp12 as phpohp12
	,ltrim(rtrim(phpohp12)) as phpohp12_conv
	,cast(phpohc01 as [NVARCHAR](3)) as phpohc01
	,cast(phpohc02 as [NVARCHAR](3)) as phpohc02
	,cast(phpohc03 as [NVARCHAR](3)) as phpohc03
	,cast(phpohc04 as [NVARCHAR](3)) as phpohc04
	,cast(phpohc05 as [NVARCHAR](3)) as phpohc05
	,cast(phpohc06 as [NVARCHAR](3)) as phpohc06
	,cast(phpohc07 as [NVARCHAR](10)) as phpohc07
	,cast(phpohc08 as [NVARCHAR](10)) as phpohc08
	,cast(phpohc09 as [NVARCHAR](10)) as phpohc09
	,cast(phpohc10 as [NVARCHAR](10)) as phpohc10
	,cast(phpohc11 as [NVARCHAR](10)) as phpohc11
	,cast(phpohc12 as [NVARCHAR](10)) as phpohc12
	,cast(phpohd01 as [INT]) as phpohd01
	,case when cast(phpohd01 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phpohd01 as [INT]) %1000, dateadd(year, cast(phpohd01 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phpohd01_conv
	,cast(phpohd02 as [INT]) as phpohd02
	,case when cast(phpohd02 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(phpohd02 as [INT]) %1000, dateadd(year, cast(phpohd02 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as phpohd02_conv
	,cast(phpohab01 as [DECIMAL](38, 4)) as phpohab01
	,cast(phpohab01 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as phpohab01_conv
	,cast(phpohab02 as [DECIMAL](38, 4)) as phpohab02
	,cast(phpohab02 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as phpohab02_conv
	,cast(phcukid as [DECIMAL](38, 4)) as phcukid
	,cast(phcukid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as phcukid_conv
	,phpohp13 as phpohp13
	,ltrim(rtrim(phpohp13)) as phpohp13_conv
	,phpohu01 as phpohu01
	,phpohu01 as phpohu01_conv
	,phpohu02 as phpohu02
	,phpohu02 as phpohu02_conv
	,phreti as phreti
	,ltrim(rtrim(phreti)) as phreti_conv
	,cast(phclass01 as [NVARCHAR](3)) as phclass01
	,cast(phclass02 as [NVARCHAR](3)) as phclass02
	,cast(phclass03 as [NVARCHAR](3)) as phclass03
	,cast(phclass04 as [NVARCHAR](3)) as phclass04
	,cast(phclass05 as [NVARCHAR](3)) as phclass05
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
	,phkcoo
	,phdoco
	,phdcto
	,phsfxo
	,phmcu
	,phokco
	,phoorn
	,phocto
	,phrkco
	,phrorn
	,phrcto
	,phan8
	,phshan
	,phdrqj
	,phtrdj
	,phpddj
	,phopdj
	,phaddj
	,phcndj
	,phpefj
	,phppdj
	,phpsdj
	,phvr01
	,phvr02
	,phdel1
	,phdel2
	,phrmk
	,phdesc
	,phinmg
	,phasn
	,phprgp
	,phptc
	,phexr1
	,phtxa1
	,phtxct
	,phhold
	,phatxt
	,phinvc
	,phntr
	,phcnid
	,phfrth
	,phzon
	,phanby
	,phancr
	,phmot
	,phcot
	,phrcd
	,phfrtc
	,phfuf1
	,phfuf2
	,photot
	,phpcrt
	,phrtnr
	,phwumd
	,phvumd
	,phpurg
	,phlgct
	,phprom
	,phmaty
	,phosts
	,phavch
	,phprpy
	,phcrmd
	,phprp5
	,phartg
	,phcord
	,phcrrm
	,phcrcd
	,phcrr
	,phlngp
	,phfap
	,phorby
	,phtkby
	,phurcd
	,phurdt
	,phurat
	,phurab
	,phurrf
	,phuser
	,phpid
	,phjobn
	,phupmj
	,phtday
	,phrsht
	,phdrqt
	,phdoc1
	,phdct4
	,phbcrc
	,phmkfr
	,phpohp01
	,phpohp02
	,phpohp03
	,phpohp04
	,phpohp05
	,phpohp06
	,phpohp07
	,phpohp08
	,phpohp09
	,phpohp10
	,phpohp11
	,phpohp12
	,phpohc01
	,phpohc02
	,phpohc03
	,phpohc04
	,phpohc05
	,phpohc06
	,phpohc07
	,phpohc08
	,phpohc09
	,phpohc10
	,phpohc11
	,phpohc12
	,phpohd01
	,phpohd02
	,phpohab01
	,phpohab02
	,phcukid
	,phpohp13
	,phpohu01
	,phpohu02
	,phreti
	,phclass01
	,phclass02
	,phclass03
	,phclass04
	,phclass05,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4301_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4301_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4301_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4301_cdc(sys_integration_id); 
