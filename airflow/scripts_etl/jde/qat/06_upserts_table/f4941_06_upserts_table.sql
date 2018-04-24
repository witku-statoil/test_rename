-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4941_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4941_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4941_cdc
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
	,cast(rsshpn as [DECIMAL](38, 4)) as rsshpn
	,cast(rsrssn as [DECIMAL](38, 4)) as rsrssn
	,cast(rsmot as [NVARCHAR](3)) as rsmot
	,rsovrm as rsovrm
	,ltrim(rtrim(rsovrm)) as rsovrm_conv
	,cast(rscars as [DECIMAL](38, 4)) as rscars
	,cast(rscars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rscars_conv
	,rsovrc as rsovrc
	,ltrim(rtrim(rsovrc)) as rsovrc_conv
	,cast(rsrout as [NVARCHAR](3)) as rsrout
	,cast(rsrtn as [DECIMAL](38, 4)) as rsrtn
	,cast(rsdlno as [DECIMAL](38, 4)) as rsdlno
	,cast(rsfrsc as [NVARCHAR](8)) as rsfrsc
	,cast(rsczon as [NVARCHAR](10)) as rsczon
	,rsvmcu as rsvmcu
	,ltrim(rtrim(rsvmcu)) as rsvmcu_conv
	,cast(rsldnm as [DECIMAL](38, 4)) as rsldnm
	,cast(rstrpl as [DECIMAL](38, 4)) as rstrpl
	,cast(rstrpl as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rstrpl_conv
	,cast(rsstsq as [DECIMAL](38, 4)) as rsstsq
	,cast(rsanid as [DECIMAL](38, 4)) as rsanid
	,cast(rsorgn as [DECIMAL](38, 4)) as rsorgn
	,cast(rsancc as [DECIMAL](38, 4)) as rsancc
	,cast(rswgts as [DECIMAL](38, 4)) as rswgts
	,cast(rswgts as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rswgts_conv
	,cast(rswtum as [NVARCHAR](2)) as rswtum
	,cast(rsscvl as [DECIMAL](38, 4)) as rsscvl
	,cast(rsscvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rsscvl_conv
	,cast(rsvlum as [NVARCHAR](2)) as rsvlum
	,cast(rslgts as [DECIMAL](38, 4)) as rslgts
	,cast(rslgts as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rslgts_conv
	,cast(rswths as [DECIMAL](38, 4)) as rswths
	,cast(rswths as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rswths_conv
	,cast(rshgts as [DECIMAL](38, 4)) as rshgts
	,cast(rshgts as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rshgts_conv
	,cast(rsgths as [DECIMAL](38, 4)) as rsgths
	,cast(rsgths as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsgths_conv
	,cast(rsluom as [NVARCHAR](2)) as rsluom
	,cast(rsnpcs as [DECIMAL](38, 4)) as rsnpcs
	,cast(rsnpcs as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rsnpcs_conv
	,cast(rsnctr as [DECIMAL](38, 4)) as rsnctr
	,cast(rsnctr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rsnctr_conv
	,cast(rsccub as [DECIMAL](38, 4)) as rsccub
	,cast(rsccub as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsccub_conv
	,cast(rsdstn as [DECIMAL](38, 4)) as rsdstn
	,cast(rsumd1 as [NVARCHAR](2)) as rsumd1
	,cast(rsdsrc as [NVARCHAR](1)) as rsdsrc
	,cast(rseltm as [DECIMAL](38, 4)) as rseltm
	,cast(rsum as [NVARCHAR](2)) as rsum
	,cast(rsaexp as [DECIMAL](38, 4)) as rsaexp
	,cast(rsaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsaexp_conv
	,cast(rsfea as [DECIMAL](38, 4)) as rsfea
	,cast(rsfea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsfea_conv
	,rscrcd as rscrcd
	,ltrim(rtrim(rscrcd)) as rscrcd_conv
	,cast(rsecst as [DECIMAL](38, 4)) as rsecst
	,cast(rsecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsecst_conv
	,cast(rsppdj as [INT]) as rsppdj
	,case when cast(rsppdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsppdj as [INT]) %1000, dateadd(year, cast(rsppdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsppdj_conv
	,cast(rspmdt as [DECIMAL](38, 4)) as rspmdt
	,cast(rsrsdj as [INT]) as rsrsdj
	,case when cast(rsrsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsrsdj as [INT]) %1000, dateadd(year, cast(rsrsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsrsdj_conv
	,cast(rsrsdt as [DECIMAL](38, 4)) as rsrsdt
	,cast(rslddt as [INT]) as rslddt
	,case when cast(rslddt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rslddt as [INT]) %1000, dateadd(year, cast(rslddt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rslddt_conv
	,cast(rsldtm as [DECIMAL](38, 4)) as rsldtm
	,cast(rsaddj as [INT]) as rsaddj
	,case when cast(rsaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsaddj as [INT]) %1000, dateadd(year, cast(rsaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsaddj_conv
	,cast(rsadtm as [DECIMAL](38, 4)) as rsadtm
	,cast(rsdldt as [INT]) as rsdldt
	,case when cast(rsdldt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsdldt as [INT]) %1000, dateadd(year, cast(rsdldt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsdldt_conv
	,cast(rsdltm as [DECIMAL](38, 4)) as rsdltm
	,cast(rsrrtr as [NVARCHAR](1)) as rsrrtr
	,cast(rsratr as [NVARCHAR](1)) as rsratr
	,rsdpcr as rsdpcr
	,ltrim(rtrim(rsdpcr)) as rsdpcr_conv
	,cast(rsrefq as [NVARCHAR](2)) as rsrefq
	,rsrefn as rsrefn
	,ltrim(rtrim(rsrefn)) as rsrefn_conv
	,rsfrtd as rsfrtd
	,ltrim(rtrim(rsfrtd)) as rsfrtd_conv
	,rsfrtv as rsfrtv
	,ltrim(rtrim(rsfrtv)) as rsfrtv_conv
	,cast(rsfrvc as [DECIMAL](38, 4)) as rsfrvc
	,cast(rsfrvc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rsfrvc_conv
	,cast(rsfrvf as [DECIMAL](38, 4)) as rsfrvf
	,cast(rsfrvf as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rsfrvf_conv
	,rscrcp as rscrcp
	,ltrim(rtrim(rscrcp)) as rscrcp_conv
	,cast(rsfrcc as [DECIMAL](38, 4)) as rsfrcc
	,cast(rsfrcc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rsfrcc_conv
	,cast(rsfrcf as [DECIMAL](38, 4)) as rsfrcf
	,cast(rsfrcf as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rsfrcf_conv
	,rscrdc as rscrdc
	,ltrim(rtrim(rscrdc)) as rscrdc_conv
	,rsintf as rsintf
	,ltrim(rtrim(rsintf)) as rsintf_conv
	,rsibrs as rsibrs
	,ltrim(rtrim(rsibrs)) as rsibrs_conv
	,cast(rslslt as [DECIMAL](38, 4)) as rslslt
	,cast(rslsut as [DECIMAL](38, 4)) as rslsut
	,cast(rslalt as [DECIMAL](38, 4)) as rslalt
	,cast(rslaut as [DECIMAL](38, 4)) as rslaut
	,cast(rstpuf as [DECIMAL](38, 4)) as rstpuf
	,cast(rstput as [DECIMAL](38, 4)) as rstput
	,cast(rstdlf as [DECIMAL](38, 4)) as rstdlf
	,cast(rstdlt as [DECIMAL](38, 4)) as rstdlt
	,cast(rsdepu as [INT]) as rsdepu
	,case when cast(rsdepu as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsdepu as [INT]) %1000, dateadd(year, cast(rsdepu as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsdepu_conv
	,cast(rsdlpu as [INT]) as rsdlpu
	,case when cast(rsdlpu as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsdlpu as [INT]) %1000, dateadd(year, cast(rsdlpu as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsdlpu_conv
	,cast(rsdedl as [INT]) as rsdedl
	,case when cast(rsdedl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsdedl as [INT]) %1000, dateadd(year, cast(rsdedl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsdedl_conv
	,cast(rsdldl as [INT]) as rsdldl
	,case when cast(rsdldl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsdldl as [INT]) %1000, dateadd(year, cast(rsdldl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsdldl_conv
	,rsdkid as rsdkid
	,ltrim(rtrim(rsdkid)) as rsdkid_conv
	,cast(rsprnb as [DECIMAL](38, 4)) as rsprnb
	,rsprte as rsprte
	,ltrim(rtrim(rsprte)) as rsprte_conv
	,cast(rslnmb as [DECIMAL](38, 4)) as rslnmb
	,rscnmr as rscnmr
	,ltrim(rtrim(rscnmr)) as rscnmr_conv
	,rsurcd as rsurcd
	,ltrim(rtrim(rsurcd)) as rsurcd_conv
	,cast(rsurdt as [INT]) as rsurdt
	,case when cast(rsurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsurdt as [INT]) %1000, dateadd(year, cast(rsurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsurdt_conv
	,cast(rsurat as [DECIMAL](38, 4)) as rsurat
	,cast(rsurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rsurat_conv
	,cast(rsurab as [DECIMAL](38, 4)) as rsurab
	,cast(rsurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rsurab_conv
	,rsurrf as rsurrf
	,ltrim(rtrim(rsurrf)) as rsurrf_conv
	,rsuser as rsuser
	,ltrim(rtrim(rsuser)) as rsuser_conv
	,rspid as rspid
	,ltrim(rtrim(rspid)) as rspid_conv
	,rsjobn as rsjobn
	,ltrim(rtrim(rsjobn)) as rsjobn_conv
	,cast(rsupmj as [INT]) as rsupmj
	,case when cast(rsupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rsupmj as [INT]) %1000, dateadd(year, cast(rsupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rsupmj_conv
	,cast(rstday as [DECIMAL](38, 4)) as rstday
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
	,rsshpn
	,rsrssn
	,rsmot
	,rsovrm
	,rscars
	,rsovrc
	,rsrout
	,rsrtn
	,rsdlno
	,rsfrsc
	,rsczon
	,rsvmcu
	,rsldnm
	,rstrpl
	,rsstsq
	,rsanid
	,rsorgn
	,rsancc
	,rswgts
	,rswtum
	,rsscvl
	,rsvlum
	,rslgts
	,rswths
	,rshgts
	,rsgths
	,rsluom
	,rsnpcs
	,rsnctr
	,rsccub
	,rsdstn
	,rsumd1
	,rsdsrc
	,rseltm
	,rsum
	,rsaexp
	,rsfea
	,rscrcd
	,rsecst
	,rsppdj
	,rspmdt
	,rsrsdj
	,rsrsdt
	,rslddt
	,rsldtm
	,rsaddj
	,rsadtm
	,rsdldt
	,rsdltm
	,rsrrtr
	,rsratr
	,rsdpcr
	,rsrefq
	,rsrefn
	,rsfrtd
	,rsfrtv
	,rsfrvc
	,rsfrvf
	,rscrcp
	,rsfrcc
	,rsfrcf
	,rscrdc
	,rsintf
	,rsibrs
	,rslslt
	,rslsut
	,rslalt
	,rslaut
	,rstpuf
	,rstput
	,rstdlf
	,rstdlt
	,rsdepu
	,rsdlpu
	,rsdedl
	,rsdldl
	,rsdkid
	,rsprnb
	,rsprte
	,rslnmb
	,rscnmr
	,rsurcd
	,rsurdt
	,rsurat
	,rsurab
	,rsurrf
	,rsuser
	,rspid
	,rsjobn
	,rsupmj
	,rstday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4941_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4941_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4941_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4941_cdc(sys_integration_id); 
