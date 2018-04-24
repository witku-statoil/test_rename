-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4071_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4071_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4071_cdc
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
	,cast(atast as [NVARCHAR](8)) as atast
	,cast(atprgr as [NVARCHAR](8)) as atprgr
	,cast(atcpgp as [NVARCHAR](8)) as atcpgp
	,cast(atsdgr as [NVARCHAR](8)) as atsdgr
	,cast(atprfr as [NVARCHAR](2)) as atprfr
	,cast(atlbt as [NVARCHAR](1)) as atlbt
	,atglc as atglc
	,ltrim(rtrim(atglc)) as atglc_conv
	,cast(atsbif as [NVARCHAR](1)) as atsbif
	,cast(atacnt as [NVARCHAR](1)) as atacnt
	,atlnty as atlnty
	,ltrim(rtrim(atlnty)) as atlnty_conv
	,atmded as atmded
	,ltrim(rtrim(atmded)) as atmded_conv
	,atabas as atabas
	,ltrim(rtrim(atabas)) as atabas_conv
	,cast(atolvl as [NVARCHAR](1)) as atolvl
	,cast(attxb as [NVARCHAR](1)) as attxb
	,cast(atpa01 as [NVARCHAR](1)) as atpa01
	,atpa02 as atpa02
	,ltrim(rtrim(atpa02)) as atpa02_conv
	,atpa03 as atpa03
	,ltrim(rtrim(atpa03)) as atpa03_conv
	,cast(atpa04 as [NVARCHAR](1)) as atpa04
	,atpa05 as atpa05
	,ltrim(rtrim(atpa05)) as atpa05_conv
	,aturcd as aturcd
	,ltrim(rtrim(aturcd)) as aturcd_conv
	,cast(aturdt as [INT]) as aturdt
	,case when cast(aturdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aturdt as [INT]) %1000, dateadd(year, cast(aturdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aturdt_conv
	,cast(aturat as [DECIMAL](38, 4)) as aturat
	,cast(aturat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aturat_conv
	,cast(aturab as [DECIMAL](38, 4)) as aturab
	,cast(aturab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aturab_conv
	,aturrf as aturrf
	,ltrim(rtrim(aturrf)) as aturrf_conv
	,atenbm as atenbm
	,ltrim(rtrim(atenbm)) as atenbm_conv
	,atsrflag as atsrflag
	,ltrim(rtrim(atsrflag)) as atsrflag_conv
	,atusadj as atusadj
	,ltrim(rtrim(atusadj)) as atusadj_conv
	,cast(atatier as [DECIMAL](38, 4)) as atatier
	,cast(atbtier as [DECIMAL](38, 4)) as atbtier
	,cast(atbnad as [DECIMAL](38, 4)) as atbnad
	,cast(ataprp1 as [NVARCHAR](3)) as ataprp1
	,cast(ataprp2 as [NVARCHAR](3)) as ataprp2
	,cast(ataprp3 as [NVARCHAR](3)) as ataprp3
	,cast(ataprp4 as [NVARCHAR](6)) as ataprp4
	,cast(ataprp5 as [NVARCHAR](6)) as ataprp5
	,cast(ataprp6 as [NVARCHAR](6)) as ataprp6
	,cast(atadjgrp as [NVARCHAR](10)) as atadjgrp
	,atmeadj as atmeadj
	,ltrim(rtrim(atmeadj)) as atmeadj_conv
	,cast(atpdcl as [NVARCHAR](1)) as atpdcl
	,atuser as atuser
	,ltrim(rtrim(atuser)) as atuser_conv
	,atpid as atpid
	,ltrim(rtrim(atpid)) as atpid_conv
	,atjobn as atjobn
	,ltrim(rtrim(atjobn)) as atjobn_conv
	,cast(atupmj as [INT]) as atupmj
	,case when cast(atupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(atupmj as [INT]) %1000, dateadd(year, cast(atupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as atupmj_conv
	,cast(attday as [DECIMAL](38, 4)) as attday
	,atdidp as atdidp
	,ltrim(rtrim(atdidp)) as atdidp_conv
	,atpmtn as atpmtn
	,ltrim(rtrim(atpmtn)) as atpmtn_conv
	,atphst as atphst
	,ltrim(rtrim(atphst)) as atphst_conv
	,atpa06 as atpa06
	,ltrim(rtrim(atpa06)) as atpa06_conv
	,atpa07 as atpa07
	,ltrim(rtrim(atpa07)) as atpa07_conv
	,atpa08 as atpa08
	,ltrim(rtrim(atpa08)) as atpa08_conv
	,atpa09 as atpa09
	,ltrim(rtrim(atpa09)) as atpa09_conv
	,atpa10 as atpa10
	,ltrim(rtrim(atpa10)) as atpa10_conv
	,atefcn as atefcn
	,ltrim(rtrim(atefcn)) as atefcn_conv
	,cast(ataptype as [NVARCHAR](2)) as ataptype
	,atmoadj as atmoadj
	,ltrim(rtrim(atmoadj)) as atmoadj_conv
	,cast(atplgrp as [NVARCHAR](3)) as atplgrp
	,atexcpl as atexcpl
	,ltrim(rtrim(atexcpl)) as atexcpl_conv
	,cast(atupmx as [INT]) as atupmx
	,cast(atupmx as [INT]) / cast(1 as [INT]) as atupmx_conv
	,cast(atmnmxaj as [NVARCHAR](1)) as atmnmxaj
	,cast(atmnmxrl as [NVARCHAR](1)) as atmnmxrl
	,attstrsnm as attstrsnm
	,ltrim(rtrim(attstrsnm)) as attstrsnm_conv
	,atadjqty as atadjqty
	,ltrim(rtrim(atadjqty)) as atadjqty_conv
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
	,atast
	,atprgr
	,atcpgp
	,atsdgr
	,atprfr
	,atlbt
	,atglc
	,atsbif
	,atacnt
	,atlnty
	,atmded
	,atabas
	,atolvl
	,attxb
	,atpa01
	,atpa02
	,atpa03
	,atpa04
	,atpa05
	,aturcd
	,aturdt
	,aturat
	,aturab
	,aturrf
	,atenbm
	,atsrflag
	,atusadj
	,atatier
	,atbtier
	,atbnad
	,ataprp1
	,ataprp2
	,ataprp3
	,ataprp4
	,ataprp5
	,ataprp6
	,atadjgrp
	,atmeadj
	,atpdcl
	,atuser
	,atpid
	,atjobn
	,atupmj
	,attday
	,atdidp
	,atpmtn
	,atphst
	,atpa06
	,atpa07
	,atpa08
	,atpa09
	,atpa10
	,atefcn
	,ataptype
	,atmoadj
	,atplgrp
	,atexcpl
	,atupmx
	,atmnmxaj
	,atmnmxrl
	,attstrsnm
	,atadjqty,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4071_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4071_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4071_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4071_cdc(sys_integration_id); 
