-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4321_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4321_cdc


CREATE TABLE stg_jde.tmp_upsert_f4321_cdc
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
	,pbkcoo as pbkcoo
	,ltrim(rtrim(pbkcoo)) as pbkcoo_conv
	,cast(pbdoco as [DECIMAL](38, 4)) as pbdoco
	,cast(pbdcto as [NVARCHAR](2)) as pbdcto
	,pbsfxo as pbsfxo
	,ltrim(rtrim(pbsfxo)) as pbsfxo_conv
	,cast(pblnid as [DECIMAL](38, 4)) as pblnid
	,cast(pblnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as pblnid_conv
	,pbdlan as pbdlan
	,ltrim(rtrim(pbdlan)) as pbdlan_conv
	,cast(pbcdf1 as [DECIMAL](38, 4)) as pbcdf1
	,cast(pbcdf1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbcdf1_conv
	,cast(pbcdf2 as [DECIMAL](38, 4)) as pbcdf2
	,cast(pbcdf2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbcdf2_conv
	,cast(pbcdf3 as [DECIMAL](38, 4)) as pbcdf3
	,cast(pbcdf3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbcdf3_conv
	,cast(pbcdf4 as [DECIMAL](38, 4)) as pbcdf4
	,cast(pbcdf4 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbcdf4_conv
	,cast(pbvsd as [DECIMAL](38, 4)) as pbvsd
	,cast(pbvsd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbvsd_conv
	,cast(pbvsw as [DECIMAL](38, 4)) as pbvsw
	,cast(pbvsw as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbvsw_conv
	,cast(pbvsm as [DECIMAL](38, 4)) as pbvsm
	,cast(pbvsm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbvsm_conv
	,cast(pbcrec as [DECIMAL](38, 4)) as pbcrec
	,cast(pbcrec as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbcrec_conv
	,cast(pbcfro as [DECIMAL](38, 4)) as pbcfro
	,cast(pbcfro as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbcfro_conv
	,cast(pbcfab as [DECIMAL](38, 4)) as pbcfab
	,cast(pbcfab as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbcfab_conv
	,cast(pbcraw as [DECIMAL](38, 4)) as pbcraw
	,cast(pbcraw as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbcraw_conv
	,cast(pbdrqj as [INT]) as pbdrqj
	,case when cast(pbdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pbdrqj as [INT]) %1000, dateadd(year, cast(pbdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pbdrqj_conv
	,cast(pbfrdj as [INT]) as pbfrdj
	,case when cast(pbfrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pbfrdj as [INT]) %1000, dateadd(year, cast(pbfrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pbfrdj_conv
	,cast(pblrdj as [INT]) as pblrdj
	,case when cast(pblrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pblrdj as [INT]) %1000, dateadd(year, cast(pblrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pblrdj_conv
	,cast(pbdoc as [DECIMAL](38, 4)) as pbdoc
	,cast(pbdct as [NVARCHAR](2)) as pbdct
	,pbvrn as pbvrn
	,ltrim(rtrim(pbvrn)) as pbvrn_conv
	,cast(pbvsst as [NVARCHAR](2)) as pbvsst
	,pbrjyn as pbrjyn
	,ltrim(rtrim(pbrjyn)) as pbrjyn_conv
	,pborfd as pborfd
	,ltrim(rtrim(pborfd)) as pborfd_conv
	,cast(pblrcj as [INT]) as pblrcj
	,case when cast(pblrcj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pblrcj as [INT]) %1000, dateadd(year, cast(pblrcj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pblrcj_conv
	,cast(pblrqt as [DECIMAL](38, 4)) as pblrqt
	,cast(pblrqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pblrqt_conv
	,cast(pbuopn as [DECIMAL](38, 4)) as pbuopn
	,cast(pbuopn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbuopn_conv
	,cast(pburec as [DECIMAL](38, 4)) as pburec
	,cast(pburec as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pburec_conv
	,cast(pbvssp as [NVARCHAR](2)) as pbvssp
	,cast(pbshlt as [DECIMAL](38, 4)) as pbshlt
	,cast(pbshlt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbshlt_conv
	,cast(pbshqt as [DECIMAL](38, 4)) as pbshqt
	,cast(pbshqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbshqt_conv
	,cast(pbupc as [DECIMAL](38, 4)) as pbupc
	,cast(pbupc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbupc_conv
	,pburcd as pburcd
	,ltrim(rtrim(pburcd)) as pburcd_conv
	,cast(pburdt as [INT]) as pburdt
	,case when cast(pburdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pburdt as [INT]) %1000, dateadd(year, cast(pburdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pburdt_conv
	,cast(pburat as [DECIMAL](38, 4)) as pburat
	,cast(pburat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pburat_conv
	,cast(pburab as [DECIMAL](38, 4)) as pburab
	,cast(pburab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pburab_conv
	,pburrf as pburrf
	,ltrim(rtrim(pburrf)) as pburrf_conv
	,pbuser as pbuser
	,ltrim(rtrim(pbuser)) as pbuser_conv
	,pbpid as pbpid
	,ltrim(rtrim(pbpid)) as pbpid_conv
	,pbjobn as pbjobn
	,ltrim(rtrim(pbjobn)) as pbjobn_conv
	,cast(pbupmj as [INT]) as pbupmj
	,case when cast(pbupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pbupmj as [INT]) %1000, dateadd(year, cast(pbupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pbupmj_conv
	,cast(pbtday as [DECIMAL](38, 4)) as pbtday
	,cast(pbvlot as [DECIMAL](38, 4)) as pbvlot
	,cast(pbvlot as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbvlot_conv
	,cast(pbrcpd as [INT]) as pbrcpd
	,case when cast(pbrcpd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pbrcpd as [INT]) %1000, dateadd(year, cast(pbrcpd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pbrcpd_conv
	,cast(pbmxqt as [DECIMAL](38, 4)) as pbmxqt
	,cast(pbmxqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pbmxqt_conv
	,cast(pbmwdh as [NVARCHAR](1)) as pbmwdh
	,cast(pbltlv as [DECIMAL](38, 4)) as pbltlv
	,cast(pbltlv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pbltlv_conv
	,cast(pbdcap as [DECIMAL](38, 4)) as pbdcap
	,pbda08 as pbda08
	,ltrim(rtrim(pbda08)) as pbda08_conv
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
	,pbkcoo
	,pbdoco
	,pbdcto
	,pbsfxo
	,pblnid
	,pbdlan
	,pbcdf1
	,pbcdf2
	,pbcdf3
	,pbcdf4
	,pbvsd
	,pbvsw
	,pbvsm
	,pbcrec
	,pbcfro
	,pbcfab
	,pbcraw
	,pbdrqj
	,pbfrdj
	,pblrdj
	,pbdoc
	,pbdct
	,pbvrn
	,pbvsst
	,pbrjyn
	,pborfd
	,pblrcj
	,pblrqt
	,pbuopn
	,pburec
	,pbvssp
	,pbshlt
	,pbshqt
	,pbupc
	,pburcd
	,pburdt
	,pburat
	,pburab
	,pburrf
	,pbuser
	,pbpid
	,pbjobn
	,pbupmj
	,pbtday
	,pbvlot
	,pbrcpd
	,pbmxqt
	,pbmwdh
	,pbltlv
	,pbdcap
	,pbda08,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4321_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4321_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4321_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4321_cdc(sys_integration_id); 
