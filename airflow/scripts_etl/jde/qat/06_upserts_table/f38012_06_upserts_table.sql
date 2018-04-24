-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f38012_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f38012_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f38012_cdc
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
	,dpdmct as dpdmct
	,ltrim(rtrim(dpdmct)) as dpdmct_conv
	,cast(dpdmcs as [DECIMAL](38, 4)) as dpdmcs
	,cast(dpitm as [DECIMAL](38, 4)) as dpitm
	,cast(dpdto as [NVARCHAR](1)) as dpdto
	,dpdes as dpdes
	,ltrim(rtrim(dpdes)) as dpdes_conv
	,cast(dpdesy as [NVARCHAR](2)) as dpdesy
	,cast(dpseq as [DECIMAL](38, 4)) as dpseq
	,dppsr as dppsr
	,ltrim(rtrim(dppsr)) as dppsr_conv
	,cast(dppsry as [NVARCHAR](2)) as dppsry
	,cast(dpcnqy as [NVARCHAR](1)) as dpcnqy
	,cast(dpcnqt as [DECIMAL](38, 4)) as dpcnqt
	,cast(dpcnqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dpcnqt_conv
	,cast(dpaa as [DECIMAL](38, 4)) as dpaa
	,cast(dpaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dpaa_conv
	,dpcrcd as dpcrcd
	,ltrim(rtrim(dpcrcd)) as dpcrcd_conv
	,cast(dpuom as [NVARCHAR](2)) as dpuom
	,cast(dpminq as [DECIMAL](38, 4)) as dpminq
	,cast(dpminq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dpminq_conv
	,cast(dpmaxq as [DECIMAL](38, 4)) as dpmaxq
	,cast(dpmaxq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as dpmaxq_conv
	,cast(dpqbal as [DECIMAL](38, 4)) as dpqbal
	,cast(dpqbal as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dpqbal_conv
	,cast(dpqcom as [DECIMAL](38, 4)) as dpqcom
	,cast(dpqcom as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as dpqcom_conv
	,cast(dpeftj as [INT]) as dpeftj
	,case when cast(dpeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dpeftj as [INT]) %1000, dateadd(year, cast(dpeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dpeftj_conv
	,cast(dpexdj as [INT]) as dpexdj
	,case when cast(dpexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dpexdj as [INT]) %1000, dateadd(year, cast(dpexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dpexdj_conv
	,dptv01 as dptv01
	,ltrim(rtrim(dptv01)) as dptv01_conv
	,dptv02 as dptv02
	,ltrim(rtrim(dptv02)) as dptv02_conv
	,dptv03 as dptv03
	,ltrim(rtrim(dptv03)) as dptv03_conv
	,cast(dpqty1 as [DECIMAL](38, 4)) as dpqty1
	,cast(dpqty1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dpqty1_conv
	,cast(dptvum as [NVARCHAR](2)) as dptvum
	,dpurcd as dpurcd
	,ltrim(rtrim(dpurcd)) as dpurcd_conv
	,cast(dpurdt as [INT]) as dpurdt
	,case when cast(dpurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dpurdt as [INT]) %1000, dateadd(year, cast(dpurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dpurdt_conv
	,cast(dpurab as [DECIMAL](38, 4)) as dpurab
	,cast(dpurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as dpurab_conv
	,dpurrf as dpurrf
	,ltrim(rtrim(dpurrf)) as dpurrf_conv
	,dpuser as dpuser
	,ltrim(rtrim(dpuser)) as dpuser_conv
	,dppid as dppid
	,ltrim(rtrim(dppid)) as dppid_conv
	,dpjobn as dpjobn
	,ltrim(rtrim(dpjobn)) as dpjobn_conv
	,cast(dpupmj as [INT]) as dpupmj
	,case when cast(dpupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(dpupmj as [INT]) %1000, dateadd(year, cast(dpupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as dpupmj_conv
	,cast(dptday as [DECIMAL](38, 4)) as dptday
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
	,dpdmct
	,dpdmcs
	,dpitm
	,dpdto
	,dpdes
	,dpdesy
	,dpseq
	,dppsr
	,dppsry
	,dpcnqy
	,dpcnqt
	,dpaa
	,dpcrcd
	,dpuom
	,dpminq
	,dpmaxq
	,dpqbal
	,dpqcom
	,dpeftj
	,dpexdj
	,dptv01
	,dptv02
	,dptv03
	,dpqty1
	,dptvum
	,dpurcd
	,dpurdt
	,dpurab
	,dpurrf
	,dpuser
	,dppid
	,dpjobn
	,dpupmj
	,dptday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f38012_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f38012_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f38012_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f38012_cdc(sys_integration_id); 
