-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f41291_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f41291_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f41291_cdc
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
	,cast(igprp5 as [NVARCHAR](3)) as igprp5
	,cast(igitm as [DECIMAL](38, 4)) as igitm
	,iglitm as iglitm
	,ltrim(rtrim(iglitm)) as iglitm_conv
	,igaitm as igaitm
	,ltrim(rtrim(igaitm)) as igaitm_conv
	,igmcu as igmcu
	,ltrim(rtrim(igmcu)) as igmcu_conv
	,iglocn as iglocn
	,ltrim(rtrim(iglocn)) as iglocn_conv
	,iglotn as iglotn
	,ltrim(rtrim(iglotn)) as iglotn_conv
	,cast(iglvla as [NVARCHAR](3)) as iglvla
	,cast(iglvlb as [NVARCHAR](3)) as iglvlb
	,cast(igpcst as [DECIMAL](38, 4)) as igpcst
	,cast(igpcst as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as igpcst_conv
	,cast(igpamt as [DECIMAL](38, 4)) as igpamt
	,cast(igpamt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as igpamt_conv
	,cast(igratf as [DECIMAL](38, 4)) as igratf
	,cast(igratf as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as igratf_conv
	,cast(igratv as [DECIMAL](38, 4)) as igratv
	,cast(igratv as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as igratv_conv
	,cast(igan8 as [DECIMAL](38, 4)) as igan8
	,igglc as igglc
	,ltrim(rtrim(igglc)) as igglc_conv
	,cast(igglpt as [NVARCHAR](4)) as igglpt
	,cast(igefff as [INT]) as igefff
	,case when cast(igefff as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(igefff as [INT]) %1000, dateadd(year, cast(igefff as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as igefff_conv
	,cast(igefft as [INT]) as igefft
	,case when cast(igefft as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(igefft as [INT]) %1000, dateadd(year, cast(igefft as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as igefft_conv
	,cast(igtx as [NVARCHAR](1)) as igtx
	,iginyn as iginyn
	,ltrim(rtrim(iginyn)) as iginyn_conv
	,igrcyn as igrcyn
	,ltrim(rtrim(igrcyn)) as igrcyn_conv
	,igaisl as igaisl
	,ltrim(rtrim(igaisl)) as igaisl_conv
	,igbin as igbin
	,ltrim(rtrim(igbin)) as igbin_conv
	,iguser as iguser
	,ltrim(rtrim(iguser)) as iguser_conv
	,igjobn as igjobn
	,ltrim(rtrim(igjobn)) as igjobn_conv
	,igpid as igpid
	,ltrim(rtrim(igpid)) as igpid_conv
	,cast(igupmj as [INT]) as igupmj
	,case when cast(igupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(igupmj as [INT]) %1000, dateadd(year, cast(igupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as igupmj_conv
	,cast(igtday as [DECIMAL](38, 4)) as igtday
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
	,igprp5
	,igitm
	,iglitm
	,igaitm
	,igmcu
	,iglocn
	,iglotn
	,iglvla
	,iglvlb
	,igpcst
	,igpamt
	,igratf
	,igratv
	,igan8
	,igglc
	,igglpt
	,igefff
	,igefft
	,igtx
	,iginyn
	,igrcyn
	,igaisl
	,igbin
	,iguser
	,igjobn
	,igpid
	,igupmj
	,igtday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f41291_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f41291_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f41291_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f41291_cdc(sys_integration_id); 
