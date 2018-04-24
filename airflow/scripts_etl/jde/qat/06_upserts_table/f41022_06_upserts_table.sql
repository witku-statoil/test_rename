-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f41022_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f41022_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f41022_cdc
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
	,pnmcu as pnmcu
	,ltrim(rtrim(pnmcu)) as pnmcu_conv
	,cast(pnitm as [DECIMAL](38, 4)) as pnitm
	,cast(pnstvl as [DECIMAL](38, 4)) as pnstvl
	,cast(pnstvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pnstvl_conv
	,cast(pnbum0 as [NVARCHAR](2)) as pnbum0
	,pnlrfg as pnlrfg
	,ltrim(rtrim(pnlrfg)) as pnlrfg_conv
	,pnhcor as pnhcor
	,ltrim(rtrim(pnhcor)) as pnhcor_conv
	,pnacor as pnacor
	,ltrim(rtrim(pnacor)) as pnacor_conv
	,pnabbl as pnabbl
	,ltrim(rtrim(pnabbl)) as pnabbl_conv
	,pnatwh as pnatwh
	,ltrim(rtrim(pnatwh)) as pnatwh_conv
	,cast(pnrplt as [NVARCHAR](3)) as pnrplt
	,cast(pnbcat as [NVARCHAR](3)) as pnbcat
	,cast(pnfcat as [NVARCHAR](3)) as pnfcat
	,cast(pnrecd as [INT]) as pnrecd
	,case when cast(pnrecd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pnrecd as [INT]) %1000, dateadd(year, cast(pnrecd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pnrecd_conv
	,pnurcd as pnurcd
	,ltrim(rtrim(pnurcd)) as pnurcd_conv
	,cast(pnurat as [DECIMAL](38, 4)) as pnurat
	,cast(pnurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pnurat_conv
	,cast(pnurab as [DECIMAL](38, 4)) as pnurab
	,cast(pnurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pnurab_conv
	,pnurrf as pnurrf
	,ltrim(rtrim(pnurrf)) as pnurrf_conv
	,cast(pnurdt as [INT]) as pnurdt
	,case when cast(pnurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pnurdt as [INT]) %1000, dateadd(year, cast(pnurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pnurdt_conv
	,pnuser as pnuser
	,ltrim(rtrim(pnuser)) as pnuser_conv
	,pnpid as pnpid
	,ltrim(rtrim(pnpid)) as pnpid_conv
	,pnjobn as pnjobn
	,ltrim(rtrim(pnjobn)) as pnjobn_conv
	,cast(pnupmj as [INT]) as pnupmj
	,case when cast(pnupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pnupmj as [INT]) %1000, dateadd(year, cast(pnupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pnupmj_conv
	,cast(pntday as [DECIMAL](38, 4)) as pntday
	,cast(pnrtob as [NVARCHAR](1)) as pnrtob
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
	,pnmcu
	,pnitm
	,pnstvl
	,pnbum0
	,pnlrfg
	,pnhcor
	,pnacor
	,pnabbl
	,pnatwh
	,pnrplt
	,pnbcat
	,pnfcat
	,pnrecd
	,pnurcd
	,pnurat
	,pnurab
	,pnurrf
	,pnurdt
	,pnuser
	,pnpid
	,pnjobn
	,pnupmj
	,pntday
	,pnrtob,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f41022_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f41022_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f41022_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f41022_cdc(sys_integration_id); 
