-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f41002_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f41002_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f41002_cdc
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
	,ummcu as ummcu
	,ltrim(rtrim(ummcu)) as ummcu_conv
	,cast(umitm as [DECIMAL](38, 4)) as umitm
	,cast(umum as [NVARCHAR](2)) as umum
	,cast(umrum as [NVARCHAR](2)) as umrum
	,cast(umustr as [NVARCHAR](1)) as umustr
	,cast(umconv as [DECIMAL](38, 4)) as umconv
	,cast(umconv as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as umconv_conv
	,cast(umcnv1 as [DECIMAL](38, 4)) as umcnv1
	,cast(umcnv1 as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as umcnv1_conv
	,umuser as umuser
	,ltrim(rtrim(umuser)) as umuser_conv
	,umpid as umpid
	,ltrim(rtrim(umpid)) as umpid_conv
	,umjobn as umjobn
	,ltrim(rtrim(umjobn)) as umjobn_conv
	,cast(umupmj as [INT]) as umupmj
	,case when cast(umupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(umupmj as [INT]) %1000, dateadd(year, cast(umupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as umupmj_conv
	,cast(umtday as [DECIMAL](38, 4)) as umtday
	,umexpo as umexpo
	,ltrim(rtrim(umexpo)) as umexpo_conv
	,umexso as umexso
	,ltrim(rtrim(umexso)) as umexso_conv
	,cast(umpupc as [INT]) as umpupc
	,cast(umpupc as [INT]) / cast(1 as [INT]) as umpupc_conv
	,cast(umsepc as [INT]) as umsepc
	,cast(umsepc as [INT]) / cast(1 as [INT]) as umsepc_conv
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
	,ummcu
	,umitm
	,umum
	,umrum
	,umustr
	,umconv
	,umcnv1
	,umuser
	,umpid
	,umjobn
	,umupmj
	,umtday
	,umexpo
	,umexso
	,umpupc
	,umsepc,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f41002_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f41002_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f41002_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f41002_cdc(sys_integration_id); 
