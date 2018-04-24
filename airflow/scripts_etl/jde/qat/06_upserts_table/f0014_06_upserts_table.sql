-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f0014_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f0014_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f0014_cdc
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
	,pnptc as pnptc
	,ltrim(rtrim(pnptc)) as pnptc_conv
	,pnptd as pnptd
	,ltrim(rtrim(pnptd)) as pnptd_conv
	,cast(pndcp as [DECIMAL](38, 4)) as pndcp
	,cast(pndcd as [DECIMAL](38, 4)) as pndcd
	,cast(pnndtp as [DECIMAL](38, 4)) as pnndtp
	,cast(pnddj as [INT]) as pnddj
	,case when cast(pnddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pnddj as [INT]) %1000, dateadd(year, cast(pnddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pnddj_conv
	,cast(pnnsp as [DECIMAL](38, 4)) as pnnsp
	,cast(pndtpa as [DECIMAL](38, 4)) as pndtpa
	,cast(pneir as [DECIMAL](38, 4)) as pneir
	,pnuser as pnuser
	,ltrim(rtrim(pnuser)) as pnuser_conv
	,pnpid as pnpid
	,ltrim(rtrim(pnpid)) as pnpid_conv
	,cast(pnupmj as [INT]) as pnupmj
	,case when cast(pnupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pnupmj as [INT]) %1000, dateadd(year, cast(pnupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pnupmj_conv
	,pnjobn as pnjobn
	,ltrim(rtrim(pnjobn)) as pnjobn_conv
	,cast(pnupmt as [DECIMAL](38, 4)) as pnupmt
	,cast(pnpxdm as [DECIMAL](38, 4)) as pnpxdm
	,cast(pnpxdm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pnpxdm_conv
	,cast(pnpxdd as [DECIMAL](38, 4)) as pnpxdd
	,cast(pnpxdd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pnpxdd_conv
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
	,pnptc
	,pnptd
	,pndcp
	,pndcd
	,pnndtp
	,pnddj
	,pnnsp
	,pndtpa
	,pneir
	,pnuser
	,pnpid
	,pnupmj
	,pnjobn
	,pnupmt
	,pnpxdm
	,pnpxdd,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f0014_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f0014_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0014_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f0014_cdc(sys_integration_id); 
