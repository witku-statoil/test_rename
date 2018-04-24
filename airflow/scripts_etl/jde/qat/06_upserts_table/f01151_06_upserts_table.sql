-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f01151_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f01151_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f01151_cdc
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
	,cast(eaan8 as [DECIMAL](38, 4)) as eaan8
	,cast(eaidln as [DECIMAL](38, 4)) as eaidln
	,cast(eaidln as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as eaidln_conv
	,cast(earck7 as [DECIMAL](38, 4)) as earck7
	,cast(eaetp as [NVARCHAR](4)) as eaetp
	,eaemal as eaemal
	,ltrim(rtrim(eaemal)) as eaemal_conv
	,eauser as eauser
	,ltrim(rtrim(eauser)) as eauser_conv
	,eapid as eapid
	,ltrim(rtrim(eapid)) as eapid_conv
	,cast(eaupmj as [INT]) as eaupmj
	,case when cast(eaupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(eaupmj as [INT]) %1000, dateadd(year, cast(eaupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as eaupmj_conv
	,eajobn as eajobn
	,ltrim(rtrim(eajobn)) as eajobn_conv
	,cast(eaupmt as [DECIMAL](38, 4)) as eaupmt
	,cast(eaehier as [DECIMAL](38, 4)) as eaehier
	,eaefor as eaefor
	,ltrim(rtrim(eaefor)) as eaefor_conv
	,cast(eaeclass as [NVARCHAR](3)) as eaeclass
	,cast(eacfno1 as [DECIMAL](38, 4)) as eacfno1
	,cast(eacfno1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as eacfno1_conv
	,eagen1 as eagen1
	,ltrim(rtrim(eagen1)) as eagen1_conv
	,eafalge as eafalge
	,ltrim(rtrim(eafalge)) as eafalge_conv
	,cast(easyncs as [DECIMAL](38, 4)) as easyncs
	,cast(eacaad as [DECIMAL](38, 4)) as eacaad
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
	,eaan8
	,eaidln
	,earck7
	,eaetp
	,eaemal
	,eauser
	,eapid
	,eaupmj
	,eajobn
	,eaupmt
	,eaehier
	,eaefor
	,eaeclass
	,eacfno1
	,eagen1
	,eafalge
	,easyncs
	,eacaad,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f01151_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f01151_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f01151_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f01151_cdc(sys_integration_id); 
