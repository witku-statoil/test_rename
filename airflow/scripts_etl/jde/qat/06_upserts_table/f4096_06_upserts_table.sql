-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4096_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4096_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4096_cdc
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
	,cast(faanum as [DECIMAL](38, 4)) as faanum
	,cast(faanum as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as faanum_conv
	,cast(faast as [NVARCHAR](8)) as faast
	,faco as faco
	,ltrim(rtrim(faco)) as faco_conv
	,faitem as faitem
	,ltrim(rtrim(faitem)) as faitem_conv
	,faobjf as faobjf
	,ltrim(rtrim(faobjf)) as faobjf_conv
	,faobjt as faobjt
	,ltrim(rtrim(faobjt)) as faobjt_conv
	,cast(fadcto as [NVARCHAR](2)) as fadcto
	,fa_1rt as fa_1rt
	,ltrim(rtrim(fa_1rt)) as fa_1rt_conv
	,cast(fael as [DECIMAL](38, 4)) as fael
	,fadl01 as fadl01
	,ltrim(rtrim(fadl01)) as fadl01_conv
	,cast(fasblt as [NVARCHAR](1)) as fasblt
	,cast(fasegs as [DECIMAL](38, 4)) as fasegs
	,cast(fasfit as [NVARCHAR](10)) as fasfit
	,cast(fasfdt as [NVARCHAR](1)) as fasfdt
	,faabt1 as faabt1
	,ltrim(rtrim(faabt1)) as faabt1_conv
	,fafile as fafile
	,ltrim(rtrim(fafile)) as fafile_conv
	,fapid as fapid
	,ltrim(rtrim(fapid)) as fapid_conv
	,fajobn as fajobn
	,ltrim(rtrim(fajobn)) as fajobn_conv
	,fauser as fauser
	,ltrim(rtrim(fauser)) as fauser_conv
	,cast(faupmj as [INT]) as faupmj
	,case when cast(faupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(faupmj as [INT]) %1000, dateadd(year, cast(faupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as faupmj_conv
	,cast(fatday as [DECIMAL](38, 4)) as fatday
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
	,faanum
	,faast
	,faco
	,faitem
	,faobjf
	,faobjt
	,fadcto
	,fa_1rt
	,fael
	,fadl01
	,fasblt
	,fasegs
	,fasfit
	,fasfdt
	,faabt1
	,fafile
	,fapid
	,fajobn
	,fauser
	,faupmj
	,fatday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4096_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4096_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4096_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4096_cdc(sys_integration_id); 
