-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f1620_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f1620_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f1620_cdc
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
	,ctabt1 as ctabt1
	,ltrim(rtrim(ctabt1)) as ctabt1_conv
	,ctdl01 as ctdl01
	,ltrim(rtrim(ctdl01)) as ctdl01_conv
	,cast(ctcmer as [NVARCHAR](1)) as ctcmer
	,ctfile as ctfile
	,ltrim(rtrim(ctfile)) as ctfile_conv
	,ctdtai as ctdtai
	,ltrim(rtrim(ctdtai)) as ctdtai_conv
	,cast(ctvalr as [NVARCHAR](2)) as ctvalr
	,ctcmvl as ctcmvl
	,ltrim(rtrim(ctcmvl)) as ctcmvl_conv
	,cast(ctsy as [NVARCHAR](4)) as ctsy
	,ctrt as ctrt
	,ltrim(rtrim(ctrt)) as ctrt_conv
	,ctuser as ctuser
	,ltrim(rtrim(ctuser)) as ctuser_conv
	,ctpid as ctpid
	,ltrim(rtrim(ctpid)) as ctpid_conv
	,cast(ctupmj as [INT]) as ctupmj
	,case when cast(ctupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ctupmj as [INT]) %1000, dateadd(year, cast(ctupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ctupmj_conv
	,cast(ctupmt as [DECIMAL](38, 4)) as ctupmt
	,ctjobn as ctjobn
	,ltrim(rtrim(ctjobn)) as ctjobn_conv
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
	,ctabt1
	,ctdl01
	,ctcmer
	,ctfile
	,ctdtai
	,ctvalr
	,ctcmvl
	,ctsy
	,ctrt
	,ctuser
	,ctpid
	,ctupmj
	,ctupmt
	,ctjobn,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f1620_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f1620_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f1620_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f1620_cdc(sys_integration_id); 
