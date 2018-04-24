-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f550312_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f550312_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f550312_cdc
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
	,cast(sglrsegt as [NVARCHAR](6)) as sglrsegt
	,cast(sgctr as [NVARCHAR](3)) as sgctr
	,cast(sgac04 as [NVARCHAR](3)) as sgac04
	,sgdsc1 as sgdsc1
	,ltrim(rtrim(sgdsc1)) as sgdsc1_conv
	,cast(sgac05 as [NVARCHAR](3)) as sgac05
	,sgdsc2 as sgdsc2
	,ltrim(rtrim(sgdsc2)) as sgdsc2_conv
	,cast(sgac01 as [NVARCHAR](3)) as sgac01
	,cast(sgclass03 as [NVARCHAR](3)) as sgclass03
	,cast(sgsrp1 as [NVARCHAR](3)) as sgsrp1
	,sgy55char1 as sgy55char1
	,ltrim(rtrim(sgy55char1)) as sgy55char1_conv
	,sgy55char2 as sgy55char2
	,ltrim(rtrim(sgy55char2)) as sgy55char2_conv
	,sgy55strg1 as sgy55strg1
	,ltrim(rtrim(sgy55strg1)) as sgy55strg1_conv
	,sgy55strg2 as sgy55strg2
	,ltrim(rtrim(sgy55strg2)) as sgy55strg2_conv
	,cast(sgy55amnt1 as [DECIMAL](38, 4)) as sgy55amnt1
	,cast(sgy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sgy55amnt1_conv
	,cast(sgy55amnt2 as [DECIMAL](38, 4)) as sgy55amnt2
	,cast(sgy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sgy55amnt2_conv
	,cast(sgy55time1 as [DECIMAL](38, 4)) as sgy55time1
	,cast(sgy55time2 as [DECIMAL](38, 4)) as sgy55time2
	,cast(sgy55date1 as [INT]) as sgy55date1
	,case when cast(sgy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sgy55date1 as [INT]) %1000, dateadd(year, cast(sgy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sgy55date1_conv
	,cast(sgy55date2 as [INT]) as sgy55date2
	,case when cast(sgy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sgy55date2 as [INT]) %1000, dateadd(year, cast(sgy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sgy55date2_conv
	,sguser as sguser
	,ltrim(rtrim(sguser)) as sguser_conv
	,sgpid as sgpid
	,ltrim(rtrim(sgpid)) as sgpid_conv
	,sgjobn as sgjobn
	,ltrim(rtrim(sgjobn)) as sgjobn_conv
	,cast(sgupmj as [INT]) as sgupmj
	,case when cast(sgupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sgupmj as [INT]) %1000, dateadd(year, cast(sgupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sgupmj_conv
	,cast(sgupmt as [DECIMAL](38, 4)) as sgupmt
	,sgdesc as sgdesc
	,ltrim(rtrim(sgdesc)) as sgdesc_conv
	,cast(sgan8 as [DECIMAL](38, 4)) as sgan8
	,sgalph as sgalph
	,ltrim(rtrim(sgalph)) as sgalph_conv
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
	,sglrsegt
	,sgctr
	,sgac04
	,sgdsc1
	,sgac05
	,sgdsc2
	,sgac01
	,sgclass03
	,sgsrp1
	,sgy55char1
	,sgy55char2
	,sgy55strg1
	,sgy55strg2
	,sgy55amnt1
	,sgy55amnt2
	,sgy55time1
	,sgy55time2
	,sgy55date1
	,sgy55date2
	,sguser
	,sgpid
	,sgjobn
	,sgupmj
	,sgupmt
	,sgdesc
	,sgan8
	,sgalph,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f550312_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f550312_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f550312_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f550312_cdc(sys_integration_id); 
