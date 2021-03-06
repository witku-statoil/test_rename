-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f41061_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f41061_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f41061_cdc
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
	,cbmcu as cbmcu
	,ltrim(rtrim(cbmcu)) as cbmcu_conv
	,cast(cban8 as [DECIMAL](38, 4)) as cban8
	,cast(cbitm as [DECIMAL](38, 4)) as cbitm
	,cblitm as cblitm
	,ltrim(rtrim(cblitm)) as cblitm_conv
	,cbaitm as cbaitm
	,ltrim(rtrim(cbaitm)) as cbaitm_conv
	,cast(cbcatn as [NVARCHAR](8)) as cbcatn
	,cbdmct as cbdmct
	,ltrim(rtrim(cbdmct)) as cbdmct_conv
	,cast(cbdmcs as [DECIMAL](38, 4)) as cbdmcs
	,cbkcoo as cbkcoo
	,ltrim(rtrim(cbkcoo)) as cbkcoo_conv
	,cast(cbdoco as [DECIMAL](38, 4)) as cbdoco
	,cast(cbdcto as [NVARCHAR](2)) as cbdcto
	,cast(cblnid as [DECIMAL](38, 4)) as cblnid
	,cast(cblnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as cblnid_conv
	,cbcrcd as cbcrcd
	,ltrim(rtrim(cbcrcd)) as cbcrcd_conv
	,cast(cbuom as [NVARCHAR](2)) as cbuom
	,cast(cbprrc as [DECIMAL](38, 4)) as cbprrc
	,cast(cbprrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as cbprrc_conv
	,cast(cbuorg as [DECIMAL](38, 4)) as cbuorg
	,cast(cbuorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as cbuorg_conv
	,cast(cbeftj as [INT]) as cbeftj
	,case when cast(cbeftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cbeftj as [INT]) %1000, dateadd(year, cast(cbeftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cbeftj_conv
	,cast(cbexdj as [INT]) as cbexdj
	,case when cast(cbexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cbexdj as [INT]) %1000, dateadd(year, cast(cbexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cbexdj_conv
	,cbuser as cbuser
	,ltrim(rtrim(cbuser)) as cbuser_conv
	,cbpid as cbpid
	,ltrim(rtrim(cbpid)) as cbpid_conv
	,cbjobn as cbjobn
	,ltrim(rtrim(cbjobn)) as cbjobn_conv
	,cast(cbupmj as [INT]) as cbupmj
	,case when cast(cbupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cbupmj as [INT]) %1000, dateadd(year, cast(cbupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cbupmj_conv
	,cast(cbtday as [DECIMAL](38, 4)) as cbtday
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
	,cbmcu
	,cban8
	,cbitm
	,cblitm
	,cbaitm
	,cbcatn
	,cbdmct
	,cbdmcs
	,cbkcoo
	,cbdoco
	,cbdcto
	,cblnid
	,cbcrcd
	,cbuom
	,cbprrc
	,cbuorg
	,cbeftj
	,cbexdj
	,cbuser
	,cbpid
	,cbjobn
	,cbupmj
	,cbtday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f41061_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f41061_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f41061_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f41061_cdc(sys_integration_id); 
