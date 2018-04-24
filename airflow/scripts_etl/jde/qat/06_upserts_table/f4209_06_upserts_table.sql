-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4209_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4209_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4209_cdc
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
	,cast(hohcod as [NVARCHAR](2)) as hohcod
	,cast(horper as [DECIMAL](38, 4)) as horper
	,cast(horper as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as horper_conv
	,cast(hoan8 as [DECIMAL](38, 4)) as hoan8
	,homcu as homcu
	,ltrim(rtrim(homcu)) as homcu_conv
	,hokcoo as hokcoo
	,ltrim(rtrim(hokcoo)) as hokcoo_conv
	,cast(hodoco as [DECIMAL](38, 4)) as hodoco
	,cast(hodcto as [NVARCHAR](2)) as hodcto
	,hosfxo as hosfxo
	,ltrim(rtrim(hosfxo)) as hosfxo_conv
	,cast(holnid as [DECIMAL](38, 4)) as holnid
	,cast(holnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as holnid_conv
	,cast(hoitm as [DECIMAL](38, 4)) as hoitm
	,holitm as holitm
	,ltrim(rtrim(holitm)) as holitm_conv
	,hoaitm as hoaitm
	,ltrim(rtrim(hoaitm)) as hoaitm_conv
	,cast(hotrdj as [INT]) as hotrdj
	,case when cast(hotrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(hotrdj as [INT]) %1000, dateadd(year, cast(hotrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as hotrdj_conv
	,cast(hodrqj as [INT]) as hodrqj
	,case when cast(hodrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(hodrqj as [INT]) %1000, dateadd(year, cast(hodrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as hodrqj_conv
	,cast(hopddj as [INT]) as hopddj
	,case when cast(hopddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(hopddj as [INT]) %1000, dateadd(year, cast(hopddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as hopddj_conv
	,cast(hoctyp as [NVARCHAR](2)) as hoctyp
	,cast(hordc as [NVARCHAR](2)) as hordc
	,hordb as hordb
	,ltrim(rtrim(hordb)) as hordb_conv
	,cast(hordj as [INT]) as hordj
	,case when cast(hordj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(hordj as [INT]) %1000, dateadd(year, cast(hordj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as hordj_conv
	,cast(hordt as [DECIMAL](38, 4)) as hordt
	,hoartg as hoartg
	,ltrim(rtrim(hoartg)) as hoartg_conv
	,hoasts as hoasts
	,ltrim(rtrim(hoasts)) as hoasts_conv
	,hoaty as hoaty
	,ltrim(rtrim(hoaty)) as hoaty_conv
	,hoedei as hoedei
	,ltrim(rtrim(hoedei)) as hoedei_conv
	,cast(hodlnid as [DECIMAL](38, 4)) as hodlnid
	,cast(hodlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as hodlnid_conv
	,cast(hopa8 as [DECIMAL](38, 4)) as hopa8
	,cast(hoshan as [DECIMAL](38, 4)) as hoshan
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
	,hohcod
	,horper
	,hoan8
	,homcu
	,hokcoo
	,hodoco
	,hodcto
	,hosfxo
	,holnid
	,hoitm
	,holitm
	,hoaitm
	,hotrdj
	,hodrqj
	,hopddj
	,hoctyp
	,hordc
	,hordb
	,hordj
	,hordt
	,hoartg
	,hoasts
	,hoaty
	,hoedei
	,hodlnid
	,hopa8
	,hoshan,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4209_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4209_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4209_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4209_cdc(sys_integration_id); 
