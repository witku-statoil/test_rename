-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f556202_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f556202_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f556202_cdc
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
	,cast(emctr as [NVARCHAR](3)) as emctr
	,emco as emco
	,ltrim(rtrim(emco)) as emco_conv
	,emmcu as emmcu
	,ltrim(rtrim(emmcu)) as emmcu_conv
	,emlocn as emlocn
	,ltrim(rtrim(emlocn)) as emlocn_conv
	,emev01 as emev01
	,ltrim(rtrim(emev01)) as emev01_conv
	,emev02 as emev02
	,ltrim(rtrim(emev02)) as emev02_conv
	,cast(emmath01 as [DECIMAL](38, 4)) as emmath01
	,cast(emmath01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as emmath01_conv
	,cast(emmath02 as [DECIMAL](38, 4)) as emmath02
	,cast(emmath02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as emmath02_conv
	,emdl01 as emdl01
	,ltrim(rtrim(emdl01)) as emdl01_conv
	,emdl02 as emdl02
	,ltrim(rtrim(emdl02)) as emdl02_conv
	,cast(emtrdj as [INT]) as emtrdj
	,case when cast(emtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(emtrdj as [INT]) %1000, dateadd(year, cast(emtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as emtrdj_conv
	,cast(empddj as [INT]) as empddj
	,case when cast(empddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(empddj as [INT]) %1000, dateadd(year, cast(empddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as empddj_conv
	,emuser as emuser
	,ltrim(rtrim(emuser)) as emuser_conv
	,empid as empid
	,ltrim(rtrim(empid)) as empid_conv
	,emjobn as emjobn
	,ltrim(rtrim(emjobn)) as emjobn_conv
	,cast(emupmj as [INT]) as emupmj
	,case when cast(emupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(emupmj as [INT]) %1000, dateadd(year, cast(emupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as emupmj_conv
	,cast(emtday as [DECIMAL](38, 4)) as emtday
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
	,emctr
	,emco
	,emmcu
	,emlocn
	,emev01
	,emev02
	,emmath01
	,emmath02
	,emdl01
	,emdl02
	,emtrdj
	,empddj
	,emuser
	,empid
	,emjobn
	,emupmj
	,emtday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f556202_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f556202_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f556202_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f556202_cdc(sys_integration_id); 
