-- Create rep new table for init
IF OBJECT_ID('rep_jde.f556202_new') IS NOT NULL
    DROP TABLE rep_jde.f556202_new


CREATE TABLE rep_jde.f556202_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
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
FROM 
    stg_jde.tmp_f556202_init
OPTION ( LABEL = 'CREATE_rep_jde.f556202_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f556202_sys_integration_id ON rep_jde.f556202_new(sys_integration_id); 
