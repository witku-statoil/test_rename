-- Create rep new table for init
IF OBJECT_ID('rep_jde.f1113_new') IS NOT NULL
    DROP TABLE rep_jde.f1113_new


CREATE TABLE rep_jde.f1113_new
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
	,cast(c1rtty as [NVARCHAR](2)) as c1rtty
	,c1crdc as c1crdc
	,ltrim(rtrim(c1crdc)) as c1crdc_conv
	,c1crcd as c1crcd
	,ltrim(rtrim(c1crcd)) as c1crcd_conv
	,cast(c1eft as [INT]) as c1eft
	,case when cast(c1eft as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(c1eft as [INT]) %1000, dateadd(year, cast(c1eft as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as c1eft_conv
	,cast(c1crr as [DECIMAL](38, 4)) as c1crr
	,cast(c1crrd as [DECIMAL](38, 4)) as c1crrd
FROM 
    stg_jde.tmp_f1113_init
OPTION ( LABEL = 'CREATE_rep_jde.f1113_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f1113_sys_integration_id ON rep_jde.f1113_new(sys_integration_id); 
