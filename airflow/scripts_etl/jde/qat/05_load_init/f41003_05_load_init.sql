-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f41003_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f41003_new


CREATE TABLE rep_jde_qat.f41003_new
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
	,cast(ucum as [NVARCHAR](2)) as ucum
	,cast(ucrum as [NVARCHAR](2)) as ucrum
	,cast(ucconv as [DECIMAL](38, 4)) as ucconv
	,cast(ucconv as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as ucconv_conv
	,ucuser as ucuser
	,ltrim(rtrim(ucuser)) as ucuser_conv
	,ucpid as ucpid
	,ltrim(rtrim(ucpid)) as ucpid_conv
	,ucjobn as ucjobn
	,ltrim(rtrim(ucjobn)) as ucjobn_conv
	,cast(ucupmj as [INT]) as ucupmj
	,case when cast(ucupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ucupmj as [INT]) %1000, dateadd(year, cast(ucupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ucupmj_conv
	,cast(uctday as [DECIMAL](38, 4)) as uctday
	,ucexpo as ucexpo
	,ltrim(rtrim(ucexpo)) as ucexpo_conv
	,ucexso as ucexso
	,ltrim(rtrim(ucexso)) as ucexso_conv
FROM 
    stg_jde_qat.tmp_f41003_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f41003_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f41003_sys_integration_id ON rep_jde_qat.f41003_new(sys_integration_id); 
