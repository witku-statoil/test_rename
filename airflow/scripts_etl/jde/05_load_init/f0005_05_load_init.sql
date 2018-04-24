-- Create rep new table for init
IF OBJECT_ID('rep_jde.f0005_new') IS NOT NULL
    DROP TABLE rep_jde.f0005_new


CREATE TABLE rep_jde.f0005_new
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
	,cast(drsy as [NVARCHAR](4)) as drsy
	,drrt as drrt
	,ltrim(rtrim(drrt)) as drrt_conv
	,drky as drky
	,ltrim(rtrim(drky)) as drky_conv
	,drdl01 as drdl01
	,ltrim(rtrim(drdl01)) as drdl01_conv
	,drdl02 as drdl02
	,ltrim(rtrim(drdl02)) as drdl02_conv
	,drsphd as drsphd
	,ltrim(rtrim(drsphd)) as drsphd_conv
	,cast(drudco as [NVARCHAR](1)) as drudco
	,drhrdc as drhrdc
	,ltrim(rtrim(drhrdc)) as drhrdc_conv
	,druser as druser
	,ltrim(rtrim(druser)) as druser_conv
	,drpid as drpid
	,ltrim(rtrim(drpid)) as drpid_conv
	,cast(drupmj as [INT]) as drupmj
	,case when cast(drupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(drupmj as [INT]) %1000, dateadd(year, cast(drupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as drupmj_conv
	,drjobn as drjobn
	,ltrim(rtrim(drjobn)) as drjobn_conv
	,cast(drupmt as [DECIMAL](38, 4)) as drupmt
FROM 
    stg_jde.tmp_f0005_init
OPTION ( LABEL = 'CREATE_rep_jde.f0005_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0005_sys_integration_id ON rep_jde.f0005_new(sys_integration_id); 
