-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4076_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4076_new


CREATE TABLE rep_jde_qat.f4076_new
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
	,cast(fmfrmn as [NVARCHAR](10)) as fmfrmn
	,fmfml as fmfml
	,ltrim(rtrim(fmfml)) as fmfml_conv
	,cast(fmaprs as [NVARCHAR](1)) as fmaprs
	,fmuser as fmuser
	,ltrim(rtrim(fmuser)) as fmuser_conv
	,fmpid as fmpid
	,ltrim(rtrim(fmpid)) as fmpid_conv
	,fmjobn as fmjobn
	,ltrim(rtrim(fmjobn)) as fmjobn_conv
	,cast(fmupmj as [INT]) as fmupmj
	,case when cast(fmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fmupmj as [INT]) %1000, dateadd(year, cast(fmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fmupmj_conv
	,cast(fmtday as [DECIMAL](38, 4)) as fmtday
FROM 
    stg_jde_qat.tmp_f4076_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4076_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4076_sys_integration_id ON rep_jde_qat.f4076_new(sys_integration_id); 
