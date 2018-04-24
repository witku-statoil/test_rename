-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f40540_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f40540_new


CREATE TABLE rep_jde_qat.f40540_new
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
	,cast(icitm as [DECIMAL](38, 4)) as icitm
	,iccmdcde as iccmdcde
	,ltrim(rtrim(iccmdcde)) as iccmdcde_conv
	,icunspsc as icunspsc
	,ltrim(rtrim(icunspsc)) as icunspsc_conv
	,icuser as icuser
	,ltrim(rtrim(icuser)) as icuser_conv
	,icpid as icpid
	,ltrim(rtrim(icpid)) as icpid_conv
	,icjobn as icjobn
	,ltrim(rtrim(icjobn)) as icjobn_conv
	,cast(icupmt as [DECIMAL](38, 4)) as icupmt
	,cast(icupmj as [INT]) as icupmj
	,case when cast(icupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(icupmj as [INT]) %1000, dateadd(year, cast(icupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as icupmj_conv
FROM 
    stg_jde_qat.tmp_f40540_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f40540_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f40540_sys_integration_id ON rep_jde_qat.f40540_new(sys_integration_id); 
