-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f43008_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f43008_new


CREATE TABLE rep_jde_qat.f43008_new
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
	,cast(apdcto as [NVARCHAR](2)) as apdcto
	,apartg as apartg
	,ltrim(rtrim(apartg)) as apartg_conv
	,apdl01 as apdl01
	,ltrim(rtrim(apdl01)) as apdl01_conv
	,cast(apalim as [DECIMAL](38, 4)) as apalim
	,cast(apalim as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as apalim_conv
	,cast(aprper as [DECIMAL](38, 4)) as aprper
	,cast(aprper as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as aprper_conv
	,apaty as apaty
	,ltrim(rtrim(apaty)) as apaty_conv
FROM 
    stg_jde_qat.tmp_f43008_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f43008_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f43008_sys_integration_id ON rep_jde_qat.f43008_new(sys_integration_id); 
