-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4092_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4092_new


CREATE TABLE rep_jde_qat.f4092_new
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
	,gpgpty as gpgpty
	,ltrim(rtrim(gpgpty)) as gpgpty_conv
	,gpgpc as gpgpc
	,ltrim(rtrim(gpgpc)) as gpgpc_conv
	,gpdl01 as gpdl01
	,ltrim(rtrim(gpdl01)) as gpdl01_conv
	,gpgpk1 as gpgpk1
	,ltrim(rtrim(gpgpk1)) as gpgpk1_conv
	,gpgpk2 as gpgpk2
	,ltrim(rtrim(gpgpk2)) as gpgpk2_conv
	,gpgpk3 as gpgpk3
	,ltrim(rtrim(gpgpk3)) as gpgpk3_conv
	,gpgpk4 as gpgpk4
	,ltrim(rtrim(gpgpk4)) as gpgpk4_conv
	,gpgpk5 as gpgpk5
	,ltrim(rtrim(gpgpk5)) as gpgpk5_conv
	,gpgpk6 as gpgpk6
	,ltrim(rtrim(gpgpk6)) as gpgpk6_conv
	,gpgpk7 as gpgpk7
	,ltrim(rtrim(gpgpk7)) as gpgpk7_conv
	,gpgpk8 as gpgpk8
	,ltrim(rtrim(gpgpk8)) as gpgpk8_conv
	,gpgpk9 as gpgpk9
	,ltrim(rtrim(gpgpk9)) as gpgpk9_conv
	,gpgpk10 as gpgpk10
	,ltrim(rtrim(gpgpk10)) as gpgpk10_conv
FROM 
    stg_jde_qat.tmp_f4092_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4092_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4092_sys_integration_id ON rep_jde_qat.f4092_new(sys_integration_id); 
