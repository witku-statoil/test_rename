-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f40943_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f40943_new


CREATE TABLE rep_jde_qat.f40943_new
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
	,cast(oksdgr as [NVARCHAR](8)) as oksdgr
	,oksdv1 as oksdv1
	,ltrim(rtrim(oksdv1)) as oksdv1_conv
	,oksdv2 as oksdv2
	,ltrim(rtrim(oksdv2)) as oksdv2_conv
	,oksdv3 as oksdv3
	,ltrim(rtrim(oksdv3)) as oksdv3_conv
	,oksdv4 as oksdv4
	,ltrim(rtrim(oksdv4)) as oksdv4_conv
	,oksdv5 as oksdv5
	,ltrim(rtrim(oksdv5)) as oksdv5_conv
	,oksdv6 as oksdv6
	,ltrim(rtrim(oksdv6)) as oksdv6_conv
	,oksdv7 as oksdv7
	,ltrim(rtrim(oksdv7)) as oksdv7_conv
	,oksdv8 as oksdv8
	,ltrim(rtrim(oksdv8)) as oksdv8_conv
	,cast(okogid as [DECIMAL](38, 4)) as okogid
	,cast(okogid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as okogid_conv
FROM 
    stg_jde_qat.tmp_f40943_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f40943_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f40943_sys_integration_id ON rep_jde_qat.f40943_new(sys_integration_id); 
