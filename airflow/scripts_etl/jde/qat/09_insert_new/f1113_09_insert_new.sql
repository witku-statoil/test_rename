-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f1113_new
(
    	sys_file_name
	,sys_file_ln
	,sys_integration_id
	,sys_extract_dt
	,sys_cdc_dt
	,sys_cdc_scn
	,sys_cdc_operation_type
	,sys_cdc_before_after
	,sys_line_modified_ind
	,c1rtty
	,c1crdc
	,c1crdc_conv
	,c1crcd
	,c1crcd_conv
	,c1eft
	,c1eft_conv
	,c1crr
	,c1crrd
)
SELECT
    	cdc.sys_file_name
	,cdc.sys_file_ln
	,cdc.sys_integration_id
	,cdc.sys_extract_dt
	,cdc.sys_cdc_dt
	,cdc.sys_cdc_scn
	,cdc.sys_cdc_operation_type
	,cdc.sys_cdc_before_after
	,cdc.sys_line_modified_ind
	,cdc.c1rtty
	,cdc.c1crdc
	,cdc.c1crdc_conv
	,cdc.c1crcd
	,cdc.c1crcd_conv
	,cdc.c1eft
	,cdc.c1eft_conv
	,cdc.c1crr
	,cdc.c1crrd
FROM stg_jde_qat.tmp_upsert_f1113_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f1113_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f1113_new AF:{{ task_instance_key_str }}' ) 