-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f5509176_new
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
	,qsmcu
	,qsmcu_conv
	,qsy55fypn2
	,qsy55fypn2_conv
	,qsrp04
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
	,cdc.qsmcu
	,cdc.qsmcu_conv
	,cdc.qsy55fypn2
	,cdc.qsy55fypn2_conv
	,cdc.qsrp04
FROM stg_jde_qat.tmp_upsert_f5509176_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f5509176_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f5509176_new AF:{{ task_instance_key_str }}' ) 