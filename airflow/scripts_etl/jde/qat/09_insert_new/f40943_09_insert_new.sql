-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f40943_new
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
	,oksdgr
	,oksdv1
	,oksdv1_conv
	,oksdv2
	,oksdv2_conv
	,oksdv3
	,oksdv3_conv
	,oksdv4
	,oksdv4_conv
	,oksdv5
	,oksdv5_conv
	,oksdv6
	,oksdv6_conv
	,oksdv7
	,oksdv7_conv
	,oksdv8
	,oksdv8_conv
	,okogid
	,okogid_conv
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
	,cdc.oksdgr
	,cdc.oksdv1
	,cdc.oksdv1_conv
	,cdc.oksdv2
	,cdc.oksdv2_conv
	,cdc.oksdv3
	,cdc.oksdv3_conv
	,cdc.oksdv4
	,cdc.oksdv4_conv
	,cdc.oksdv5
	,cdc.oksdv5_conv
	,cdc.oksdv6
	,cdc.oksdv6_conv
	,cdc.oksdv7
	,cdc.oksdv7_conv
	,cdc.oksdv8
	,cdc.oksdv8_conv
	,cdc.okogid
	,cdc.okogid_conv
FROM stg_jde_qat.tmp_upsert_f40943_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f40943_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f40943_new AF:{{ task_instance_key_str }}' ) 