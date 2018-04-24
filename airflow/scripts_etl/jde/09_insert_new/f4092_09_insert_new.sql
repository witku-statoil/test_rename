-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f4092_new
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
	,gpgpty
	,gpgpty_conv
	,gpgpc
	,gpgpc_conv
	,gpdl01
	,gpdl01_conv
	,gpgpk1
	,gpgpk1_conv
	,gpgpk2
	,gpgpk2_conv
	,gpgpk3
	,gpgpk3_conv
	,gpgpk4
	,gpgpk4_conv
	,gpgpk5
	,gpgpk5_conv
	,gpgpk6
	,gpgpk6_conv
	,gpgpk7
	,gpgpk7_conv
	,gpgpk8
	,gpgpk8_conv
	,gpgpk9
	,gpgpk9_conv
	,gpgpk10
	,gpgpk10_conv
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
	,cdc.gpgpty
	,cdc.gpgpty_conv
	,cdc.gpgpc
	,cdc.gpgpc_conv
	,cdc.gpdl01
	,cdc.gpdl01_conv
	,cdc.gpgpk1
	,cdc.gpgpk1_conv
	,cdc.gpgpk2
	,cdc.gpgpk2_conv
	,cdc.gpgpk3
	,cdc.gpgpk3_conv
	,cdc.gpgpk4
	,cdc.gpgpk4_conv
	,cdc.gpgpk5
	,cdc.gpgpk5_conv
	,cdc.gpgpk6
	,cdc.gpgpk6_conv
	,cdc.gpgpk7
	,cdc.gpgpk7_conv
	,cdc.gpgpk8
	,cdc.gpgpk8_conv
	,cdc.gpgpk9
	,cdc.gpgpk9_conv
	,cdc.gpgpk10
	,cdc.gpgpk10_conv
FROM stg_jde.tmp_upsert_f4092_cdc cdc
LEFT OUTER JOIN rep_jde.f4092_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f4092_new AF:{{ task_instance_key_str }}' ) 