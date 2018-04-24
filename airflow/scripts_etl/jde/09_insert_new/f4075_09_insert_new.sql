-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f4075_new
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
	,vbvbt
	,vbcrcd
	,vbcrcd_conv
	,vbuom
	,vbuprc
	,vbuprc_conv
	,vbeftj
	,vbeftj_conv
	,vbexdj
	,vbexdj_conv
	,vbaprs
	,vbuser
	,vbuser_conv
	,vbpid
	,vbpid_conv
	,vbjobn
	,vbjobn_conv
	,vbupmj
	,vbupmj_conv
	,vbtday
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
	,cdc.vbvbt
	,cdc.vbcrcd
	,cdc.vbcrcd_conv
	,cdc.vbuom
	,cdc.vbuprc
	,cdc.vbuprc_conv
	,cdc.vbeftj
	,cdc.vbeftj_conv
	,cdc.vbexdj
	,cdc.vbexdj_conv
	,cdc.vbaprs
	,cdc.vbuser
	,cdc.vbuser_conv
	,cdc.vbpid
	,cdc.vbpid_conv
	,cdc.vbjobn
	,cdc.vbjobn_conv
	,cdc.vbupmj
	,cdc.vbupmj_conv
	,cdc.vbtday
FROM stg_jde.tmp_upsert_f4075_cdc cdc
LEFT OUTER JOIN rep_jde.f4075_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f4075_new AF:{{ task_instance_key_str }}' ) 