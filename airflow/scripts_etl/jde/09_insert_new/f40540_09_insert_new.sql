-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f40540_new
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
	,icitm
	,iccmdcde
	,iccmdcde_conv
	,icunspsc
	,icunspsc_conv
	,icuser
	,icuser_conv
	,icpid
	,icpid_conv
	,icjobn
	,icjobn_conv
	,icupmt
	,icupmj
	,icupmj_conv
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
	,cdc.icitm
	,cdc.iccmdcde
	,cdc.iccmdcde_conv
	,cdc.icunspsc
	,cdc.icunspsc_conv
	,cdc.icuser
	,cdc.icuser_conv
	,cdc.icpid
	,cdc.icpid_conv
	,cdc.icjobn
	,cdc.icjobn_conv
	,cdc.icupmt
	,cdc.icupmj
	,cdc.icupmj_conv
FROM stg_jde.tmp_upsert_f40540_cdc cdc
LEFT OUTER JOIN rep_jde.f40540_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f40540_new AF:{{ task_instance_key_str }}' ) 