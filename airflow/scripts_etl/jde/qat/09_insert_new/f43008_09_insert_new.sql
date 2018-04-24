-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f43008_new
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
	,apdcto
	,apartg
	,apartg_conv
	,apdl01
	,apdl01_conv
	,apalim
	,apalim_conv
	,aprper
	,aprper_conv
	,apaty
	,apaty_conv
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
	,cdc.apdcto
	,cdc.apartg
	,cdc.apartg_conv
	,cdc.apdl01
	,cdc.apdl01_conv
	,cdc.apalim
	,cdc.apalim_conv
	,cdc.aprper
	,cdc.aprper_conv
	,cdc.apaty
	,cdc.apaty_conv
FROM stg_jde_qat.tmp_upsert_f43008_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f43008_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f43008_new AF:{{ task_instance_key_str }}' ) 