-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f4076_new
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
	,fmfrmn
	,fmfml
	,fmfml_conv
	,fmaprs
	,fmuser
	,fmuser_conv
	,fmpid
	,fmpid_conv
	,fmjobn
	,fmjobn_conv
	,fmupmj
	,fmupmj_conv
	,fmtday
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
	,cdc.fmfrmn
	,cdc.fmfml
	,cdc.fmfml_conv
	,cdc.fmaprs
	,cdc.fmuser
	,cdc.fmuser_conv
	,cdc.fmpid
	,cdc.fmpid_conv
	,cdc.fmjobn
	,cdc.fmjobn_conv
	,cdc.fmupmj
	,cdc.fmupmj_conv
	,cdc.fmtday
FROM stg_jde_qat.tmp_upsert_f4076_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f4076_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f4076_new AF:{{ task_instance_key_str }}' ) 