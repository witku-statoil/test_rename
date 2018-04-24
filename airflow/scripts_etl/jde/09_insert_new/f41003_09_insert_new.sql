-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f41003_new
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
	,ucum
	,ucrum
	,ucconv
	,ucconv_conv
	,ucuser
	,ucuser_conv
	,ucpid
	,ucpid_conv
	,ucjobn
	,ucjobn_conv
	,ucupmj
	,ucupmj_conv
	,uctday
	,ucexpo
	,ucexpo_conv
	,ucexso
	,ucexso_conv
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
	,cdc.ucum
	,cdc.ucrum
	,cdc.ucconv
	,cdc.ucconv_conv
	,cdc.ucuser
	,cdc.ucuser_conv
	,cdc.ucpid
	,cdc.ucpid_conv
	,cdc.ucjobn
	,cdc.ucjobn_conv
	,cdc.ucupmj
	,cdc.ucupmj_conv
	,cdc.uctday
	,cdc.ucexpo
	,cdc.ucexpo_conv
	,cdc.ucexso
	,cdc.ucexso_conv
FROM stg_jde.tmp_upsert_f41003_cdc cdc
LEFT OUTER JOIN rep_jde.f41003_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f41003_new AF:{{ task_instance_key_str }}' ) 