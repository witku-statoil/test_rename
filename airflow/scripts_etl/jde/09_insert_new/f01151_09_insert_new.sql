-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f01151_new
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
	,eaan8
	,eaidln
	,eaidln_conv
	,earck7
	,eaetp
	,eaemal
	,eaemal_conv
	,eauser
	,eauser_conv
	,eapid
	,eapid_conv
	,eaupmj
	,eaupmj_conv
	,eajobn
	,eajobn_conv
	,eaupmt
	,eaehier
	,eaefor
	,eaefor_conv
	,eaeclass
	,eacfno1
	,eacfno1_conv
	,eagen1
	,eagen1_conv
	,eafalge
	,eafalge_conv
	,easyncs
	,eacaad
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
	,cdc.eaan8
	,cdc.eaidln
	,cdc.eaidln_conv
	,cdc.earck7
	,cdc.eaetp
	,cdc.eaemal
	,cdc.eaemal_conv
	,cdc.eauser
	,cdc.eauser_conv
	,cdc.eapid
	,cdc.eapid_conv
	,cdc.eaupmj
	,cdc.eaupmj_conv
	,cdc.eajobn
	,cdc.eajobn_conv
	,cdc.eaupmt
	,cdc.eaehier
	,cdc.eaefor
	,cdc.eaefor_conv
	,cdc.eaeclass
	,cdc.eacfno1
	,cdc.eacfno1_conv
	,cdc.eagen1
	,cdc.eagen1_conv
	,cdc.eafalge
	,cdc.eafalge_conv
	,cdc.easyncs
	,cdc.eacaad
FROM stg_jde.tmp_upsert_f01151_cdc cdc
LEFT OUTER JOIN rep_jde.f01151_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f01151_new AF:{{ task_instance_key_str }}' ) 