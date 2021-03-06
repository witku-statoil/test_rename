-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f4006_new
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
	,oadoco
	,oadcto
	,oakcoo
	,oakcoo_conv
	,oaanty
	,oamlnm
	,oamlnm_conv
	,oaadd1
	,oaadd1_conv
	,oaadd2
	,oaadd2_conv
	,oaadd3
	,oaadd3_conv
	,oaadd4
	,oaadd4_conv
	,oaaddz
	,oaaddz_conv
	,oacty1
	,oacty1_conv
	,oacoun
	,oaadds
	,oacrte
	,oacrte_conv
	,oabkml
	,oabkml_conv
	,oactr
	,oauser
	,oauser_conv
	,oapid
	,oapid_conv
	,oaupmj
	,oaupmj_conv
	,oajobn
	,oajobn_conv
	,oaupmt
	,oalnid
	,oalnid_conv
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
	,cdc.oadoco
	,cdc.oadcto
	,cdc.oakcoo
	,cdc.oakcoo_conv
	,cdc.oaanty
	,cdc.oamlnm
	,cdc.oamlnm_conv
	,cdc.oaadd1
	,cdc.oaadd1_conv
	,cdc.oaadd2
	,cdc.oaadd2_conv
	,cdc.oaadd3
	,cdc.oaadd3_conv
	,cdc.oaadd4
	,cdc.oaadd4_conv
	,cdc.oaaddz
	,cdc.oaaddz_conv
	,cdc.oacty1
	,cdc.oacty1_conv
	,cdc.oacoun
	,cdc.oaadds
	,cdc.oacrte
	,cdc.oacrte_conv
	,cdc.oabkml
	,cdc.oabkml_conv
	,cdc.oactr
	,cdc.oauser
	,cdc.oauser_conv
	,cdc.oapid
	,cdc.oapid_conv
	,cdc.oaupmj
	,cdc.oaupmj_conv
	,cdc.oajobn
	,cdc.oajobn_conv
	,cdc.oaupmt
	,cdc.oalnid
	,cdc.oalnid_conv
FROM stg_jde_qat.tmp_upsert_f4006_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f4006_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f4006_new AF:{{ task_instance_key_str }}' ) 