-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f4209_new
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
	,hohcod
	,horper
	,horper_conv
	,hoan8
	,homcu
	,homcu_conv
	,hokcoo
	,hokcoo_conv
	,hodoco
	,hodcto
	,hosfxo
	,hosfxo_conv
	,holnid
	,holnid_conv
	,hoitm
	,holitm
	,holitm_conv
	,hoaitm
	,hoaitm_conv
	,hotrdj
	,hotrdj_conv
	,hodrqj
	,hodrqj_conv
	,hopddj
	,hopddj_conv
	,hoctyp
	,hordc
	,hordb
	,hordb_conv
	,hordj
	,hordj_conv
	,hordt
	,hoartg
	,hoartg_conv
	,hoasts
	,hoasts_conv
	,hoaty
	,hoaty_conv
	,hoedei
	,hoedei_conv
	,hodlnid
	,hodlnid_conv
	,hopa8
	,hoshan
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
	,cdc.hohcod
	,cdc.horper
	,cdc.horper_conv
	,cdc.hoan8
	,cdc.homcu
	,cdc.homcu_conv
	,cdc.hokcoo
	,cdc.hokcoo_conv
	,cdc.hodoco
	,cdc.hodcto
	,cdc.hosfxo
	,cdc.hosfxo_conv
	,cdc.holnid
	,cdc.holnid_conv
	,cdc.hoitm
	,cdc.holitm
	,cdc.holitm_conv
	,cdc.hoaitm
	,cdc.hoaitm_conv
	,cdc.hotrdj
	,cdc.hotrdj_conv
	,cdc.hodrqj
	,cdc.hodrqj_conv
	,cdc.hopddj
	,cdc.hopddj_conv
	,cdc.hoctyp
	,cdc.hordc
	,cdc.hordb
	,cdc.hordb_conv
	,cdc.hordj
	,cdc.hordj_conv
	,cdc.hordt
	,cdc.hoartg
	,cdc.hoartg_conv
	,cdc.hoasts
	,cdc.hoasts_conv
	,cdc.hoaty
	,cdc.hoaty_conv
	,cdc.hoedei
	,cdc.hoedei_conv
	,cdc.hodlnid
	,cdc.hodlnid_conv
	,cdc.hopa8
	,cdc.hoshan
FROM stg_jde.tmp_upsert_f4209_cdc cdc
LEFT OUTER JOIN rep_jde.f4209_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f4209_new AF:{{ task_instance_key_str }}' ) 