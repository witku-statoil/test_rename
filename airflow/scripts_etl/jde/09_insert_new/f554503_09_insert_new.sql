-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f554503_new
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
	,pry55qn
	,pry55qn_conv
	,pry55qt
	,pry55qdte
	,pry55qdte_conv
	,pry55qs
	,pry55quty
	,pry55quty_conv
	,pruncs
	,pruncs_conv
	,prcrcd
	,prcrcd_conv
	,pruom
	,pry55avin
	,pry55jdqn
	,pry55jdqn_conv
	,prflag
	,prflag_conv
	,prurrf
	,prurrf_conv
	,prurcd
	,prurcd_conv
	,prurab
	,prurab_conv
	,prurat
	,prurat_conv
	,prurdt
	,prurdt_conv
	,pruser
	,pruser_conv
	,prpid
	,prpid_conv
	,prjobn
	,prjobn_conv
	,prupmt
	,prupmj
	,prupmj_conv
	,pry55char1
	,pry55char1_conv
	,pry55char2
	,pry55char2_conv
	,pry55date1
	,pry55date1_conv
	,pry55date2
	,pry55date2_conv
	,pry55strg1
	,pry55strg1_conv
	,pry55strg2
	,pry55strg2_conv
	,pry55time1
	,pry55time2
	,pry55amnt1
	,pry55amnt1_conv
	,pry55amnt2
	,pry55amnt2_conv
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
	,cdc.pry55qn
	,cdc.pry55qn_conv
	,cdc.pry55qt
	,cdc.pry55qdte
	,cdc.pry55qdte_conv
	,cdc.pry55qs
	,cdc.pry55quty
	,cdc.pry55quty_conv
	,cdc.pruncs
	,cdc.pruncs_conv
	,cdc.prcrcd
	,cdc.prcrcd_conv
	,cdc.pruom
	,cdc.pry55avin
	,cdc.pry55jdqn
	,cdc.pry55jdqn_conv
	,cdc.prflag
	,cdc.prflag_conv
	,cdc.prurrf
	,cdc.prurrf_conv
	,cdc.prurcd
	,cdc.prurcd_conv
	,cdc.prurab
	,cdc.prurab_conv
	,cdc.prurat
	,cdc.prurat_conv
	,cdc.prurdt
	,cdc.prurdt_conv
	,cdc.pruser
	,cdc.pruser_conv
	,cdc.prpid
	,cdc.prpid_conv
	,cdc.prjobn
	,cdc.prjobn_conv
	,cdc.prupmt
	,cdc.prupmj
	,cdc.prupmj_conv
	,cdc.pry55char1
	,cdc.pry55char1_conv
	,cdc.pry55char2
	,cdc.pry55char2_conv
	,cdc.pry55date1
	,cdc.pry55date1_conv
	,cdc.pry55date2
	,cdc.pry55date2_conv
	,cdc.pry55strg1
	,cdc.pry55strg1_conv
	,cdc.pry55strg2
	,cdc.pry55strg2_conv
	,cdc.pry55time1
	,cdc.pry55time2
	,cdc.pry55amnt1
	,cdc.pry55amnt1_conv
	,cdc.pry55amnt2
	,cdc.pry55amnt2_conv
FROM stg_jde.tmp_upsert_f554503_cdc cdc
LEFT OUTER JOIN rep_jde.f554503_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f554503_new AF:{{ task_instance_key_str }}' ) 