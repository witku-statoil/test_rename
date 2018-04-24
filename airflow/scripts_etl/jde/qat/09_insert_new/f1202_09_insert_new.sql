-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f1202_new
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
	,flaid
	,flaid_conv
	,flctry
	,flfy
	,flfy_conv
	,flfq
	,fllt
	,flsbl
	,flsbl_conv
	,flco
	,flco_conv
	,flapyc
	,flapyc_conv
	,flan01
	,flan01_conv
	,flan02
	,flan02_conv
	,flan03
	,flan03_conv
	,flan04
	,flan04_conv
	,flan05
	,flan05_conv
	,flan06
	,flan06_conv
	,flan07
	,flan07_conv
	,flan08
	,flan08_conv
	,flan09
	,flan09_conv
	,flan10
	,flan10_conv
	,flan11
	,flan11_conv
	,flan12
	,flan12_conv
	,flan13
	,flan13_conv
	,flan14
	,flan14_conv
	,flapyn
	,flapyn_conv
	,flawtd
	,flawtd_conv
	,flborg
	,flborg_conv
	,flpou
	,flpou_conv
	,flpc
	,fltker
	,fltker_conv
	,flbreq
	,flbreq_conv
	,flbapr
	,flbapr_conv
	,flmcu
	,flmcu_conv
	,flobj
	,flobj_conv
	,flsub
	,flsub_conv
	,flnumb
	,fladlm
	,fladm
	,flitac
	,fladmp
	,fladsn
	,fladsn_conv
	,fldir1
	,fldsd
	,fldsd_conv
	,fluser
	,fluser_conv
	,fllct
	,fllct_conv
	,flpid
	,flpid_conv
	,flsblt
	,flcrcd
	,flcrcd_conv
	,flupmj
	,flupmj_conv
	,fljobn
	,fljobn_conv
	,flupmt
	,flchcd
	,fldpcf
	,fldpcf_conv
	,flcbxr
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
	,cdc.flaid
	,cdc.flaid_conv
	,cdc.flctry
	,cdc.flfy
	,cdc.flfy_conv
	,cdc.flfq
	,cdc.fllt
	,cdc.flsbl
	,cdc.flsbl_conv
	,cdc.flco
	,cdc.flco_conv
	,cdc.flapyc
	,cdc.flapyc_conv
	,cdc.flan01
	,cdc.flan01_conv
	,cdc.flan02
	,cdc.flan02_conv
	,cdc.flan03
	,cdc.flan03_conv
	,cdc.flan04
	,cdc.flan04_conv
	,cdc.flan05
	,cdc.flan05_conv
	,cdc.flan06
	,cdc.flan06_conv
	,cdc.flan07
	,cdc.flan07_conv
	,cdc.flan08
	,cdc.flan08_conv
	,cdc.flan09
	,cdc.flan09_conv
	,cdc.flan10
	,cdc.flan10_conv
	,cdc.flan11
	,cdc.flan11_conv
	,cdc.flan12
	,cdc.flan12_conv
	,cdc.flan13
	,cdc.flan13_conv
	,cdc.flan14
	,cdc.flan14_conv
	,cdc.flapyn
	,cdc.flapyn_conv
	,cdc.flawtd
	,cdc.flawtd_conv
	,cdc.flborg
	,cdc.flborg_conv
	,cdc.flpou
	,cdc.flpou_conv
	,cdc.flpc
	,cdc.fltker
	,cdc.fltker_conv
	,cdc.flbreq
	,cdc.flbreq_conv
	,cdc.flbapr
	,cdc.flbapr_conv
	,cdc.flmcu
	,cdc.flmcu_conv
	,cdc.flobj
	,cdc.flobj_conv
	,cdc.flsub
	,cdc.flsub_conv
	,cdc.flnumb
	,cdc.fladlm
	,cdc.fladm
	,cdc.flitac
	,cdc.fladmp
	,cdc.fladsn
	,cdc.fladsn_conv
	,cdc.fldir1
	,cdc.fldsd
	,cdc.fldsd_conv
	,cdc.fluser
	,cdc.fluser_conv
	,cdc.fllct
	,cdc.fllct_conv
	,cdc.flpid
	,cdc.flpid_conv
	,cdc.flsblt
	,cdc.flcrcd
	,cdc.flcrcd_conv
	,cdc.flupmj
	,cdc.flupmj_conv
	,cdc.fljobn
	,cdc.fljobn_conv
	,cdc.flupmt
	,cdc.flchcd
	,cdc.fldpcf
	,cdc.fldpcf_conv
	,cdc.flcbxr
FROM stg_jde_qat.tmp_upsert_f1202_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f1202_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f1202_new AF:{{ task_instance_key_str }}' ) 