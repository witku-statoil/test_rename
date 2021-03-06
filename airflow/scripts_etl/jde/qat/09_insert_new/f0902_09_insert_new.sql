-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f0902_new
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
	,gbaid
	,gbaid_conv
	,gbctry
	,gbfy
	,gbfy_conv
	,gbfq
	,gblt
	,gbsbl
	,gbsbl_conv
	,gbco
	,gbco_conv
	,gbapyc
	,gbapyc_conv
	,gban01
	,gban01_conv
	,gban02
	,gban02_conv
	,gban03
	,gban03_conv
	,gban04
	,gban04_conv
	,gban05
	,gban05_conv
	,gban06
	,gban06_conv
	,gban07
	,gban07_conv
	,gban08
	,gban08_conv
	,gban09
	,gban09_conv
	,gban10
	,gban10_conv
	,gban11
	,gban11_conv
	,gban12
	,gban12_conv
	,gban13
	,gban13_conv
	,gban14
	,gban14_conv
	,gbapyn
	,gbapyn_conv
	,gbawtd
	,gbawtd_conv
	,gbborg
	,gbborg_conv
	,gbpou
	,gbpou_conv
	,gbpc
	,gbtker
	,gbtker_conv
	,gbbreq
	,gbbreq_conv
	,gbbapr
	,gbbapr_conv
	,gbmcu
	,gbmcu_conv
	,gbobj
	,gbobj_conv
	,gbsub
	,gbsub_conv
	,gbuser
	,gbuser_conv
	,gbpid
	,gbpid_conv
	,gbupmj
	,gbupmj_conv
	,gbjobn
	,gbjobn_conv
	,gbsblt
	,gbupmt
	,gbcrcd
	,gbcrcd_conv
	,gbcrcx
	,gbcrcx_conv
	,gbprgf
	,gbprgf_conv
	,gband01
	,gband01_conv
	,gband02
	,gband02_conv
	,gband03
	,gband03_conv
	,gband04
	,gband04_conv
	,gband05
	,gband05_conv
	,gband06
	,gband06_conv
	,gband07
	,gband07_conv
	,gband08
	,gband08_conv
	,gband09
	,gband09_conv
	,gband10
	,gband10_conv
	,gband11
	,gband11_conv
	,gband12
	,gband12_conv
	,gband13
	,gband13_conv
	,gband14
	,gband14_conv
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
	,cdc.gbaid
	,cdc.gbaid_conv
	,cdc.gbctry
	,cdc.gbfy
	,cdc.gbfy_conv
	,cdc.gbfq
	,cdc.gblt
	,cdc.gbsbl
	,cdc.gbsbl_conv
	,cdc.gbco
	,cdc.gbco_conv
	,cdc.gbapyc
	,cdc.gbapyc_conv
	,cdc.gban01
	,cdc.gban01_conv
	,cdc.gban02
	,cdc.gban02_conv
	,cdc.gban03
	,cdc.gban03_conv
	,cdc.gban04
	,cdc.gban04_conv
	,cdc.gban05
	,cdc.gban05_conv
	,cdc.gban06
	,cdc.gban06_conv
	,cdc.gban07
	,cdc.gban07_conv
	,cdc.gban08
	,cdc.gban08_conv
	,cdc.gban09
	,cdc.gban09_conv
	,cdc.gban10
	,cdc.gban10_conv
	,cdc.gban11
	,cdc.gban11_conv
	,cdc.gban12
	,cdc.gban12_conv
	,cdc.gban13
	,cdc.gban13_conv
	,cdc.gban14
	,cdc.gban14_conv
	,cdc.gbapyn
	,cdc.gbapyn_conv
	,cdc.gbawtd
	,cdc.gbawtd_conv
	,cdc.gbborg
	,cdc.gbborg_conv
	,cdc.gbpou
	,cdc.gbpou_conv
	,cdc.gbpc
	,cdc.gbtker
	,cdc.gbtker_conv
	,cdc.gbbreq
	,cdc.gbbreq_conv
	,cdc.gbbapr
	,cdc.gbbapr_conv
	,cdc.gbmcu
	,cdc.gbmcu_conv
	,cdc.gbobj
	,cdc.gbobj_conv
	,cdc.gbsub
	,cdc.gbsub_conv
	,cdc.gbuser
	,cdc.gbuser_conv
	,cdc.gbpid
	,cdc.gbpid_conv
	,cdc.gbupmj
	,cdc.gbupmj_conv
	,cdc.gbjobn
	,cdc.gbjobn_conv
	,cdc.gbsblt
	,cdc.gbupmt
	,cdc.gbcrcd
	,cdc.gbcrcd_conv
	,cdc.gbcrcx
	,cdc.gbcrcx_conv
	,cdc.gbprgf
	,cdc.gbprgf_conv
	,cdc.gband01
	,cdc.gband01_conv
	,cdc.gband02
	,cdc.gband02_conv
	,cdc.gband03
	,cdc.gband03_conv
	,cdc.gband04
	,cdc.gband04_conv
	,cdc.gband05
	,cdc.gband05_conv
	,cdc.gband06
	,cdc.gband06_conv
	,cdc.gband07
	,cdc.gband07_conv
	,cdc.gband08
	,cdc.gband08_conv
	,cdc.gband09
	,cdc.gband09_conv
	,cdc.gband10
	,cdc.gband10_conv
	,cdc.gband11
	,cdc.gband11_conv
	,cdc.gband12
	,cdc.gband12_conv
	,cdc.gband13
	,cdc.gband13_conv
	,cdc.gband14
	,cdc.gband14_conv
FROM stg_jde_qat.tmp_upsert_f0902_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f0902_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f0902_new AF:{{ task_instance_key_str }}' ) 