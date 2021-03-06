-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f38011_new
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
	,dfdmct
	,dfdmct_conv
	,dfdmcs
	,dfcnqy
	,dfdto
	,dfdes
	,dfdes_conv
	,dfdesy
	,dfitm
	,dfseq
	,dfcnqt
	,dfcnqt_conv
	,dfaa
	,dfaa_conv
	,dfuom
	,dfminq
	,dfminq_conv
	,dfmaxq
	,dfmaxq_conv
	,dfeftj
	,dfeftj_conv
	,dfexdj
	,dfexdj_conv
	,dfuprc
	,dfuprc_conv
	,dfasn
	,dfpasn
	,dfmcu
	,dfmcu_conv
	,dfcrcd
	,dfcrcd_conv
	,dftv01
	,dftv01_conv
	,dftv02
	,dftv02_conv
	,dftv03
	,dftv03_conv
	,dfqty1
	,dfqty1_conv
	,dftvum
	,dfurcd
	,dfurcd_conv
	,dfurdt
	,dfurdt_conv
	,dfurat
	,dfurat_conv
	,dfurab
	,dfurab_conv
	,dfurrf
	,dfurrf_conv
	,dfuser
	,dfuser_conv
	,dfpid
	,dfpid_conv
	,dfjobn
	,dfjobn_conv
	,dfupmj
	,dfupmj_conv
	,dftday
	,dfrpqt
	,dfrpqt_conv
	,dfqedt
	,dfqedt_conv
	,dfqed3
	,dfqed3_conv
	,dfqed2
	,dfqed2_conv
	,dfpdp5
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
	,cdc.dfdmct
	,cdc.dfdmct_conv
	,cdc.dfdmcs
	,cdc.dfcnqy
	,cdc.dfdto
	,cdc.dfdes
	,cdc.dfdes_conv
	,cdc.dfdesy
	,cdc.dfitm
	,cdc.dfseq
	,cdc.dfcnqt
	,cdc.dfcnqt_conv
	,cdc.dfaa
	,cdc.dfaa_conv
	,cdc.dfuom
	,cdc.dfminq
	,cdc.dfminq_conv
	,cdc.dfmaxq
	,cdc.dfmaxq_conv
	,cdc.dfeftj
	,cdc.dfeftj_conv
	,cdc.dfexdj
	,cdc.dfexdj_conv
	,cdc.dfuprc
	,cdc.dfuprc_conv
	,cdc.dfasn
	,cdc.dfpasn
	,cdc.dfmcu
	,cdc.dfmcu_conv
	,cdc.dfcrcd
	,cdc.dfcrcd_conv
	,cdc.dftv01
	,cdc.dftv01_conv
	,cdc.dftv02
	,cdc.dftv02_conv
	,cdc.dftv03
	,cdc.dftv03_conv
	,cdc.dfqty1
	,cdc.dfqty1_conv
	,cdc.dftvum
	,cdc.dfurcd
	,cdc.dfurcd_conv
	,cdc.dfurdt
	,cdc.dfurdt_conv
	,cdc.dfurat
	,cdc.dfurat_conv
	,cdc.dfurab
	,cdc.dfurab_conv
	,cdc.dfurrf
	,cdc.dfurrf_conv
	,cdc.dfuser
	,cdc.dfuser_conv
	,cdc.dfpid
	,cdc.dfpid_conv
	,cdc.dfjobn
	,cdc.dfjobn_conv
	,cdc.dfupmj
	,cdc.dfupmj_conv
	,cdc.dftday
	,cdc.dfrpqt
	,cdc.dfrpqt_conv
	,cdc.dfqedt
	,cdc.dfqedt_conv
	,cdc.dfqed3
	,cdc.dfqed3_conv
	,cdc.dfqed2
	,cdc.dfqed2_conv
	,cdc.dfpdp5
FROM stg_jde_qat.tmp_upsert_f38011_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f38011_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f38011_new AF:{{ task_instance_key_str }}' ) 