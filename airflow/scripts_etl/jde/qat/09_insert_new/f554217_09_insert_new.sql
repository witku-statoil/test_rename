-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f554217_new
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
	,span8
	,spac06
	,spaddz
	,spaddz_conv
	,spshan
	,spat1
	,spac01
	,spac02
	,spac03
	,spar1
	,spar1_conv
	,spph1
	,spph1_conv
	,spcmtb
	,spcmtb_conv
	,spptmb
	,spcmte
	,spcmte_conv
	,spev01
	,spev01_conv
	,spptme
	,spurab
	,spurab_conv
	,spupmj
	,spupmj_conv
	,spupmt
	,spjobn
	,spjobn_conv
	,sppid
	,sppid_conv
	,spuser
	,spuser_conv
	,spmcu
	,spmcu_conv
	,sprp24
	,spy55char1
	,spy55char1_conv
	,spy55char2
	,spy55char2_conv
	,spy55strg1
	,spy55strg1_conv
	,spy55strg2
	,spy55strg2_conv
	,spy55amnt1
	,spy55amnt1_conv
	,spy55amnt2
	,spy55amnt2_conv
	,spy55time1
	,spy55time2
	,spy55date1
	,spy55date1_conv
	,spy55date2
	,spy55date2_conv
	,spcrtj
	,spcrtj_conv
	,spterm
	,spterm_conv
	,sppa8
	,sptx1
	,sptx1_conv
	,sptax
	,sptax_conv
	,spaddznh
	,spaddznh_conv
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
	,cdc.span8
	,cdc.spac06
	,cdc.spaddz
	,cdc.spaddz_conv
	,cdc.spshan
	,cdc.spat1
	,cdc.spac01
	,cdc.spac02
	,cdc.spac03
	,cdc.spar1
	,cdc.spar1_conv
	,cdc.spph1
	,cdc.spph1_conv
	,cdc.spcmtb
	,cdc.spcmtb_conv
	,cdc.spptmb
	,cdc.spcmte
	,cdc.spcmte_conv
	,cdc.spev01
	,cdc.spev01_conv
	,cdc.spptme
	,cdc.spurab
	,cdc.spurab_conv
	,cdc.spupmj
	,cdc.spupmj_conv
	,cdc.spupmt
	,cdc.spjobn
	,cdc.spjobn_conv
	,cdc.sppid
	,cdc.sppid_conv
	,cdc.spuser
	,cdc.spuser_conv
	,cdc.spmcu
	,cdc.spmcu_conv
	,cdc.sprp24
	,cdc.spy55char1
	,cdc.spy55char1_conv
	,cdc.spy55char2
	,cdc.spy55char2_conv
	,cdc.spy55strg1
	,cdc.spy55strg1_conv
	,cdc.spy55strg2
	,cdc.spy55strg2_conv
	,cdc.spy55amnt1
	,cdc.spy55amnt1_conv
	,cdc.spy55amnt2
	,cdc.spy55amnt2_conv
	,cdc.spy55time1
	,cdc.spy55time2
	,cdc.spy55date1
	,cdc.spy55date1_conv
	,cdc.spy55date2
	,cdc.spy55date2_conv
	,cdc.spcrtj
	,cdc.spcrtj_conv
	,cdc.spterm
	,cdc.spterm_conv
	,cdc.sppa8
	,cdc.sptx1
	,cdc.sptx1_conv
	,cdc.sptax
	,cdc.sptax_conv
	,cdc.spaddznh
	,cdc.spaddznh_conv
FROM stg_jde_qat.tmp_upsert_f554217_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f554217_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f554217_new AF:{{ task_instance_key_str }}' ) 