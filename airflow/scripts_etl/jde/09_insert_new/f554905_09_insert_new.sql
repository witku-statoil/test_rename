-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f554905_new
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
	,rdmcu
	,rdmcu_conv
	,rdmot
	,rdan8
	,rdvehi
	,rdvehi_conv
	,rdrtnm
	,rdcgc1
	,rdcgc2
	,rdfrtp
	,rdrtgb
	,rdrtum
	,rdfrcg
	,rdfrcg_conv
	,rdeftj
	,rdeftj_conv
	,rdexdj
	,rdexdj_conv
	,rdcrcd
	,rdcrcd_conv
	,rduser
	,rduser_conv
	,rdpid
	,rdpid_conv
	,rdjobn
	,rdjobn_conv
	,rdupmj
	,rdupmj_conv
	,rdupmt
	,rdurcd
	,rdurcd_conv
	,rdurdt
	,rdurdt_conv
	,rdurat
	,rdurat_conv
	,rdurab
	,rdurab_conv
	,rdurrf
	,rdurrf_conv
	,rdy55char1
	,rdy55char1_conv
	,rdy55char2
	,rdy55char2_conv
	,rdy55date1
	,rdy55date1_conv
	,rdy55date2
	,rdy55date2_conv
	,rdy55strg1
	,rdy55strg1_conv
	,rdy55strg2
	,rdy55strg2_conv
	,rdy55time1
	,rdy55time2
	,rdy55amnt1
	,rdy55amnt1_conv
	,rdy55amnt2
	,rdy55amnt2_conv
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
	,cdc.rdmcu
	,cdc.rdmcu_conv
	,cdc.rdmot
	,cdc.rdan8
	,cdc.rdvehi
	,cdc.rdvehi_conv
	,cdc.rdrtnm
	,cdc.rdcgc1
	,cdc.rdcgc2
	,cdc.rdfrtp
	,cdc.rdrtgb
	,cdc.rdrtum
	,cdc.rdfrcg
	,cdc.rdfrcg_conv
	,cdc.rdeftj
	,cdc.rdeftj_conv
	,cdc.rdexdj
	,cdc.rdexdj_conv
	,cdc.rdcrcd
	,cdc.rdcrcd_conv
	,cdc.rduser
	,cdc.rduser_conv
	,cdc.rdpid
	,cdc.rdpid_conv
	,cdc.rdjobn
	,cdc.rdjobn_conv
	,cdc.rdupmj
	,cdc.rdupmj_conv
	,cdc.rdupmt
	,cdc.rdurcd
	,cdc.rdurcd_conv
	,cdc.rdurdt
	,cdc.rdurdt_conv
	,cdc.rdurat
	,cdc.rdurat_conv
	,cdc.rdurab
	,cdc.rdurab_conv
	,cdc.rdurrf
	,cdc.rdurrf_conv
	,cdc.rdy55char1
	,cdc.rdy55char1_conv
	,cdc.rdy55char2
	,cdc.rdy55char2_conv
	,cdc.rdy55date1
	,cdc.rdy55date1_conv
	,cdc.rdy55date2
	,cdc.rdy55date2_conv
	,cdc.rdy55strg1
	,cdc.rdy55strg1_conv
	,cdc.rdy55strg2
	,cdc.rdy55strg2_conv
	,cdc.rdy55time1
	,cdc.rdy55time2
	,cdc.rdy55amnt1
	,cdc.rdy55amnt1_conv
	,cdc.rdy55amnt2
	,cdc.rdy55amnt2_conv
FROM stg_jde.tmp_upsert_f554905_cdc cdc
LEFT OUTER JOIN rep_jde.f554905_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f554905_new AF:{{ task_instance_key_str }}' ) 