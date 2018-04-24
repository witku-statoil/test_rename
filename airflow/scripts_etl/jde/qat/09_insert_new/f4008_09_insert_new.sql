-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f4008_new
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
	,tatxa1
	,tatxa1_conv
	,tataxa
	,tataxa_conv
	,tata1
	,tatxr1
	,tatxr1_conv
	,tata2
	,tatxr2
	,tatxr2_conv
	,tata3
	,tatxr3
	,tatxr3_conv
	,tata4
	,tatxr4
	,tatxr4_conv
	,tata5
	,tatxr5
	,tatxr5_conv
	,taefdj
	,taefdj_conv
	,taeftj
	,taeftj_conv
	,tagl01
	,tagl01_conv
	,tagl02
	,tagl02_conv
	,tagl03
	,tagl03_conv
	,tagl04
	,tagl04_conv
	,tagl05
	,tagl05_conv
	,taitm
	,talitm
	,talitm_conv
	,taaitm
	,taaitm_conv
	,tauom
	,tafvty
	,tamtax
	,tamtax_conv
	,tatc1
	,tatc1_conv
	,tatc2
	,tatc2_conv
	,tatc3
	,tatc3_conv
	,tatc4
	,tatc4_conv
	,tatc5
	,tatc5_conv
	,tatt1
	,tatt1_conv
	,tatt2
	,tatt2_conv
	,tatt3
	,tatt4
	,tatt5
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
	,cdc.tatxa1
	,cdc.tatxa1_conv
	,cdc.tataxa
	,cdc.tataxa_conv
	,cdc.tata1
	,cdc.tatxr1
	,cdc.tatxr1_conv
	,cdc.tata2
	,cdc.tatxr2
	,cdc.tatxr2_conv
	,cdc.tata3
	,cdc.tatxr3
	,cdc.tatxr3_conv
	,cdc.tata4
	,cdc.tatxr4
	,cdc.tatxr4_conv
	,cdc.tata5
	,cdc.tatxr5
	,cdc.tatxr5_conv
	,cdc.taefdj
	,cdc.taefdj_conv
	,cdc.taeftj
	,cdc.taeftj_conv
	,cdc.tagl01
	,cdc.tagl01_conv
	,cdc.tagl02
	,cdc.tagl02_conv
	,cdc.tagl03
	,cdc.tagl03_conv
	,cdc.tagl04
	,cdc.tagl04_conv
	,cdc.tagl05
	,cdc.tagl05_conv
	,cdc.taitm
	,cdc.talitm
	,cdc.talitm_conv
	,cdc.taaitm
	,cdc.taaitm_conv
	,cdc.tauom
	,cdc.tafvty
	,cdc.tamtax
	,cdc.tamtax_conv
	,cdc.tatc1
	,cdc.tatc1_conv
	,cdc.tatc2
	,cdc.tatc2_conv
	,cdc.tatc3
	,cdc.tatc3_conv
	,cdc.tatc4
	,cdc.tatc4_conv
	,cdc.tatc5
	,cdc.tatc5_conv
	,cdc.tatt1
	,cdc.tatt1_conv
	,cdc.tatt2
	,cdc.tatt2_conv
	,cdc.tatt3
	,cdc.tatt4
	,cdc.tatt5
FROM stg_jde_qat.tmp_upsert_f4008_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f4008_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f4008_new AF:{{ task_instance_key_str }}' ) 