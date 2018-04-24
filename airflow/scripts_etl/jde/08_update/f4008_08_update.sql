-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4008_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[tatxa1] = cdc.[tatxa1]
	,[tatxa1_conv] = cdc.[tatxa1_conv]
	,[tataxa] = cdc.[tataxa]
	,[tataxa_conv] = cdc.[tataxa_conv]
	,[tata1] = cdc.[tata1]
	,[tatxr1] = cdc.[tatxr1]
	,[tatxr1_conv] = cdc.[tatxr1_conv]
	,[tata2] = cdc.[tata2]
	,[tatxr2] = cdc.[tatxr2]
	,[tatxr2_conv] = cdc.[tatxr2_conv]
	,[tata3] = cdc.[tata3]
	,[tatxr3] = cdc.[tatxr3]
	,[tatxr3_conv] = cdc.[tatxr3_conv]
	,[tata4] = cdc.[tata4]
	,[tatxr4] = cdc.[tatxr4]
	,[tatxr4_conv] = cdc.[tatxr4_conv]
	,[tata5] = cdc.[tata5]
	,[tatxr5] = cdc.[tatxr5]
	,[tatxr5_conv] = cdc.[tatxr5_conv]
	,[taefdj] = cdc.[taefdj]
	,[taefdj_conv] = cdc.[taefdj_conv]
	,[taeftj] = cdc.[taeftj]
	,[taeftj_conv] = cdc.[taeftj_conv]
	,[tagl01] = cdc.[tagl01]
	,[tagl01_conv] = cdc.[tagl01_conv]
	,[tagl02] = cdc.[tagl02]
	,[tagl02_conv] = cdc.[tagl02_conv]
	,[tagl03] = cdc.[tagl03]
	,[tagl03_conv] = cdc.[tagl03_conv]
	,[tagl04] = cdc.[tagl04]
	,[tagl04_conv] = cdc.[tagl04_conv]
	,[tagl05] = cdc.[tagl05]
	,[tagl05_conv] = cdc.[tagl05_conv]
	,[taitm] = cdc.[taitm]
	,[talitm] = cdc.[talitm]
	,[talitm_conv] = cdc.[talitm_conv]
	,[taaitm] = cdc.[taaitm]
	,[taaitm_conv] = cdc.[taaitm_conv]
	,[tauom] = cdc.[tauom]
	,[tafvty] = cdc.[tafvty]
	,[tamtax] = cdc.[tamtax]
	,[tamtax_conv] = cdc.[tamtax_conv]
	,[tatc1] = cdc.[tatc1]
	,[tatc1_conv] = cdc.[tatc1_conv]
	,[tatc2] = cdc.[tatc2]
	,[tatc2_conv] = cdc.[tatc2_conv]
	,[tatc3] = cdc.[tatc3]
	,[tatc3_conv] = cdc.[tatc3_conv]
	,[tatc4] = cdc.[tatc4]
	,[tatc4_conv] = cdc.[tatc4_conv]
	,[tatc5] = cdc.[tatc5]
	,[tatc5_conv] = cdc.[tatc5_conv]
	,[tatt1] = cdc.[tatt1]
	,[tatt1_conv] = cdc.[tatt1_conv]
	,[tatt2] = cdc.[tatt2]
	,[tatt2_conv] = cdc.[tatt2_conv]
	,[tatt3] = cdc.[tatt3]
	,[tatt4] = cdc.[tatt4]
	,[tatt5] = cdc.[tatt5] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4008_cdc cdc
WHERE
    rep_jde.f4008_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4008_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4008_new AF:{{ task_instance_key_str }}' ) 
