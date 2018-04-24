-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f554501_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[qmukid] = cdc.[qmukid]
	,[qmy55qn] = cdc.[qmy55qn]
	,[qmy55qn_conv] = cdc.[qmy55qn_conv]
	,[qmdsc1] = cdc.[qmdsc1]
	,[qmdsc1_conv] = cdc.[qmdsc1_conv]
	,[qmy55qs] = cdc.[qmy55qs]
	,[qmy55qt] = cdc.[qmy55qt]
	,[qmy55jdqn] = cdc.[qmy55jdqn]
	,[qmy55jdqn_conv] = cdc.[qmy55jdqn_conv]
	,[qmdsc2] = cdc.[qmdsc2]
	,[qmdsc2_conv] = cdc.[qmdsc2_conv]
	,[qmcrcd] = cdc.[qmcrcd]
	,[qmcrcd_conv] = cdc.[qmcrcd_conv]
	,[qmuom] = cdc.[qmuom]
	,[qmdend] = cdc.[qmdend]
	,[qmdend_conv] = cdc.[qmdend_conv]
	,[qmdntp] = cdc.[qmdntp]
	,[qmdte] = cdc.[qmdte]
	,[qmdte_conv] = cdc.[qmdte_conv]
	,[qmy55avin] = cdc.[qmy55avin]
	,[qmco] = cdc.[qmco]
	,[qmco_conv] = cdc.[qmco_conv]
	,[qmmcu] = cdc.[qmmcu]
	,[qmmcu_conv] = cdc.[qmmcu_conv]
	,[qmprp4] = cdc.[qmprp4]
	,[qmcomments] = cdc.[qmcomments]
	,[qmcomments_conv] = cdc.[qmcomments_conv]
	,[qmflag] = cdc.[qmflag]
	,[qmflag_conv] = cdc.[qmflag_conv]
	,[qmurrf] = cdc.[qmurrf]
	,[qmurrf_conv] = cdc.[qmurrf_conv]
	,[qmurcd] = cdc.[qmurcd]
	,[qmurcd_conv] = cdc.[qmurcd_conv]
	,[qmurab] = cdc.[qmurab]
	,[qmurab_conv] = cdc.[qmurab_conv]
	,[qmurat] = cdc.[qmurat]
	,[qmurat_conv] = cdc.[qmurat_conv]
	,[qmurdt] = cdc.[qmurdt]
	,[qmurdt_conv] = cdc.[qmurdt_conv]
	,[qmuser] = cdc.[qmuser]
	,[qmuser_conv] = cdc.[qmuser_conv]
	,[qmpid] = cdc.[qmpid]
	,[qmpid_conv] = cdc.[qmpid_conv]
	,[qmjobn] = cdc.[qmjobn]
	,[qmjobn_conv] = cdc.[qmjobn_conv]
	,[qmupmt] = cdc.[qmupmt]
	,[qmupmj] = cdc.[qmupmj]
	,[qmupmj_conv] = cdc.[qmupmj_conv]
	,[qmev01] = cdc.[qmev01]
	,[qmev01_conv] = cdc.[qmev01_conv]
	,[qmev02] = cdc.[qmev02]
	,[qmev02_conv] = cdc.[qmev02_conv]
	,[qmev03] = cdc.[qmev03]
	,[qmev03_conv] = cdc.[qmev03_conv]
	,[qmev04] = cdc.[qmev04]
	,[qmev04_conv] = cdc.[qmev04_conv]
	,[qmy55char1] = cdc.[qmy55char1]
	,[qmy55char1_conv] = cdc.[qmy55char1_conv]
	,[qmy55char2] = cdc.[qmy55char2]
	,[qmy55char2_conv] = cdc.[qmy55char2_conv]
	,[qmy55date1] = cdc.[qmy55date1]
	,[qmy55date1_conv] = cdc.[qmy55date1_conv]
	,[qmy55date2] = cdc.[qmy55date2]
	,[qmy55date2_conv] = cdc.[qmy55date2_conv]
	,[qmy55strg1] = cdc.[qmy55strg1]
	,[qmy55strg1_conv] = cdc.[qmy55strg1_conv]
	,[qmy55strg2] = cdc.[qmy55strg2]
	,[qmy55strg2_conv] = cdc.[qmy55strg2_conv]
	,[qmy55time1] = cdc.[qmy55time1]
	,[qmy55time2] = cdc.[qmy55time2]
	,[qmy55amnt1] = cdc.[qmy55amnt1]
	,[qmy55amnt1_conv] = cdc.[qmy55amnt1_conv]
	,[qmy55amnt2] = cdc.[qmy55amnt2]
	,[qmy55amnt2_conv] = cdc.[qmy55amnt2_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f554501_cdc cdc
WHERE
    rep_jde_qat.f554501_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f554501_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f554501_new AF:{{ task_instance_key_str }}' ) 