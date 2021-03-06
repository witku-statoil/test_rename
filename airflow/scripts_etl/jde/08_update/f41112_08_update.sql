-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f41112_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[indct] = cdc.[indct]
	,[infy] = cdc.[infy]
	,[infy_conv] = cdc.[infy_conv]
	,[inctry] = cdc.[inctry]
	,[initm] = cdc.[initm]
	,[inlitm] = cdc.[inlitm]
	,[inlitm_conv] = cdc.[inlitm_conv]
	,[inaitm] = cdc.[inaitm]
	,[inaitm_conv] = cdc.[inaitm_conv]
	,[inmcu] = cdc.[inmcu]
	,[inmcu_conv] = cdc.[inmcu_conv]
	,[inlocn] = cdc.[inlocn]
	,[inlocn_conv] = cdc.[inlocn_conv]
	,[inlotn] = cdc.[inlotn]
	,[inlotn_conv] = cdc.[inlotn_conv]
	,[inglpt] = cdc.[inglpt]
	,[inan8] = cdc.[inan8]
	,[inshan] = cdc.[inshan]
	,[innq01] = cdc.[innq01]
	,[innq01_conv] = cdc.[innq01_conv]
	,[innq02] = cdc.[innq02]
	,[innq02_conv] = cdc.[innq02_conv]
	,[innq03] = cdc.[innq03]
	,[innq03_conv] = cdc.[innq03_conv]
	,[innq04] = cdc.[innq04]
	,[innq04_conv] = cdc.[innq04_conv]
	,[innq05] = cdc.[innq05]
	,[innq05_conv] = cdc.[innq05_conv]
	,[innq06] = cdc.[innq06]
	,[innq06_conv] = cdc.[innq06_conv]
	,[innq07] = cdc.[innq07]
	,[innq07_conv] = cdc.[innq07_conv]
	,[innq08] = cdc.[innq08]
	,[innq08_conv] = cdc.[innq08_conv]
	,[innq09] = cdc.[innq09]
	,[innq09_conv] = cdc.[innq09_conv]
	,[innq10] = cdc.[innq10]
	,[innq10_conv] = cdc.[innq10_conv]
	,[innq11] = cdc.[innq11]
	,[innq11_conv] = cdc.[innq11_conv]
	,[innq12] = cdc.[innq12]
	,[innq12_conv] = cdc.[innq12_conv]
	,[innq13] = cdc.[innq13]
	,[innq13_conv] = cdc.[innq13_conv]
	,[innq14] = cdc.[innq14]
	,[innq14_conv] = cdc.[innq14_conv]
	,[inan01] = cdc.[inan01]
	,[inan01_conv] = cdc.[inan01_conv]
	,[inan02] = cdc.[inan02]
	,[inan02_conv] = cdc.[inan02_conv]
	,[inan03] = cdc.[inan03]
	,[inan03_conv] = cdc.[inan03_conv]
	,[inan04] = cdc.[inan04]
	,[inan04_conv] = cdc.[inan04_conv]
	,[inan05] = cdc.[inan05]
	,[inan05_conv] = cdc.[inan05_conv]
	,[inan06] = cdc.[inan06]
	,[inan06_conv] = cdc.[inan06_conv]
	,[inan07] = cdc.[inan07]
	,[inan07_conv] = cdc.[inan07_conv]
	,[inan08] = cdc.[inan08]
	,[inan08_conv] = cdc.[inan08_conv]
	,[inan09] = cdc.[inan09]
	,[inan09_conv] = cdc.[inan09_conv]
	,[inan10] = cdc.[inan10]
	,[inan10_conv] = cdc.[inan10_conv]
	,[inan11] = cdc.[inan11]
	,[inan11_conv] = cdc.[inan11_conv]
	,[inan12] = cdc.[inan12]
	,[inan12_conv] = cdc.[inan12_conv]
	,[inan13] = cdc.[inan13]
	,[inan13_conv] = cdc.[inan13_conv]
	,[inan14] = cdc.[inan14]
	,[inan14_conv] = cdc.[inan14_conv]
	,[incuma] = cdc.[incuma]
	,[incuma_conv] = cdc.[incuma_conv]
	,[incmqt] = cdc.[incmqt]
	,[incmqt_conv] = cdc.[incmqt_conv]
	,[inaisl] = cdc.[inaisl]
	,[inaisl_conv] = cdc.[inaisl_conv]
	,[inbin] = cdc.[inbin]
	,[inbin_conv] = cdc.[inbin_conv]
	,[intday] = cdc.[intday]
	,[inuser] = cdc.[inuser]
	,[inuser_conv] = cdc.[inuser_conv]
	,[inpid] = cdc.[inpid]
	,[inpid_conv] = cdc.[inpid_conv]
	,[inupmj] = cdc.[inupmj]
	,[inupmj_conv] = cdc.[inupmj_conv]
	,[injobn] = cdc.[injobn]
	,[injobn_conv] = cdc.[injobn_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f41112_cdc cdc
WHERE
    rep_jde.f41112_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f41112_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f41112_new AF:{{ task_instance_key_str }}' ) 
