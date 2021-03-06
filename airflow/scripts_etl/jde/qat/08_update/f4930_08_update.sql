-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f4930_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[vmvehi] = cdc.[vmvehi]
	,[vmvehi_conv] = cdc.[vmvehi_conv]
	,[vmvehs] = cdc.[vmvehs]
	,[vmvehs_conv] = cdc.[vmvehs_conv]
	,[vmdl01] = cdc.[vmdl01]
	,[vmdl01_conv] = cdc.[vmdl01_conv]
	,[vmlrfg] = cdc.[vmlrfg]
	,[vmlrfg_conv] = cdc.[vmlrfg_conv]
	,[vmmcu] = cdc.[vmmcu]
	,[vmmcu_conv] = cdc.[vmmcu_conv]
	,[vmvtyp] = cdc.[vmvtyp]
	,[vmvtyp_conv] = cdc.[vmvtyp_conv]
	,[vmdumv] = cdc.[vmdumv]
	,[vmdumv_conv] = cdc.[vmdumv_conv]
	,[vmvown] = cdc.[vmvown]
	,[vmnce] = cdc.[vmnce]
	,[vmnce_conv] = cdc.[vmnce_conv]
	,[vmwtem] = cdc.[vmwtem]
	,[vmwtem_conv] = cdc.[vmwtem_conv]
	,[vmwtca] = cdc.[vmwtca]
	,[vmwtca_conv] = cdc.[vmwtca_conv]
	,[vmwtum] = cdc.[vmwtum]
	,[vmvlcp] = cdc.[vmvlcp]
	,[vmvlcp_conv] = cdc.[vmvlcp_conv]
	,[vmvlcs] = cdc.[vmvlcs]
	,[vmvlcs_conv] = cdc.[vmvlcs_conv]
	,[vmvlum] = cdc.[vmvlum]
	,[vmcvol] = cdc.[vmcvol]
	,[vmcvol_conv] = cdc.[vmcvol_conv]
	,[vmcvum] = cdc.[vmcvum]
	,[vmlcnt] = cdc.[vmlcnt]
	,[vmmxml] = cdc.[vmmxml]
	,[vmumd1] = cdc.[vmumd1]
	,[vminmg] = cdc.[vminmg]
	,[vmmest] = cdc.[vmmest]
	,[vmvehn] = cdc.[vmvehn]
	,[vmvehn_conv] = cdc.[vmvehn_conv]
	,[vmurcd] = cdc.[vmurcd]
	,[vmurcd_conv] = cdc.[vmurcd_conv]
	,[vmurdt] = cdc.[vmurdt]
	,[vmurdt_conv] = cdc.[vmurdt_conv]
	,[vmurat] = cdc.[vmurat]
	,[vmurat_conv] = cdc.[vmurat_conv]
	,[vmurab] = cdc.[vmurab]
	,[vmurab_conv] = cdc.[vmurab_conv]
	,[vmurrf] = cdc.[vmurrf]
	,[vmurrf_conv] = cdc.[vmurrf_conv]
	,[vmuser] = cdc.[vmuser]
	,[vmuser_conv] = cdc.[vmuser_conv]
	,[vmpid] = cdc.[vmpid]
	,[vmpid_conv] = cdc.[vmpid_conv]
	,[vmjobn] = cdc.[vmjobn]
	,[vmjobn_conv] = cdc.[vmjobn_conv]
	,[vmupmj] = cdc.[vmupmj]
	,[vmupmj_conv] = cdc.[vmupmj_conv]
	,[vmtday] = cdc.[vmtday] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f4930_cdc cdc
WHERE
    rep_jde_qat.f4930_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f4930_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f4930_new AF:{{ task_instance_key_str }}' ) 
