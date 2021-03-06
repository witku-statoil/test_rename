-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4945_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[scshpn] = cdc.[scshpn]
	,[scrssn] = cdc.[scrssn]
	,[scvmcu] = cdc.[scvmcu]
	,[scvmcu_conv] = cdc.[scvmcu_conv]
	,[scldnm] = cdc.[scldnm]
	,[scdlno] = cdc.[scdlno]
	,[scoseq] = cdc.[scoseq]
	,[scoseq_conv] = cdc.[scoseq_conv]
	,[scrtnm] = cdc.[scrtnm]
	,[scsdsq] = cdc.[scsdsq]
	,[scsdsq_conv] = cdc.[scsdsq_conv]
	,[scscsn] = cdc.[scscsn]
	,[scscsn_conv] = cdc.[scscsn_conv]
	,[scnmfc] = cdc.[scnmfc]
	,[scdsgp] = cdc.[scdsgp]
	,[scfrt1] = cdc.[scfrt1]
	,[scfrt2] = cdc.[scfrt2]
	,[sccgc1] = cdc.[sccgc1]
	,[scag] = cdc.[scag]
	,[scag_conv] = cdc.[scag_conv]
	,[scblpb] = cdc.[scblpb]
	,[scblpb_conv] = cdc.[scblpb_conv]
	,[sccrdc] = cdc.[sccrdc]
	,[sccrdc_conv] = cdc.[sccrdc_conv]
	,[scfaa] = cdc.[scfaa]
	,[scfaa_conv] = cdc.[scfaa_conv]
	,[scnamf] = cdc.[scnamf]
	,[scnamf_conv] = cdc.[scnamf_conv]
	,[scrtdq] = cdc.[scrtdq]
	,[scrtdq_conv] = cdc.[scrtdq_conv]
	,[scnamt] = cdc.[scnamt]
	,[scnamt_conv] = cdc.[scnamt_conv]
	,[scuom] = cdc.[scuom]
	,[scrtgb] = cdc.[scrtgb]
	,[sccrcd] = cdc.[sccrcd]
	,[sccrcd_conv] = cdc.[sccrcd_conv]
	,[scdoco] = cdc.[scdoco]
	,[scdcto] = cdc.[scdcto]
	,[sckcoo] = cdc.[sckcoo]
	,[sckcoo_conv] = cdc.[sckcoo_conv]
	,[sclnid] = cdc.[sclnid]
	,[sclnid_conv] = cdc.[sclnid_conv]
	,[scovfg] = cdc.[scovfg]
	,[scukid] = cdc.[scukid]
	,[scuk01] = cdc.[scuk01]
	,[scurcd] = cdc.[scurcd]
	,[scurcd_conv] = cdc.[scurcd_conv]
	,[scurdt] = cdc.[scurdt]
	,[scurdt_conv] = cdc.[scurdt_conv]
	,[scurat] = cdc.[scurat]
	,[scurat_conv] = cdc.[scurat_conv]
	,[scurab] = cdc.[scurab]
	,[scurab_conv] = cdc.[scurab_conv]
	,[scurrf] = cdc.[scurrf]
	,[scurrf_conv] = cdc.[scurrf_conv]
	,[scuser] = cdc.[scuser]
	,[scuser_conv] = cdc.[scuser_conv]
	,[scpid] = cdc.[scpid]
	,[scpid_conv] = cdc.[scpid_conv]
	,[scjobn] = cdc.[scjobn]
	,[scjobn_conv] = cdc.[scjobn_conv]
	,[scupmj] = cdc.[scupmj]
	,[scupmj_conv] = cdc.[scupmj_conv]
	,[sctday] = cdc.[sctday]
	,[scrtn] = cdc.[scrtn]
	,[sclnmb] = cdc.[sclnmb]
	,[scprte] = cdc.[scprte]
	,[scprte_conv] = cdc.[scprte_conv]
	,[sceftj] = cdc.[sceftj]
	,[sceftj_conv] = cdc.[sceftj_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4945_cdc cdc
WHERE
    rep_jde.f4945_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4945_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4945_new AF:{{ task_instance_key_str }}' ) 
