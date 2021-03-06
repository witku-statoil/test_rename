-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f4074_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[aldoco] = cdc.[aldoco]
	,[aldcto] = cdc.[aldcto]
	,[alkcoo] = cdc.[alkcoo]
	,[alkcoo_conv] = cdc.[alkcoo_conv]
	,[alsfxo] = cdc.[alsfxo]
	,[alsfxo_conv] = cdc.[alsfxo_conv]
	,[allnid] = cdc.[allnid]
	,[allnid_conv] = cdc.[allnid_conv]
	,[alakid] = cdc.[alakid]
	,[alsrcfd] = cdc.[alsrcfd]
	,[aloseq] = cdc.[aloseq]
	,[aloseq_conv] = cdc.[aloseq_conv]
	,[alsubseq] = cdc.[alsubseq]
	,[alsubseq_conv] = cdc.[alsubseq_conv]
	,[altier] = cdc.[altier]
	,[alasn] = cdc.[alasn]
	,[alast] = cdc.[alast]
	,[alitm] = cdc.[alitm]
	,[alan8] = cdc.[alan8]
	,[alcrcd] = cdc.[alcrcd]
	,[alcrcd_conv] = cdc.[alcrcd_conv]
	,[aluom] = cdc.[aluom]
	,[almnq] = cdc.[almnq]
	,[almnq_conv] = cdc.[almnq_conv]
	,[alledg] = cdc.[alledg]
	,[alfrmn] = cdc.[alfrmn]
	,[albscd] = cdc.[albscd]
	,[alfvtr] = cdc.[alfvtr]
	,[alfvtr_conv] = cdc.[alfvtr_conv]
	,[alabas] = cdc.[alabas]
	,[alabas_conv] = cdc.[alabas_conv]
	,[aluprc] = cdc.[aluprc]
	,[aluprc_conv] = cdc.[aluprc_conv]
	,[alfup] = cdc.[alfup]
	,[alfup_conv] = cdc.[alfup_conv]
	,[alglc] = cdc.[alglc]
	,[alglc_conv] = cdc.[alglc_conv]
	,[alarsn] = cdc.[alarsn]
	,[alacnt] = cdc.[alacnt]
	,[alsbif] = cdc.[alsbif]
	,[almded] = cdc.[almded]
	,[almded_conv] = cdc.[almded_conv]
	,[alprov] = cdc.[alprov]
	,[alprov_conv] = cdc.[alprov_conv]
	,[alatid] = cdc.[alatid]
	,[aligid] = cdc.[aligid]
	,[aligid_conv] = cdc.[aligid_conv]
	,[alcgid] = cdc.[alcgid]
	,[alcgid_conv] = cdc.[alcgid_conv]
	,[alogid] = cdc.[alogid]
	,[alogid_conv] = cdc.[alogid_conv]
	,[alanps] = cdc.[alanps]
	,[alanps_conv] = cdc.[alanps_conv]
	,[albsdval] = cdc.[albsdval]
	,[albsdval_conv] = cdc.[albsdval_conv]
	,[alsrflag] = cdc.[alsrflag]
	,[alsrflag_conv] = cdc.[alsrflag_conv]
	,[aladjcal] = cdc.[aladjcal]
	,[aladjcal_conv] = cdc.[aladjcal_conv]
	,[alnbrord] = cdc.[alnbrord]
	,[alnbrord_conv] = cdc.[alnbrord_conv]
	,[aluomvid] = cdc.[aluomvid]
	,[alolvl] = cdc.[alolvl]
	,[aladjsts] = cdc.[aladjsts]
	,[aladjref] = cdc.[aladjref]
	,[aladjref_conv] = cdc.[aladjref_conv]
	,[alaccan8] = cdc.[alaccan8]
	,[albnad] = cdc.[albnad]
	,[aladjgrp] = cdc.[aladjgrp]
	,[almeadj] = cdc.[almeadj]
	,[almeadj_conv] = cdc.[almeadj_conv]
	,[alfvum] = cdc.[alfvum]
	,[alpdcl] = cdc.[alpdcl]
	,[alcfgid] = cdc.[alcfgid]
	,[alcfgid_conv] = cdc.[alcfgid_conv]
	,[alcfgcid] = cdc.[alcfgcid]
	,[alcfgcid_conv] = cdc.[alcfgcid_conv]
	,[alaprp1] = cdc.[alaprp1]
	,[alaprp2] = cdc.[alaprp2]
	,[alaprp3] = cdc.[alaprp3]
	,[alaprp4] = cdc.[alaprp4]
	,[alaprp5] = cdc.[alaprp5]
	,[alaprp6] = cdc.[alaprp6]
	,[alndpi] = cdc.[alndpi]
	,[alndpi_conv] = cdc.[alndpi_conv]
	,[aluser] = cdc.[aluser]
	,[aluser_conv] = cdc.[aluser_conv]
	,[alpid] = cdc.[alpid]
	,[alpid_conv] = cdc.[alpid_conv]
	,[aljobn] = cdc.[aljobn]
	,[aljobn_conv] = cdc.[aljobn_conv]
	,[alupmj] = cdc.[alupmj]
	,[alupmj_conv] = cdc.[alupmj_conv]
	,[altday] = cdc.[altday]
	,[alpmtn] = cdc.[alpmtn]
	,[alpmtn_conv] = cdc.[alpmtn_conv]
	,[alrulename] = cdc.[alrulename]
	,[alrulename_conv] = cdc.[alrulename_conv]
	,[alpa04] = cdc.[alpa04]
	,[aladjqty] = cdc.[aladjqty]
	,[aladjqty_conv] = cdc.[aladjqty_conv]
	,[alqtypy] = cdc.[alqtypy]
	,[alqtypy_conv] = cdc.[alqtypy_conv]
	,[alstprcf] = cdc.[alstprcf]
	,[alstprcf_conv] = cdc.[alstprcf_conv]
	,[altstrsnm] = cdc.[altstrsnm]
	,[altstrsnm_conv] = cdc.[altstrsnm_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f4074_cdc cdc
WHERE
    rep_jde.f4074_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f4074_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f4074_new AF:{{ task_instance_key_str }}' ) 
