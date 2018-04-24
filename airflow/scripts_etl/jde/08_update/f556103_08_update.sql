-- Apply updates from upsert table onto rep new table
UPDATE rep_jde.f556103_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[saitm] = cdc.[saitm]
	,[sactr] = cdc.[sactr]
	,[saflag] = cdc.[saflag]
	,[saflag_conv] = cdc.[saflag_conv]
	,[say55prhy] = cdc.[say55prhy]
	,[say55prhy_conv] = cdc.[say55prhy_conv]
	,[say55mcat] = cdc.[say55mcat]
	,[say55mcat_conv] = cdc.[say55mcat_conv]
	,[say55mnm] = cdc.[say55mnm]
	,[sagnnm] = cdc.[sagnnm]
	,[sagnnm_conv] = cdc.[sagnnm_conv]
	,[sanevo] = cdc.[sanevo]
	,[sanevo_conv] = cdc.[sanevo_conv]
	,[saprouom] = cdc.[saprouom]
	,[saliqv] = cdc.[saliqv]
	,[saliqv_conv] = cdc.[saliqv_conv]
	,[savolum] = cdc.[savolum]
	,[say55tcat] = cdc.[say55tcat]
	,[saactiveyn] = cdc.[saactiveyn]
	,[saactiveyn_conv] = cdc.[saactiveyn_conv]
	,[samkt3] = cdc.[samkt3]
	,[samkt3_conv] = cdc.[samkt3_conv]
	,[sapr016] = cdc.[sapr016]
	,[sapr016_conv] = cdc.[sapr016_conv]
	,[say55ilcl] = cdc.[say55ilcl]
	,[say55csif] = cdc.[say55csif]
	,[say55csif_conv] = cdc.[say55csif_conv]
	,[sagdep] = cdc.[sagdep]
	,[sagdep_conv] = cdc.[sagdep_conv]
	,[sagwid] = cdc.[sagwid]
	,[sagwid_conv] = cdc.[sagwid_conv]
	,[saghet] = cdc.[saghet]
	,[saghet_conv] = cdc.[saghet_conv]
	,[sadimuom] = cdc.[sadimuom]
	,[say55clu01] = cdc.[say55clu01]
	,[say55clu02] = cdc.[say55clu02]
	,[say55clu03] = cdc.[say55clu03]
	,[say55clu04] = cdc.[say55clu04]
	,[say55clu05] = cdc.[say55clu05]
	,[say55clu06] = cdc.[say55clu06]
	,[say55clu07] = cdc.[say55clu07]
	,[say55clu08] = cdc.[say55clu08]
	,[say55clu09] = cdc.[say55clu09]
	,[say55clu10] = cdc.[say55clu10]
	,[say55clu11] = cdc.[say55clu11]
	,[say55clu12] = cdc.[say55clu12]
	,[say55clu13] = cdc.[say55clu13]
	,[say55clu14] = cdc.[say55clu14]
	,[say55clu15] = cdc.[say55clu15]
	,[sapmfg] = cdc.[sapmfg]
	,[sapmfg_conv] = cdc.[sapmfg_conv]
	,[sadl01] = cdc.[sadl01]
	,[sadl01_conv] = cdc.[sadl01_conv]
	,[sadl02] = cdc.[sadl02]
	,[sadl02_conv] = cdc.[sadl02_conv]
	,[sadl03] = cdc.[sadl03]
	,[sadl03_conv] = cdc.[sadl03_conv]
	,[sagfl1] = cdc.[sagfl1]
	,[sagfl1_conv] = cdc.[sagfl1_conv]
	,[sagfl2] = cdc.[sagfl2]
	,[sagfl2_conv] = cdc.[sagfl2_conv]
	,[sagfl3] = cdc.[sagfl3]
	,[sagfl3_conv] = cdc.[sagfl3_conv]
	,[sagfl4] = cdc.[sagfl4]
	,[sagfl4_conv] = cdc.[sagfl4_conv]
	,[sagfl5] = cdc.[sagfl5]
	,[sagfl5_conv] = cdc.[sagfl5_conv]
	,[sadate01] = cdc.[sadate01]
	,[sadate01_conv] = cdc.[sadate01_conv]
	,[sadate02] = cdc.[sadate02]
	,[sadate02_conv] = cdc.[sadate02_conv]
	,[sadate03] = cdc.[sadate03]
	,[sadate03_conv] = cdc.[sadate03_conv]
	,[sadesc3] = cdc.[sadesc3]
	,[sadesc3_conv] = cdc.[sadesc3_conv]
	,[sadesc4] = cdc.[sadesc4]
	,[sadesc4_conv] = cdc.[sadesc4_conv]
	,[sadesc5] = cdc.[sadesc5]
	,[sadesc5_conv] = cdc.[sadesc5_conv]
	,[sadesc6] = cdc.[sadesc6]
	,[sadesc6_conv] = cdc.[sadesc6_conv]
	,[sadesc7] = cdc.[sadesc7]
	,[sadesc7_conv] = cdc.[sadesc7_conv]
	,[saaa] = cdc.[saaa]
	,[saaa_conv] = cdc.[saaa_conv]
	,[saaa1] = cdc.[saaa1]
	,[saaa1_conv] = cdc.[saaa1_conv]
	,[saaa2] = cdc.[saaa2]
	,[saaa2_conv] = cdc.[saaa2_conv]
	,[saaa3] = cdc.[saaa3]
	,[saaa3_conv] = cdc.[saaa3_conv]
	,[samath06] = cdc.[samath06]
	,[samath06_conv] = cdc.[samath06_conv]
	,[samath07] = cdc.[samath07]
	,[samath07_conv] = cdc.[samath07_conv]
	,[samath08] = cdc.[samath08]
	,[samath08_conv] = cdc.[samath08_conv]
	,[samath09] = cdc.[samath09]
	,[samath09_conv] = cdc.[samath09_conv]
	,[samath10] = cdc.[samath10]
	,[samath10_conv] = cdc.[samath10_conv]
	,[saurab] = cdc.[saurab]
	,[saurab_conv] = cdc.[saurab_conv]
	,[saurdt] = cdc.[saurdt]
	,[saurdt_conv] = cdc.[saurdt_conv]
	,[saurat] = cdc.[saurat]
	,[saurat_conv] = cdc.[saurat_conv]
	,[saurrf] = cdc.[saurrf]
	,[saurrf_conv] = cdc.[saurrf_conv]
	,[saurcd] = cdc.[saurcd]
	,[saurcd_conv] = cdc.[saurcd_conv]
	,[sauser] = cdc.[sauser]
	,[sauser_conv] = cdc.[sauser_conv]
	,[sapid] = cdc.[sapid]
	,[sapid_conv] = cdc.[sapid_conv]
	,[samkey] = cdc.[samkey]
	,[samkey_conv] = cdc.[samkey_conv]
	,[saupmj] = cdc.[saupmj]
	,[saupmj_conv] = cdc.[saupmj_conv]
	,[saupmt] = cdc.[saupmt]
	,[sacm01] = cdc.[sacm01]
	,[sacm01_conv] = cdc.[sacm01_conv]
	,[sacm02] = cdc.[sacm02]
	,[sacm02_conv] = cdc.[sacm02_conv]
	,[sacm03] = cdc.[sacm03]
	,[sacm03_conv] = cdc.[sacm03_conv]
	,[sacm04] = cdc.[sacm04]
	,[sacm04_conv] = cdc.[sacm04_conv]
	,[sacm05] = cdc.[sacm05]
	,[sacm05_conv] = cdc.[sacm05_conv]
	,[sadl05] = cdc.[sadl05]
	,[sadl05_conv] = cdc.[sadl05_conv]
	,[sadl06] = cdc.[sadl06]
	,[sadl06_conv] = cdc.[sadl06_conv]
	,[sadl07] = cdc.[sadl07]
	,[sadl07_conv] = cdc.[sadl07_conv]
	,[sadl08] = cdc.[sadl08]
	,[sadl08_conv] = cdc.[sadl08_conv]
	,[sadl09] = cdc.[sadl09]
	,[sadl09_conv] = cdc.[sadl09_conv]
	,[say55ascd] = cdc.[say55ascd]
	,[saev01] = cdc.[saev01]
	,[saev01_conv] = cdc.[saev01_conv]
	,[saev02] = cdc.[saev02]
	,[saev02_conv] = cdc.[saev02_conv]
	,[saev03] = cdc.[saev03]
	,[saev03_conv] = cdc.[saev03_conv]
	,[saev04] = cdc.[saev04]
	,[saev04_conv] = cdc.[saev04_conv]
	,[saev05] = cdc.[saev05]
	,[saev05_conv] = cdc.[saev05_conv]
	,[saua02] = cdc.[saua02]
	,[saua02_conv] = cdc.[saua02_conv]
	,[saua03] = cdc.[saua03]
	,[saua03_conv] = cdc.[saua03_conv]
	,[saua04] = cdc.[saua04]
	,[saua04_conv] = cdc.[saua04_conv]
	,[saua05] = cdc.[saua05]
	,[saua05_conv] = cdc.[saua05_conv]
	,[say55salbr] = cdc.[say55salbr]
	,[say55salbr_conv] = cdc.[say55salbr_conv]
	,[say55purbr] = cdc.[say55purbr]
	,[say55purbr_conv] = cdc.[say55purbr_conv]
	,[say55posrp] = cdc.[say55posrp]
	,[saky] = cdc.[saky]
	,[saky_conv] = cdc.[saky_conv]
	,[saev06] = cdc.[saev06]
	,[saev06_conv] = cdc.[saev06_conv]
	,[saev07] = cdc.[saev07]
	,[saev07_conv] = cdc.[saev07_conv]
	,[saev08] = cdc.[saev08]
	,[saev08_conv] = cdc.[saev08_conv]
	,[saev09] = cdc.[saev09]
	,[saev09_conv] = cdc.[saev09_conv]
	,[saev10] = cdc.[saev10]
	,[saev10_conv] = cdc.[saev10_conv]
	,[say55rctxt] = cdc.[say55rctxt]
	,[say55rctxt_conv] = cdc.[say55rctxt_conv]
	,[say55prchg] = cdc.[say55prchg]
	,[say55necon] = cdc.[say55necon]
	,[say55necon_conv] = cdc.[say55necon_conv]
	,[sancooc] = cdc.[sancooc]
	,[satax1] = cdc.[satax1]
	,[saaca1] = cdc.[saaca1]
	,[saaca1_conv] = cdc.[saaca1_conv]
	,[saaap1] = cdc.[saaap1]
	,[saaap1_conv] = cdc.[saaap1_conv]
	,[saaap2] = cdc.[saaap2]
	,[saaap2_conv] = cdc.[saaap2_conv]
	,[saaap3] = cdc.[saaap3]
	,[saaap3_conv] = cdc.[saaap3_conv]
	,[sak001] = cdc.[sak001]
	,[sak002] = cdc.[sak002]
	,[sak003] = cdc.[sak003]
	,[sak004] = cdc.[sak004]
	,[sadscrp] = cdc.[sadscrp]
	,[sadscrp_conv] = cdc.[sadscrp_conv]
	,[sadscrp1] = cdc.[sadscrp1]
	,[sadscrp1_conv] = cdc.[sadscrp1_conv]
	,[sadscrp2] = cdc.[sadscrp2]
	,[sadscrp2_conv] = cdc.[sadscrp2_conv]
	,[sadscrp3] = cdc.[sadscrp3]
	,[sadscrp3_conv] = cdc.[sadscrp3_conv]
	,[sadscrp4] = cdc.[sadscrp4]
	,[sadscrp4_conv] = cdc.[sadscrp4_conv]
	,[sadscrp5] = cdc.[sadscrp5]
	,[sadscrp5_conv] = cdc.[sadscrp5_conv]
	,[saba01] = cdc.[saba01]
	,[saba01_conv] = cdc.[saba01_conv]
	,[saba02] = cdc.[saba02]
	,[saba02_conv] = cdc.[saba02_conv]
	,[saba03] = cdc.[saba03]
	,[saba03_conv] = cdc.[saba03_conv]
	,[satxb] = cdc.[satxb]
	,[say55bycrc] = cdc.[say55bycrc]
	,[say55bycrc_conv] = cdc.[say55bycrc_conv]
	,[say55rewitm] = cdc.[say55rewitm]
	,[say55rewitm_conv] = cdc.[say55rewitm_conv]
	,[say55suppid] = cdc.[say55suppid]
	,[say55suppid_conv] = cdc.[say55suppid_conv]
	,[say55catlcod] = cdc.[say55catlcod]
	,[say55catlcod_conv] = cdc.[say55catlcod_conv]
	,[say55supcode] = cdc.[say55supcode]
	,[say55supcode_conv] = cdc.[say55supcode_conv]
	,[say55uprc] = cdc.[say55uprc]
	,[say55uprc_conv] = cdc.[say55uprc_conv]
	,[say55loydesc] = cdc.[say55loydesc]
	,[say55loydesc_conv] = cdc.[say55loydesc_conv]
	,[say55ccobom] = cdc.[say55ccobom]
	,[say55ccobom_conv] = cdc.[say55ccobom_conv]
	,[say55alcpc] = cdc.[say55alcpc]
	,[say55alcpc_conv] = cdc.[say55alcpc_conv]
	,[saagpr] = cdc.[saagpr]
	,[saagpr_conv] = cdc.[saagpr_conv]
	,[say55rnum1] = cdc.[say55rnum1]
	,[say55rnum1_conv] = cdc.[say55rnum1_conv]
	,[say55rnum2] = cdc.[say55rnum2]
	,[say55rnum2_conv] = cdc.[say55rnum2_conv]
	,[say55rnum3] = cdc.[say55rnum3]
	,[say55rnum3_conv] = cdc.[say55rnum3_conv]
	,[say55rnum4] = cdc.[say55rnum4]
	,[say55rnum4_conv] = cdc.[say55rnum4_conv]
	,[say55rnum5] = cdc.[say55rnum5]
	,[say55rnum5_conv] = cdc.[say55rnum5_conv]
	,[say55rdte1] = cdc.[say55rdte1]
	,[say55rdte1_conv] = cdc.[say55rdte1_conv]
	,[say55rtme1] = cdc.[say55rtme1]
	,[say55rtme1_conv] = cdc.[say55rtme1_conv]
	,[say55rdte2] = cdc.[say55rdte2]
	,[say55rdte2_conv] = cdc.[say55rdte2_conv]
	,[say55rtme2] = cdc.[say55rtme2]
	,[say55rtme2_conv] = cdc.[say55rtme2_conv]
	,[say55rdte3] = cdc.[say55rdte3]
	,[say55rdte3_conv] = cdc.[say55rdte3_conv]
	,[say55rtme3] = cdc.[say55rtme3]
	,[say55rtme3_conv] = cdc.[say55rtme3_conv]
	,[say55rchr1] = cdc.[say55rchr1]
	,[say55rchr1_conv] = cdc.[say55rchr1_conv]
	,[say55rchr2] = cdc.[say55rchr2]
	,[say55rchr2_conv] = cdc.[say55rchr2_conv]
	,[say55rchr3] = cdc.[say55rchr3]
	,[say55rchr3_conv] = cdc.[say55rchr3_conv]
	,[say55rstr1] = cdc.[say55rstr1]
	,[say55rstr1_conv] = cdc.[say55rstr1_conv]
	,[say55rstr2] = cdc.[say55rstr2]
	,[say55rstr2_conv] = cdc.[say55rstr2_conv]
	,[say55rstr3] = cdc.[say55rstr3]
	,[say55rstr3_conv] = cdc.[say55rstr3_conv]
	,[say55rstr4] = cdc.[say55rstr4]
	,[say55rstr4_conv] = cdc.[say55rstr4_conv]
	,[say55rstr5] = cdc.[say55rstr5]
	,[say55rstr5_conv] = cdc.[say55rstr5_conv] -- exclude distribution column
FROM stg_jde.tmp_upsert_f556103_cdc cdc
WHERE
    rep_jde.f556103_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde.f556103_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde.f556103_new AF:{{ task_instance_key_str }}' ) 
