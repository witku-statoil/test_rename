-- Apply updates from upsert table onto rep new table
UPDATE rep_jde_qat.f1217_new
SET
	[sys_file_name] = cdc.[sys_file_name]
	,[sys_file_ln] = cdc.[sys_file_ln]
	,[sys_extract_dt] = cdc.[sys_extract_dt]
	,[sys_cdc_dt] = cdc.[sys_cdc_dt]
	,[sys_cdc_scn] = cdc.[sys_cdc_scn]
	,[sys_cdc_operation_type] = cdc.[sys_cdc_operation_type]
	,[sys_cdc_before_after] = cdc.[sys_cdc_before_after]
	,[sys_line_modified_ind] = cdc.[sys_line_modified_ind]
	,[wrnumb] = cdc.[wrnumb]
	,[wrctr] = cdc.[wrctr]
	,[wrcoown] = cdc.[wrcoown]
	,[wrcoown_conv] = cdc.[wrcoown_conv]
	,[wrmmcu] = cdc.[wrmmcu]
	,[wrmmcu_conv] = cdc.[wrmmcu_conv]
	,[wrdoco] = cdc.[wrdoco]
	,[wrdcto] = cdc.[wrdcto]
	,[wrkcoo] = cdc.[wrkcoo]
	,[wrkcoo_conv] = cdc.[wrkcoo_conv]
	,[wrlnid] = cdc.[wrlnid]
	,[wrlnid_conv] = cdc.[wrlnid_conv]
	,[wrordj] = cdc.[wrordj]
	,[wrordj_conv] = cdc.[wrordj_conv]
	,[wrshpj] = cdc.[wrshpj]
	,[wrshpj_conv] = cdc.[wrshpj_conv]
	,[wraddj] = cdc.[wraddj]
	,[wraddj_conv] = cdc.[wraddj_conv]
	,[wrshan] = cdc.[wrshan]
	,[wrpa8] = cdc.[wrpa8]
	,[wrslsm] = cdc.[wrslsm]
	,[wranob] = cdc.[wranob]
	,[wrrorn] = cdc.[wrrorn]
	,[wrrorn_conv] = cdc.[wrrorn_conv]
	,[wrrcto] = cdc.[wrrcto]
	,[wrstrx] = cdc.[wrstrx]
	,[wrstrx_conv] = cdc.[wrstrx_conv]
	,[wrvend] = cdc.[wrvend]
	,[wrvend_conv] = cdc.[wrvend_conv]
	,[wroorn] = cdc.[wroorn]
	,[wroorn_conv] = cdc.[wroorn_conv]
	,[wrocto] = cdc.[wrocto]
	,[wrokco] = cdc.[wrokco]
	,[wrokco_conv] = cdc.[wrokco_conv]
	,[wrsfxo] = cdc.[wrsfxo]
	,[wrsfxo_conv] = cdc.[wrsfxo_conv]
	,[wrogno] = cdc.[wrogno]
	,[wrogno_conv] = cdc.[wrogno_conv]
	,[wrprodf] = cdc.[wrprodf]
	,[wrprodm] = cdc.[wrprodm]
	,[wrprodc] = cdc.[wrprodc]
	,[wrcmod] = cdc.[wrcmod]
	,[wrcmod_conv] = cdc.[wrcmod_conv]
	,[wrsyem] = cdc.[wrsyem]
	,[wrsyem_conv] = cdc.[wrsyem_conv]
	,[wrvinnu] = cdc.[wrvinnu]
	,[wrvinnu_conv] = cdc.[wrvinnu_conv]
	,[wrrefn] = cdc.[wrrefn]
	,[wrrefn_conv] = cdc.[wrrefn_conv]
	,[wrze01] = cdc.[wrze01]
	,[wrze02] = cdc.[wrze02]
	,[wrze03] = cdc.[wrze03]
	,[wrze04] = cdc.[wrze04]
	,[wrze05] = cdc.[wrze05]
	,[wrze06] = cdc.[wrze06]
	,[wrze07] = cdc.[wrze07]
	,[wrze08] = cdc.[wrze08]
	,[wrze09] = cdc.[wrze09]
	,[wrze10] = cdc.[wrze10]
	,[wrurcd] = cdc.[wrurcd]
	,[wrurcd_conv] = cdc.[wrurcd_conv]
	,[wrurdt] = cdc.[wrurdt]
	,[wrurdt_conv] = cdc.[wrurdt_conv]
	,[wrurat] = cdc.[wrurat]
	,[wrurat_conv] = cdc.[wrurat_conv]
	,[wrurab] = cdc.[wrurab]
	,[wrurab_conv] = cdc.[wrurab_conv]
	,[wrurrf] = cdc.[wrurrf]
	,[wrurrf_conv] = cdc.[wrurrf_conv]
	,[wrcrtu] = cdc.[wrcrtu]
	,[wrcrtu_conv] = cdc.[wrcrtu_conv]
	,[wruser] = cdc.[wruser]
	,[wruser_conv] = cdc.[wruser_conv]
	,[wrpid] = cdc.[wrpid]
	,[wrpid_conv] = cdc.[wrpid_conv]
	,[wrjobn] = cdc.[wrjobn]
	,[wrjobn_conv] = cdc.[wrjobn_conv]
	,[wrupmj] = cdc.[wrupmj]
	,[wrupmj_conv] = cdc.[wrupmj_conv]
	,[wrupmt] = cdc.[wrupmt]
	,[wrlotn] = cdc.[wrlotn]
	,[wrlotn_conv] = cdc.[wrlotn_conv]
	,[wrlocn] = cdc.[wrlocn]
	,[wrlocn_conv] = cdc.[wrlocn_conv]
	,[wrwod] = cdc.[wrwod]
	,[wrbrev] = cdc.[wrbrev]
	,[wrbrev_conv] = cdc.[wrbrev_conv]
	,[wrefff] = cdc.[wrefff]
	,[wrefff_conv] = cdc.[wrefff_conv]
	,[wran8dl] = cdc.[wran8dl]
	,[wran8as] = cdc.[wran8as]
	,[wran8as_conv] = cdc.[wran8as_conv]
	,[wrtermyn] = cdc.[wrtermyn]
	,[wrtermyn_conv] = cdc.[wrtermyn_conv]
	,[wrsatyp] = cdc.[wrsatyp]
	,[wrinsdte] = cdc.[wrinsdte]
	,[wrinsdte_conv] = cdc.[wrinsdte_conv]
	,[wrmcu] = cdc.[wrmcu]
	,[wrmcu_conv] = cdc.[wrmcu_conv]
	,[wrmrryn] = cdc.[wrmrryn]
	,[wrmrryn_conv] = cdc.[wrmrryn_conv]
	,[wrregsts] = cdc.[wrregsts]
	,[wrvmrs31] = cdc.[wrvmrs31]
	,[wrvmrs32] = cdc.[wrvmrs32]
	,[wrvmrs34] = cdc.[wrvmrs34]
	,[wrvmrs34_conv] = cdc.[wrvmrs34_conv]
	,[wran8dr] = cdc.[wran8dr]
	,[wran8dr_conv] = cdc.[wran8dr_conv]
	,[wreqpn] = cdc.[wreqpn]
	,[wreqpn_conv] = cdc.[wreqpn_conv]
	,[wrmtryn] = cdc.[wrmtryn]
	,[wrmtryn_conv] = cdc.[wrmtryn_conv] -- exclude distribution column
FROM stg_jde_qat.tmp_upsert_f1217_cdc cdc
WHERE
    rep_jde_qat.f1217_new.sys_integration_id = cdc.sys_integration_id
    AND rep_jde_qat.f1217_new.sys_cdc_scn < cdc.sys_cdc_scn 
OPTION ( LABEL = 'UPDATE_rep_jde_qat.f1217_new AF:{{ task_instance_key_str }}' ) 
