-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde_qat.f554506_new
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
	,ismcu
	,ismcu_conv
	,isitm
	,isy55qdte
	,isy55qdte_conv
	,istme0
	,islitm
	,islitm_conv
	,isaitm
	,isaitm_conv
	,isy55qn
	,isy55qn_conv
	,isy55jdqn
	,isy55jdqn_conv
	,isy55qt
	,isuom1
	,isuncs
	,isuncs_conv
	,iscrcd
	,iscrcd_conv
	,isy55usg
	,isy55usg_conv
	,isy55crcd1
	,isy55crcd1_conv
	,isy55uom1
	,isy55lsg
	,isy55lsg_conv
	,isy55crcd2
	,isy55crcd2_conv
	,isy55uom2
	,isy55acs
	,isy55acs_conv
	,isy55crcd3
	,isy55crcd3_conv
	,isy55uom3
	,isy55trc
	,isy55trc_conv
	,isy55crcd4
	,isy55crcd4_conv
	,isy55uom4
	,isy55esg
	,isy55esg_conv
	,isy55crcd6
	,isy55crcd6_conv
	,isy55uom6
	,isy55disc
	,isy55disc_conv
	,isy55osl
	,isy55osl_conv
	,isy55tcst
	,isy55tcst_conv
	,isy55crcd5
	,isy55crcd5_conv
	,isy55uom5
	,isy55avin
	,isdte
	,isdte_conv
	,isflag
	,isflag_conv
	,isev01
	,isev01_conv
	,isev02
	,isev02_conv
	,isev03
	,isev03_conv
	,isev04
	,isev04_conv
	,isev05
	,isev05_conv
	,isurrf
	,isurrf_conv
	,isurcd
	,isurcd_conv
	,isurab
	,isurab_conv
	,isurat
	,isurat_conv
	,isurdt
	,isurdt_conv
	,isuser
	,isuser_conv
	,ispid
	,ispid_conv
	,isjobn
	,isjobn_conv
	,isupmt
	,isupmj
	,isupmj_conv
	,isy55char1
	,isy55char1_conv
	,isy55char2
	,isy55char2_conv
	,isy55date1
	,isy55date1_conv
	,isy55date2
	,isy55date2_conv
	,isy55strg1
	,isy55strg1_conv
	,isy55strg2
	,isy55strg2_conv
	,isy55time1
	,isy55time2
	,isy55amnt1
	,isy55amnt1_conv
	,isy55amnt2
	,isy55amnt2_conv
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
	,cdc.ismcu
	,cdc.ismcu_conv
	,cdc.isitm
	,cdc.isy55qdte
	,cdc.isy55qdte_conv
	,cdc.istme0
	,cdc.islitm
	,cdc.islitm_conv
	,cdc.isaitm
	,cdc.isaitm_conv
	,cdc.isy55qn
	,cdc.isy55qn_conv
	,cdc.isy55jdqn
	,cdc.isy55jdqn_conv
	,cdc.isy55qt
	,cdc.isuom1
	,cdc.isuncs
	,cdc.isuncs_conv
	,cdc.iscrcd
	,cdc.iscrcd_conv
	,cdc.isy55usg
	,cdc.isy55usg_conv
	,cdc.isy55crcd1
	,cdc.isy55crcd1_conv
	,cdc.isy55uom1
	,cdc.isy55lsg
	,cdc.isy55lsg_conv
	,cdc.isy55crcd2
	,cdc.isy55crcd2_conv
	,cdc.isy55uom2
	,cdc.isy55acs
	,cdc.isy55acs_conv
	,cdc.isy55crcd3
	,cdc.isy55crcd3_conv
	,cdc.isy55uom3
	,cdc.isy55trc
	,cdc.isy55trc_conv
	,cdc.isy55crcd4
	,cdc.isy55crcd4_conv
	,cdc.isy55uom4
	,cdc.isy55esg
	,cdc.isy55esg_conv
	,cdc.isy55crcd6
	,cdc.isy55crcd6_conv
	,cdc.isy55uom6
	,cdc.isy55disc
	,cdc.isy55disc_conv
	,cdc.isy55osl
	,cdc.isy55osl_conv
	,cdc.isy55tcst
	,cdc.isy55tcst_conv
	,cdc.isy55crcd5
	,cdc.isy55crcd5_conv
	,cdc.isy55uom5
	,cdc.isy55avin
	,cdc.isdte
	,cdc.isdte_conv
	,cdc.isflag
	,cdc.isflag_conv
	,cdc.isev01
	,cdc.isev01_conv
	,cdc.isev02
	,cdc.isev02_conv
	,cdc.isev03
	,cdc.isev03_conv
	,cdc.isev04
	,cdc.isev04_conv
	,cdc.isev05
	,cdc.isev05_conv
	,cdc.isurrf
	,cdc.isurrf_conv
	,cdc.isurcd
	,cdc.isurcd_conv
	,cdc.isurab
	,cdc.isurab_conv
	,cdc.isurat
	,cdc.isurat_conv
	,cdc.isurdt
	,cdc.isurdt_conv
	,cdc.isuser
	,cdc.isuser_conv
	,cdc.ispid
	,cdc.ispid_conv
	,cdc.isjobn
	,cdc.isjobn_conv
	,cdc.isupmt
	,cdc.isupmj
	,cdc.isupmj_conv
	,cdc.isy55char1
	,cdc.isy55char1_conv
	,cdc.isy55char2
	,cdc.isy55char2_conv
	,cdc.isy55date1
	,cdc.isy55date1_conv
	,cdc.isy55date2
	,cdc.isy55date2_conv
	,cdc.isy55strg1
	,cdc.isy55strg1_conv
	,cdc.isy55strg2
	,cdc.isy55strg2_conv
	,cdc.isy55time1
	,cdc.isy55time2
	,cdc.isy55amnt1
	,cdc.isy55amnt1_conv
	,cdc.isy55amnt2
	,cdc.isy55amnt2_conv
FROM stg_jde_qat.tmp_upsert_f554506_cdc cdc
LEFT OUTER JOIN rep_jde_qat.f554506_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde_qat.f554506_new AF:{{ task_instance_key_str }}' ) 