-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f550005_new
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
	,cmy55cmpcd
	,cmcmpid
	,cmcmpid_conv
	,cmy55cmpty
	,cmtxky
	,cmtxky_conv
	,cmsdca
	,cmsdca_conv
	,cmedca
	,cmedca_conv
	,cmctr
	,cmy55theme
	,cmcrcd
	,cmcrcd_conv
	,cmexdt0
	,cmexdt0_conv
	,cmexdt1
	,cmexdt1_conv
	,cmurab
	,cmurab_conv
	,cmurrf
	,cmurrf_conv
	,cmurdt
	,cmurdt_conv
	,cmurat
	,cmurat_conv
	,cmurcd
	,cmurcd_conv
	,cmcrtu
	,cmcrtu_conv
	,cmcrdj
	,cmcrdj_conv
	,cmc9crp
	,cmc9crp_conv
	,cmupmj
	,cmupmj_conv
	,cmupmt
	,cmjobn
	,cmjobn_conv
	,cmpid
	,cmpid_conv
	,cmuser
	,cmuser_conv
	,cmy55lckdt
	,cmy55lckdt_conv
	,cmy55alapr
	,cmy55alapr_conv
	,cmy55strg1
	,cmy55strg1_conv
	,cmy55strg2
	,cmy55strg2_conv
	,cmy55amnt1
	,cmy55amnt1_conv
	,cmy55amnt2
	,cmy55amnt2_conv
	,cmy55char1
	,cmy55char1_conv
	,cmy55char2
	,cmy55char2_conv
	,cmy55date1
	,cmy55date1_conv
	,cmy55date2
	,cmy55date2_conv
	,cmy55time1
	,cmy55time2
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
	,cdc.cmy55cmpcd
	,cdc.cmcmpid
	,cdc.cmcmpid_conv
	,cdc.cmy55cmpty
	,cdc.cmtxky
	,cdc.cmtxky_conv
	,cdc.cmsdca
	,cdc.cmsdca_conv
	,cdc.cmedca
	,cdc.cmedca_conv
	,cdc.cmctr
	,cdc.cmy55theme
	,cdc.cmcrcd
	,cdc.cmcrcd_conv
	,cdc.cmexdt0
	,cdc.cmexdt0_conv
	,cdc.cmexdt1
	,cdc.cmexdt1_conv
	,cdc.cmurab
	,cdc.cmurab_conv
	,cdc.cmurrf
	,cdc.cmurrf_conv
	,cdc.cmurdt
	,cdc.cmurdt_conv
	,cdc.cmurat
	,cdc.cmurat_conv
	,cdc.cmurcd
	,cdc.cmurcd_conv
	,cdc.cmcrtu
	,cdc.cmcrtu_conv
	,cdc.cmcrdj
	,cdc.cmcrdj_conv
	,cdc.cmc9crp
	,cdc.cmc9crp_conv
	,cdc.cmupmj
	,cdc.cmupmj_conv
	,cdc.cmupmt
	,cdc.cmjobn
	,cdc.cmjobn_conv
	,cdc.cmpid
	,cdc.cmpid_conv
	,cdc.cmuser
	,cdc.cmuser_conv
	,cdc.cmy55lckdt
	,cdc.cmy55lckdt_conv
	,cdc.cmy55alapr
	,cdc.cmy55alapr_conv
	,cdc.cmy55strg1
	,cdc.cmy55strg1_conv
	,cdc.cmy55strg2
	,cdc.cmy55strg2_conv
	,cdc.cmy55amnt1
	,cdc.cmy55amnt1_conv
	,cdc.cmy55amnt2
	,cdc.cmy55amnt2_conv
	,cdc.cmy55char1
	,cdc.cmy55char1_conv
	,cdc.cmy55char2
	,cdc.cmy55char2_conv
	,cdc.cmy55date1
	,cdc.cmy55date1_conv
	,cdc.cmy55date2
	,cdc.cmy55date2_conv
	,cdc.cmy55time1
	,cdc.cmy55time2
FROM stg_jde.tmp_upsert_f550005_cdc cdc
LEFT OUTER JOIN rep_jde.f550005_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f550005_new AF:{{ task_instance_key_str }}' ) 