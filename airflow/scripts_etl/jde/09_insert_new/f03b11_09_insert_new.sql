-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f03b11_new
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
	,rpdoc
	,rpdct
	,rpkco
	,rpkco_conv
	,rpsfx
	,rpsfx_conv
	,rpan8
	,rpdgj
	,rpdgj_conv
	,rpdivj
	,rpdivj_conv
	,rpicut
	,rpicu
	,rpdicj
	,rpdicj_conv
	,rpfy
	,rpfy_conv
	,rpctry
	,rppn
	,rpco
	,rpco_conv
	,rpglc
	,rpglc_conv
	,rpaid
	,rpaid_conv
	,rppa8
	,rpan8j
	,rppyr
	,rppost
	,rppost_conv
	,rpistr
	,rpistr_conv
	,rpbalj
	,rppst
	,rpag
	,rpag_conv
	,rpaap
	,rpaap_conv
	,rpadsc
	,rpadsc_conv
	,rpadsa
	,rpadsa_conv
	,rpatxa
	,rpatxa_conv
	,rpatxn
	,rpatxn_conv
	,rpstam
	,rpstam_conv
	,rpbcrc
	,rpbcrc_conv
	,rpcrrm
	,rpcrcd
	,rpcrcd_conv
	,rpcrr
	,rpdmcd
	,rpdmcd_conv
	,rpacr
	,rpacr_conv
	,rpfap
	,rpfap_conv
	,rpcds
	,rpcds_conv
	,rpcdsa
	,rpcdsa_conv
	,rpctxa
	,rpctxa_conv
	,rpctxn
	,rpctxn_conv
	,rpctam
	,rpctam_conv
	,rptxa1
	,rptxa1_conv
	,rpexr1
	,rpdsvj
	,rpdsvj_conv
	,rpglba
	,rpglba_conv
	,rpam
	,rpaid2
	,rpaid2_conv
	,rpam2
	,rpmcu
	,rpmcu_conv
	,rpobj
	,rpobj_conv
	,rpsub
	,rpsub_conv
	,rpsblt
	,rpsbl
	,rpsbl_conv
	,rpptc
	,rpptc_conv
	,rpddj
	,rpddj_conv
	,rpddnj
	,rpddnj_conv
	,rprddj
	,rprddj_conv
	,rprdsj
	,rprdsj_conv
	,rplfcj
	,rplfcj_conv
	,rpsmtj
	,rpsmtj_conv
	,rpnbrr
	,rprdrl
	,rprdrl_conv
	,rprmds
	,rpcoll
	,rpcorc
	,rpafc
	,rpdnlt
	,rpdnlt_conv
	,rprsco
	,rpodoc
	,rpodct
	,rpokco
	,rpokco_conv
	,rposfx
	,rposfx_conv
	,rpvinv
	,rpvinv_conv
	,rppo
	,rppo_conv
	,rppdct
	,rppkco
	,rppkco_conv
	,rpdcto
	,rplnid
	,rplnid_conv
	,rpsdoc
	,rpsdct
	,rpskco
	,rpskco_conv
	,rpsfxo
	,rpsfxo_conv
	,rpvldt
	,rpvldt_conv
	,rpcmc1
	,rpcmc1_conv
	,rpvr01
	,rpvr01_conv
	,rpunit
	,rpunit_conv
	,rpmcu2
	,rpmcu2_conv
	,rprmk
	,rprmk_conv
	,rpalph
	,rpalph_conv
	,rprf
	,rpdrf
	,rpctl
	,rpctl_conv
	,rpfnlp
	,rpfnlp_conv
	,rpitm
	,rpu
	,rpu_conv
	,rpum
	,rpalt6
	,rpalt6_conv
	,rpryin
	,rpvdgj
	,rpvdgj_conv
	,rpvod
	,rpvod_conv
	,rprp1
	,rprp1_conv
	,rprp2
	,rprp2_conv
	,rprp3
	,rprp3_conv
	,rpar01
	,rpar02
	,rpar03
	,rpar04
	,rpar05
	,rpar06
	,rpar07
	,rpar08
	,rpar09
	,rpar10
	,rpistc
	,rpistc_conv
	,rppyid
	,rpurc1
	,rpurc1_conv
	,rpurdt
	,rpurdt_conv
	,rpurat
	,rpurat_conv
	,rpurab
	,rpurab_conv
	,rpurrf
	,rpurrf_conv
	,rprnid
	,rpppdi
	,rpppdi_conv
	,rptorg
	,rptorg_conv
	,rpuser
	,rpuser_conv
	,rpjcl
	,rpjcl_conv
	,rppid
	,rppid_conv
	,rpupmj
	,rpupmj_conv
	,rpupmt
	,rpddex
	,rpjobn
	,rpjobn_conv
	,rphcrr
	,rphdgj
	,rphdgj_conv
	,rpshpn
	,rpdtxs
	,rpdtxs_conv
	,rpomod
	,rpclmg
	,rpcmgr
	,rpatad
	,rpatad_conv
	,rpctad
	,rpctad_conv
	,rpnrta
	,rpnrta_conv
	,rpfnrt
	,rpfnrt_conv
	,rpprgf
	,rpprgf_conv
	,rpgfl1
	,rpgfl1_conv
	,rpgfl2
	,rpgfl2_conv
	,rpdoco
	,rpkcoo
	,rpkcoo_conv
	,rpsotf
	,rpsotf_conv
	,rpdtpb
	,rpdtpb_conv
	,rperdj
	,rperdj_conv
	,rppwpg
	,rppwpg_conv
	,rpnettcid
	,rpnettcid_conv
	,rpnetdoc
	,rpnetdoc_conv
	,rpnetrc5
	,rpnetrc5_conv
	,rpnetst
	,rpajcl
	,rpajcl_conv
	,rprmr1
	,rprmr1_conv
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
	,cdc.rpdoc
	,cdc.rpdct
	,cdc.rpkco
	,cdc.rpkco_conv
	,cdc.rpsfx
	,cdc.rpsfx_conv
	,cdc.rpan8
	,cdc.rpdgj
	,cdc.rpdgj_conv
	,cdc.rpdivj
	,cdc.rpdivj_conv
	,cdc.rpicut
	,cdc.rpicu
	,cdc.rpdicj
	,cdc.rpdicj_conv
	,cdc.rpfy
	,cdc.rpfy_conv
	,cdc.rpctry
	,cdc.rppn
	,cdc.rpco
	,cdc.rpco_conv
	,cdc.rpglc
	,cdc.rpglc_conv
	,cdc.rpaid
	,cdc.rpaid_conv
	,cdc.rppa8
	,cdc.rpan8j
	,cdc.rppyr
	,cdc.rppost
	,cdc.rppost_conv
	,cdc.rpistr
	,cdc.rpistr_conv
	,cdc.rpbalj
	,cdc.rppst
	,cdc.rpag
	,cdc.rpag_conv
	,cdc.rpaap
	,cdc.rpaap_conv
	,cdc.rpadsc
	,cdc.rpadsc_conv
	,cdc.rpadsa
	,cdc.rpadsa_conv
	,cdc.rpatxa
	,cdc.rpatxa_conv
	,cdc.rpatxn
	,cdc.rpatxn_conv
	,cdc.rpstam
	,cdc.rpstam_conv
	,cdc.rpbcrc
	,cdc.rpbcrc_conv
	,cdc.rpcrrm
	,cdc.rpcrcd
	,cdc.rpcrcd_conv
	,cdc.rpcrr
	,cdc.rpdmcd
	,cdc.rpdmcd_conv
	,cdc.rpacr
	,cdc.rpacr_conv
	,cdc.rpfap
	,cdc.rpfap_conv
	,cdc.rpcds
	,cdc.rpcds_conv
	,cdc.rpcdsa
	,cdc.rpcdsa_conv
	,cdc.rpctxa
	,cdc.rpctxa_conv
	,cdc.rpctxn
	,cdc.rpctxn_conv
	,cdc.rpctam
	,cdc.rpctam_conv
	,cdc.rptxa1
	,cdc.rptxa1_conv
	,cdc.rpexr1
	,cdc.rpdsvj
	,cdc.rpdsvj_conv
	,cdc.rpglba
	,cdc.rpglba_conv
	,cdc.rpam
	,cdc.rpaid2
	,cdc.rpaid2_conv
	,cdc.rpam2
	,cdc.rpmcu
	,cdc.rpmcu_conv
	,cdc.rpobj
	,cdc.rpobj_conv
	,cdc.rpsub
	,cdc.rpsub_conv
	,cdc.rpsblt
	,cdc.rpsbl
	,cdc.rpsbl_conv
	,cdc.rpptc
	,cdc.rpptc_conv
	,cdc.rpddj
	,cdc.rpddj_conv
	,cdc.rpddnj
	,cdc.rpddnj_conv
	,cdc.rprddj
	,cdc.rprddj_conv
	,cdc.rprdsj
	,cdc.rprdsj_conv
	,cdc.rplfcj
	,cdc.rplfcj_conv
	,cdc.rpsmtj
	,cdc.rpsmtj_conv
	,cdc.rpnbrr
	,cdc.rprdrl
	,cdc.rprdrl_conv
	,cdc.rprmds
	,cdc.rpcoll
	,cdc.rpcorc
	,cdc.rpafc
	,cdc.rpdnlt
	,cdc.rpdnlt_conv
	,cdc.rprsco
	,cdc.rpodoc
	,cdc.rpodct
	,cdc.rpokco
	,cdc.rpokco_conv
	,cdc.rposfx
	,cdc.rposfx_conv
	,cdc.rpvinv
	,cdc.rpvinv_conv
	,cdc.rppo
	,cdc.rppo_conv
	,cdc.rppdct
	,cdc.rppkco
	,cdc.rppkco_conv
	,cdc.rpdcto
	,cdc.rplnid
	,cdc.rplnid_conv
	,cdc.rpsdoc
	,cdc.rpsdct
	,cdc.rpskco
	,cdc.rpskco_conv
	,cdc.rpsfxo
	,cdc.rpsfxo_conv
	,cdc.rpvldt
	,cdc.rpvldt_conv
	,cdc.rpcmc1
	,cdc.rpcmc1_conv
	,cdc.rpvr01
	,cdc.rpvr01_conv
	,cdc.rpunit
	,cdc.rpunit_conv
	,cdc.rpmcu2
	,cdc.rpmcu2_conv
	,cdc.rprmk
	,cdc.rprmk_conv
	,cdc.rpalph
	,cdc.rpalph_conv
	,cdc.rprf
	,cdc.rpdrf
	,cdc.rpctl
	,cdc.rpctl_conv
	,cdc.rpfnlp
	,cdc.rpfnlp_conv
	,cdc.rpitm
	,cdc.rpu
	,cdc.rpu_conv
	,cdc.rpum
	,cdc.rpalt6
	,cdc.rpalt6_conv
	,cdc.rpryin
	,cdc.rpvdgj
	,cdc.rpvdgj_conv
	,cdc.rpvod
	,cdc.rpvod_conv
	,cdc.rprp1
	,cdc.rprp1_conv
	,cdc.rprp2
	,cdc.rprp2_conv
	,cdc.rprp3
	,cdc.rprp3_conv
	,cdc.rpar01
	,cdc.rpar02
	,cdc.rpar03
	,cdc.rpar04
	,cdc.rpar05
	,cdc.rpar06
	,cdc.rpar07
	,cdc.rpar08
	,cdc.rpar09
	,cdc.rpar10
	,cdc.rpistc
	,cdc.rpistc_conv
	,cdc.rppyid
	,cdc.rpurc1
	,cdc.rpurc1_conv
	,cdc.rpurdt
	,cdc.rpurdt_conv
	,cdc.rpurat
	,cdc.rpurat_conv
	,cdc.rpurab
	,cdc.rpurab_conv
	,cdc.rpurrf
	,cdc.rpurrf_conv
	,cdc.rprnid
	,cdc.rpppdi
	,cdc.rpppdi_conv
	,cdc.rptorg
	,cdc.rptorg_conv
	,cdc.rpuser
	,cdc.rpuser_conv
	,cdc.rpjcl
	,cdc.rpjcl_conv
	,cdc.rppid
	,cdc.rppid_conv
	,cdc.rpupmj
	,cdc.rpupmj_conv
	,cdc.rpupmt
	,cdc.rpddex
	,cdc.rpjobn
	,cdc.rpjobn_conv
	,cdc.rphcrr
	,cdc.rphdgj
	,cdc.rphdgj_conv
	,cdc.rpshpn
	,cdc.rpdtxs
	,cdc.rpdtxs_conv
	,cdc.rpomod
	,cdc.rpclmg
	,cdc.rpcmgr
	,cdc.rpatad
	,cdc.rpatad_conv
	,cdc.rpctad
	,cdc.rpctad_conv
	,cdc.rpnrta
	,cdc.rpnrta_conv
	,cdc.rpfnrt
	,cdc.rpfnrt_conv
	,cdc.rpprgf
	,cdc.rpprgf_conv
	,cdc.rpgfl1
	,cdc.rpgfl1_conv
	,cdc.rpgfl2
	,cdc.rpgfl2_conv
	,cdc.rpdoco
	,cdc.rpkcoo
	,cdc.rpkcoo_conv
	,cdc.rpsotf
	,cdc.rpsotf_conv
	,cdc.rpdtpb
	,cdc.rpdtpb_conv
	,cdc.rperdj
	,cdc.rperdj_conv
	,cdc.rppwpg
	,cdc.rppwpg_conv
	,cdc.rpnettcid
	,cdc.rpnettcid_conv
	,cdc.rpnetdoc
	,cdc.rpnetdoc_conv
	,cdc.rpnetrc5
	,cdc.rpnetrc5_conv
	,cdc.rpnetst
	,cdc.rpajcl
	,cdc.rpajcl_conv
	,cdc.rprmr1
	,cdc.rprmr1_conv
FROM stg_jde.tmp_upsert_f03b11_cdc cdc
LEFT OUTER JOIN rep_jde.f03b11_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f03b11_new AF:{{ task_instance_key_str }}' ) 