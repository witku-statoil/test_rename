-- Insert new rows from upsert table into rep new table
INSERT INTO rep_jde.f0006_new
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
	,mcmcu
	,mcmcu_conv
	,mcstyl
	,mcdc
	,mcdc_conv
	,mcldm
	,mcco
	,mcco_conv
	,mcan8
	,mcan8o
	,mccnty
	,mcadds
	,mcfmod
	,mcdl01
	,mcdl01_conv
	,mcdl02
	,mcdl02_conv
	,mcdl03
	,mcdl03_conv
	,mcdl04
	,mcdl04_conv
	,mcrp01
	,mcrp02
	,mcrp03
	,mcrp04
	,mcrp05
	,mcrp06
	,mcrp07
	,mcrp08
	,mcrp09
	,mcrp10
	,mcrp11
	,mcrp12
	,mcrp13
	,mcrp14
	,mcrp15
	,mcrp16
	,mcrp17
	,mcrp18
	,mcrp19
	,mcrp20
	,mcrp21
	,mcrp22
	,mcrp23
	,mcrp24
	,mcrp25
	,mcrp26
	,mcrp27
	,mcrp28
	,mcrp29
	,mcrp30
	,mcta
	,mcta_conv
	,mctxjs
	,mctxa1
	,mctxa1_conv
	,mcexr1
	,mctc01
	,mctc01_conv
	,mctc02
	,mctc02_conv
	,mctc03
	,mctc03_conv
	,mctc04
	,mctc04_conv
	,mctc05
	,mctc05_conv
	,mctc06
	,mctc06_conv
	,mctc07
	,mctc07_conv
	,mctc08
	,mctc08_conv
	,mctc09
	,mctc09_conv
	,mctc10
	,mctc10_conv
	,mcnd01
	,mcnd02
	,mcnd03
	,mcnd04
	,mcnd05
	,mcnd06
	,mcnd07
	,mcnd08
	,mcnd09
	,mcnd10
	,mccc01
	,mccc01_conv
	,mccc02
	,mccc02_conv
	,mccc03
	,mccc03_conv
	,mccc04
	,mccc04_conv
	,mccc05
	,mccc05_conv
	,mccc06
	,mccc06_conv
	,mccc07
	,mccc07_conv
	,mccc08
	,mccc08_conv
	,mccc09
	,mccc09_conv
	,mccc10
	,mccc10_conv
	,mcpecc
	,mcals
	,mciss
	,mcglba
	,mcglba_conv
	,mcalcl
	,mcalcl_conv
	,mclmth
	,mclf
	,mclf_conv
	,mcobj1
	,mcobj1_conv
	,mcobj2
	,mcobj2_conv
	,mcobj3
	,mcobj3_conv
	,mcsub1
	,mcsub1_conv
	,mctou
	,mctou_conv
	,mcsbli
	,mcanpa
	,mcct
	,mccert
	,mcmcus
	,mcmcus_conv
	,mcbtyp
	,mcbtyp_conv
	,mcpc
	,mcpca
	,mcpca_conv
	,mcpcc
	,mcpcc_conv
	,mcinta
	,mcinta_conv
	,mcintl
	,mcintl_conv
	,mcd1j
	,mcd1j_conv
	,mcd2j
	,mcd2j_conv
	,mcd3j
	,mcd3j_conv
	,mcd4j
	,mcd4j_conv
	,mcd5j
	,mcd5j_conv
	,mcd6j
	,mcd6j_conv
	,mcfpdj
	,mcfpdj_conv
	,mccac
	,mccac_conv
	,mcpac
	,mcpac_conv
	,mceeo
	,mcerc
	,mcuser
	,mcuser_conv
	,mcpid
	,mcpid_conv
	,mcupmj
	,mcupmj_conv
	,mcjobn
	,mcjobn_conv
	,mcupmt
	,mcbptp
	,mcbptp_conv
	,mcapsb
	,mcapsb_conv
	,mctsbu
	,mctsbu_conv
	,mcrp31
	,mcrp32
	,mcrp33
	,mcrp34
	,mcrp35
	,mcrp36
	,mcrp37
	,mcrp38
	,mcrp39
	,mcrp40
	,mcrp41
	,mcrp42
	,mcrp43
	,mcrp44
	,mcrp45
	,mcrp46
	,mcrp47
	,mcrp48
	,mcrp49
	,mcrp50
	,mcan8gca1
	,mcan8gca2
	,mcan8gca3
	,mcan8gca4
	,mcan8gca5
	,mcrmcu1
	,mcrmcu1_conv
	,mcdoco
	,mcpctn
	,mcclnu
	,mcbuca
	,mcadjent
	,mcadjent_conv
	,mcuafl
	,mcuafl_conv
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
	,cdc.mcmcu
	,cdc.mcmcu_conv
	,cdc.mcstyl
	,cdc.mcdc
	,cdc.mcdc_conv
	,cdc.mcldm
	,cdc.mcco
	,cdc.mcco_conv
	,cdc.mcan8
	,cdc.mcan8o
	,cdc.mccnty
	,cdc.mcadds
	,cdc.mcfmod
	,cdc.mcdl01
	,cdc.mcdl01_conv
	,cdc.mcdl02
	,cdc.mcdl02_conv
	,cdc.mcdl03
	,cdc.mcdl03_conv
	,cdc.mcdl04
	,cdc.mcdl04_conv
	,cdc.mcrp01
	,cdc.mcrp02
	,cdc.mcrp03
	,cdc.mcrp04
	,cdc.mcrp05
	,cdc.mcrp06
	,cdc.mcrp07
	,cdc.mcrp08
	,cdc.mcrp09
	,cdc.mcrp10
	,cdc.mcrp11
	,cdc.mcrp12
	,cdc.mcrp13
	,cdc.mcrp14
	,cdc.mcrp15
	,cdc.mcrp16
	,cdc.mcrp17
	,cdc.mcrp18
	,cdc.mcrp19
	,cdc.mcrp20
	,cdc.mcrp21
	,cdc.mcrp22
	,cdc.mcrp23
	,cdc.mcrp24
	,cdc.mcrp25
	,cdc.mcrp26
	,cdc.mcrp27
	,cdc.mcrp28
	,cdc.mcrp29
	,cdc.mcrp30
	,cdc.mcta
	,cdc.mcta_conv
	,cdc.mctxjs
	,cdc.mctxa1
	,cdc.mctxa1_conv
	,cdc.mcexr1
	,cdc.mctc01
	,cdc.mctc01_conv
	,cdc.mctc02
	,cdc.mctc02_conv
	,cdc.mctc03
	,cdc.mctc03_conv
	,cdc.mctc04
	,cdc.mctc04_conv
	,cdc.mctc05
	,cdc.mctc05_conv
	,cdc.mctc06
	,cdc.mctc06_conv
	,cdc.mctc07
	,cdc.mctc07_conv
	,cdc.mctc08
	,cdc.mctc08_conv
	,cdc.mctc09
	,cdc.mctc09_conv
	,cdc.mctc10
	,cdc.mctc10_conv
	,cdc.mcnd01
	,cdc.mcnd02
	,cdc.mcnd03
	,cdc.mcnd04
	,cdc.mcnd05
	,cdc.mcnd06
	,cdc.mcnd07
	,cdc.mcnd08
	,cdc.mcnd09
	,cdc.mcnd10
	,cdc.mccc01
	,cdc.mccc01_conv
	,cdc.mccc02
	,cdc.mccc02_conv
	,cdc.mccc03
	,cdc.mccc03_conv
	,cdc.mccc04
	,cdc.mccc04_conv
	,cdc.mccc05
	,cdc.mccc05_conv
	,cdc.mccc06
	,cdc.mccc06_conv
	,cdc.mccc07
	,cdc.mccc07_conv
	,cdc.mccc08
	,cdc.mccc08_conv
	,cdc.mccc09
	,cdc.mccc09_conv
	,cdc.mccc10
	,cdc.mccc10_conv
	,cdc.mcpecc
	,cdc.mcals
	,cdc.mciss
	,cdc.mcglba
	,cdc.mcglba_conv
	,cdc.mcalcl
	,cdc.mcalcl_conv
	,cdc.mclmth
	,cdc.mclf
	,cdc.mclf_conv
	,cdc.mcobj1
	,cdc.mcobj1_conv
	,cdc.mcobj2
	,cdc.mcobj2_conv
	,cdc.mcobj3
	,cdc.mcobj3_conv
	,cdc.mcsub1
	,cdc.mcsub1_conv
	,cdc.mctou
	,cdc.mctou_conv
	,cdc.mcsbli
	,cdc.mcanpa
	,cdc.mcct
	,cdc.mccert
	,cdc.mcmcus
	,cdc.mcmcus_conv
	,cdc.mcbtyp
	,cdc.mcbtyp_conv
	,cdc.mcpc
	,cdc.mcpca
	,cdc.mcpca_conv
	,cdc.mcpcc
	,cdc.mcpcc_conv
	,cdc.mcinta
	,cdc.mcinta_conv
	,cdc.mcintl
	,cdc.mcintl_conv
	,cdc.mcd1j
	,cdc.mcd1j_conv
	,cdc.mcd2j
	,cdc.mcd2j_conv
	,cdc.mcd3j
	,cdc.mcd3j_conv
	,cdc.mcd4j
	,cdc.mcd4j_conv
	,cdc.mcd5j
	,cdc.mcd5j_conv
	,cdc.mcd6j
	,cdc.mcd6j_conv
	,cdc.mcfpdj
	,cdc.mcfpdj_conv
	,cdc.mccac
	,cdc.mccac_conv
	,cdc.mcpac
	,cdc.mcpac_conv
	,cdc.mceeo
	,cdc.mcerc
	,cdc.mcuser
	,cdc.mcuser_conv
	,cdc.mcpid
	,cdc.mcpid_conv
	,cdc.mcupmj
	,cdc.mcupmj_conv
	,cdc.mcjobn
	,cdc.mcjobn_conv
	,cdc.mcupmt
	,cdc.mcbptp
	,cdc.mcbptp_conv
	,cdc.mcapsb
	,cdc.mcapsb_conv
	,cdc.mctsbu
	,cdc.mctsbu_conv
	,cdc.mcrp31
	,cdc.mcrp32
	,cdc.mcrp33
	,cdc.mcrp34
	,cdc.mcrp35
	,cdc.mcrp36
	,cdc.mcrp37
	,cdc.mcrp38
	,cdc.mcrp39
	,cdc.mcrp40
	,cdc.mcrp41
	,cdc.mcrp42
	,cdc.mcrp43
	,cdc.mcrp44
	,cdc.mcrp45
	,cdc.mcrp46
	,cdc.mcrp47
	,cdc.mcrp48
	,cdc.mcrp49
	,cdc.mcrp50
	,cdc.mcan8gca1
	,cdc.mcan8gca2
	,cdc.mcan8gca3
	,cdc.mcan8gca4
	,cdc.mcan8gca5
	,cdc.mcrmcu1
	,cdc.mcrmcu1_conv
	,cdc.mcdoco
	,cdc.mcpctn
	,cdc.mcclnu
	,cdc.mcbuca
	,cdc.mcadjent
	,cdc.mcadjent_conv
	,cdc.mcuafl
	,cdc.mcuafl_conv
FROM stg_jde.tmp_upsert_f0006_cdc cdc
LEFT OUTER JOIN rep_jde.f0006_new f ON cdc.sys_integration_id = f.sys_integration_id
WHERE f.sys_integration_id IS NULL
OPTION ( LABEL = 'INSERT_rep_jde.f0006_new AF:{{ task_instance_key_str }}' ) 