-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f03b11_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f03b11_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f03b11_cdc
WITH 
(
    DISTRIBUTION = HASH(sys_integration_id) 
    ,HEAP
) AS 
SELECT
    	cast(sys_file_name as [NVARCHAR](100)) as sys_file_name
	,cast(sys_file_ln as [INT]) as sys_file_ln
	,cast(sys_integration_id as [NVARCHAR](200)) as sys_integration_id
	,cast(sys_extract_dt as [NVARCHAR](40)) as sys_extract_dt
	,cast(sys_cdc_dt as [NVARCHAR](40)) as sys_cdc_dt
	,cast(sys_cdc_scn as [NVARCHAR](14)) as sys_cdc_scn
	,cast(sys_cdc_operation_type as [NVARCHAR](15)) as sys_cdc_operation_type
	,cast(sys_cdc_before_after as [NVARCHAR](10)) as sys_cdc_before_after
	,cast(sys_line_modified_ind as [INT]) as sys_line_modified_ind
	,cast(rpdoc as [DECIMAL](38, 4)) as rpdoc
	,cast(rpdct as [NVARCHAR](2)) as rpdct
	,rpkco as rpkco
	,ltrim(rtrim(rpkco)) as rpkco_conv
	,rpsfx as rpsfx
	,ltrim(rtrim(rpsfx)) as rpsfx_conv
	,cast(rpan8 as [DECIMAL](38, 4)) as rpan8
	,cast(rpdgj as [INT]) as rpdgj
	,case when cast(rpdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdgj as [INT]) %1000, dateadd(year, cast(rpdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdgj_conv
	,cast(rpdivj as [INT]) as rpdivj
	,case when cast(rpdivj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdivj as [INT]) %1000, dateadd(year, cast(rpdivj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdivj_conv
	,cast(rpicut as [NVARCHAR](2)) as rpicut
	,cast(rpicu as [DECIMAL](38, 4)) as rpicu
	,cast(rpdicj as [INT]) as rpdicj
	,case when cast(rpdicj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdicj as [INT]) %1000, dateadd(year, cast(rpdicj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdicj_conv
	,cast(rpfy as [DECIMAL](38, 4)) as rpfy
	,cast(rpfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpfy_conv
	,cast(rpctry as [DECIMAL](38, 4)) as rpctry
	,cast(rppn as [DECIMAL](38, 4)) as rppn
	,rpco as rpco
	,ltrim(rtrim(rpco)) as rpco_conv
	,rpglc as rpglc
	,ltrim(rtrim(rpglc)) as rpglc_conv
	,rpaid as rpaid
	,ltrim(rtrim(rpaid)) as rpaid_conv
	,cast(rppa8 as [DECIMAL](38, 4)) as rppa8
	,cast(rpan8j as [DECIMAL](38, 4)) as rpan8j
	,cast(rppyr as [DECIMAL](38, 4)) as rppyr
	,rppost as rppost
	,ltrim(rtrim(rppost)) as rppost_conv
	,rpistr as rpistr
	,ltrim(rtrim(rpistr)) as rpistr_conv
	,cast(rpbalj as [NVARCHAR](1)) as rpbalj
	,cast(rppst as [NVARCHAR](1)) as rppst
	,cast(rpag as [DECIMAL](38, 4)) as rpag
	,cast(rpag as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpag_conv
	,cast(rpaap as [DECIMAL](38, 4)) as rpaap
	,cast(rpaap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpaap_conv
	,cast(rpadsc as [DECIMAL](38, 4)) as rpadsc
	,cast(rpadsc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpadsc_conv
	,cast(rpadsa as [DECIMAL](38, 4)) as rpadsa
	,cast(rpadsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpadsa_conv
	,cast(rpatxa as [DECIMAL](38, 4)) as rpatxa
	,cast(rpatxa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpatxa_conv
	,cast(rpatxn as [DECIMAL](38, 4)) as rpatxn
	,cast(rpatxn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpatxn_conv
	,cast(rpstam as [DECIMAL](38, 4)) as rpstam
	,cast(rpstam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpstam_conv
	,rpbcrc as rpbcrc
	,ltrim(rtrim(rpbcrc)) as rpbcrc_conv
	,cast(rpcrrm as [NVARCHAR](1)) as rpcrrm
	,rpcrcd as rpcrcd
	,ltrim(rtrim(rpcrcd)) as rpcrcd_conv
	,cast(rpcrr as [DECIMAL](38, 4)) as rpcrr
	,rpdmcd as rpdmcd
	,ltrim(rtrim(rpdmcd)) as rpdmcd_conv
	,cast(rpacr as [DECIMAL](38, 4)) as rpacr
	,cast(rpacr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpacr_conv
	,cast(rpfap as [DECIMAL](38, 4)) as rpfap
	,cast(rpfap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpfap_conv
	,cast(rpcds as [DECIMAL](38, 4)) as rpcds
	,cast(rpcds as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpcds_conv
	,cast(rpcdsa as [DECIMAL](38, 4)) as rpcdsa
	,cast(rpcdsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpcdsa_conv
	,cast(rpctxa as [DECIMAL](38, 4)) as rpctxa
	,cast(rpctxa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpctxa_conv
	,cast(rpctxn as [DECIMAL](38, 4)) as rpctxn
	,cast(rpctxn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpctxn_conv
	,cast(rpctam as [DECIMAL](38, 4)) as rpctam
	,cast(rpctam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpctam_conv
	,rptxa1 as rptxa1
	,ltrim(rtrim(rptxa1)) as rptxa1_conv
	,cast(rpexr1 as [NVARCHAR](2)) as rpexr1
	,cast(rpdsvj as [INT]) as rpdsvj
	,case when cast(rpdsvj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdsvj as [INT]) %1000, dateadd(year, cast(rpdsvj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdsvj_conv
	,rpglba as rpglba
	,ltrim(rtrim(rpglba)) as rpglba_conv
	,cast(rpam as [NVARCHAR](1)) as rpam
	,rpaid2 as rpaid2
	,ltrim(rtrim(rpaid2)) as rpaid2_conv
	,cast(rpam2 as [NVARCHAR](1)) as rpam2
	,rpmcu as rpmcu
	,ltrim(rtrim(rpmcu)) as rpmcu_conv
	,rpobj as rpobj
	,ltrim(rtrim(rpobj)) as rpobj_conv
	,rpsub as rpsub
	,ltrim(rtrim(rpsub)) as rpsub_conv
	,cast(rpsblt as [NVARCHAR](1)) as rpsblt
	,rpsbl as rpsbl
	,ltrim(rtrim(rpsbl)) as rpsbl_conv
	,rpptc as rpptc
	,ltrim(rtrim(rpptc)) as rpptc_conv
	,cast(rpddj as [INT]) as rpddj
	,case when cast(rpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpddj as [INT]) %1000, dateadd(year, cast(rpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpddj_conv
	,cast(rpddnj as [INT]) as rpddnj
	,case when cast(rpddnj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpddnj as [INT]) %1000, dateadd(year, cast(rpddnj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpddnj_conv
	,cast(rprddj as [INT]) as rprddj
	,case when cast(rprddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rprddj as [INT]) %1000, dateadd(year, cast(rprddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rprddj_conv
	,cast(rprdsj as [INT]) as rprdsj
	,case when cast(rprdsj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rprdsj as [INT]) %1000, dateadd(year, cast(rprdsj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rprdsj_conv
	,cast(rplfcj as [INT]) as rplfcj
	,case when cast(rplfcj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rplfcj as [INT]) %1000, dateadd(year, cast(rplfcj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rplfcj_conv
	,cast(rpsmtj as [INT]) as rpsmtj
	,case when cast(rpsmtj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpsmtj as [INT]) %1000, dateadd(year, cast(rpsmtj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpsmtj_conv
	,cast(rpnbrr as [NVARCHAR](1)) as rpnbrr
	,rprdrl as rprdrl
	,ltrim(rtrim(rprdrl)) as rprdrl_conv
	,cast(rprmds as [DECIMAL](38, 4)) as rprmds
	,cast(rpcoll as [NVARCHAR](1)) as rpcoll
	,cast(rpcorc as [NVARCHAR](2)) as rpcorc
	,cast(rpafc as [NVARCHAR](1)) as rpafc
	,rpdnlt as rpdnlt
	,ltrim(rtrim(rpdnlt)) as rpdnlt_conv
	,cast(rprsco as [NVARCHAR](2)) as rprsco
	,cast(rpodoc as [DECIMAL](38, 4)) as rpodoc
	,cast(rpodct as [NVARCHAR](2)) as rpodct
	,rpokco as rpokco
	,ltrim(rtrim(rpokco)) as rpokco_conv
	,rposfx as rposfx
	,ltrim(rtrim(rposfx)) as rposfx_conv
	,rpvinv as rpvinv
	,ltrim(rtrim(rpvinv)) as rpvinv_conv
	,rppo as rppo
	,ltrim(rtrim(rppo)) as rppo_conv
	,cast(rppdct as [NVARCHAR](2)) as rppdct
	,rppkco as rppkco
	,ltrim(rtrim(rppkco)) as rppkco_conv
	,cast(rpdcto as [NVARCHAR](2)) as rpdcto
	,cast(rplnid as [DECIMAL](38, 4)) as rplnid
	,cast(rplnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as rplnid_conv
	,cast(rpsdoc as [DECIMAL](38, 4)) as rpsdoc
	,cast(rpsdct as [NVARCHAR](2)) as rpsdct
	,rpskco as rpskco
	,ltrim(rtrim(rpskco)) as rpskco_conv
	,rpsfxo as rpsfxo
	,ltrim(rtrim(rpsfxo)) as rpsfxo_conv
	,cast(rpvldt as [INT]) as rpvldt
	,case when cast(rpvldt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpvldt as [INT]) %1000, dateadd(year, cast(rpvldt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpvldt_conv
	,cast(rpcmc1 as [DECIMAL](38, 4)) as rpcmc1
	,cast(rpcmc1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpcmc1_conv
	,rpvr01 as rpvr01
	,ltrim(rtrim(rpvr01)) as rpvr01_conv
	,rpunit as rpunit
	,ltrim(rtrim(rpunit)) as rpunit_conv
	,rpmcu2 as rpmcu2
	,ltrim(rtrim(rpmcu2)) as rpmcu2_conv
	,rprmk as rprmk
	,ltrim(rtrim(rprmk)) as rprmk_conv
	,rpalph as rpalph
	,ltrim(rtrim(rpalph)) as rpalph_conv
	,cast(rprf as [NVARCHAR](2)) as rprf
	,cast(rpdrf as [DECIMAL](38, 4)) as rpdrf
	,rpctl as rpctl
	,ltrim(rtrim(rpctl)) as rpctl_conv
	,rpfnlp as rpfnlp
	,ltrim(rtrim(rpfnlp)) as rpfnlp_conv
	,cast(rpitm as [DECIMAL](38, 4)) as rpitm
	,cast(rpu as [DECIMAL](38, 4)) as rpu
	,cast(rpu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpu_conv
	,cast(rpum as [NVARCHAR](2)) as rpum
	,rpalt6 as rpalt6
	,ltrim(rtrim(rpalt6)) as rpalt6_conv
	,cast(rpryin as [NVARCHAR](1)) as rpryin
	,cast(rpvdgj as [INT]) as rpvdgj
	,case when cast(rpvdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpvdgj as [INT]) %1000, dateadd(year, cast(rpvdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpvdgj_conv
	,rpvod as rpvod
	,ltrim(rtrim(rpvod)) as rpvod_conv
	,rprp1 as rprp1
	,ltrim(rtrim(rprp1)) as rprp1_conv
	,rprp2 as rprp2
	,ltrim(rtrim(rprp2)) as rprp2_conv
	,rprp3 as rprp3
	,ltrim(rtrim(rprp3)) as rprp3_conv
	,cast(rpar01 as [NVARCHAR](3)) as rpar01
	,cast(rpar02 as [NVARCHAR](3)) as rpar02
	,cast(rpar03 as [NVARCHAR](3)) as rpar03
	,cast(rpar04 as [NVARCHAR](3)) as rpar04
	,cast(rpar05 as [NVARCHAR](3)) as rpar05
	,cast(rpar06 as [NVARCHAR](3)) as rpar06
	,cast(rpar07 as [NVARCHAR](3)) as rpar07
	,cast(rpar08 as [NVARCHAR](3)) as rpar08
	,cast(rpar09 as [NVARCHAR](3)) as rpar09
	,cast(rpar10 as [NVARCHAR](3)) as rpar10
	,rpistc as rpistc
	,ltrim(rtrim(rpistc)) as rpistc_conv
	,cast(rppyid as [DECIMAL](38, 4)) as rppyid
	,rpurc1 as rpurc1
	,ltrim(rtrim(rpurc1)) as rpurc1_conv
	,cast(rpurdt as [INT]) as rpurdt
	,case when cast(rpurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpurdt as [INT]) %1000, dateadd(year, cast(rpurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpurdt_conv
	,cast(rpurat as [DECIMAL](38, 4)) as rpurat
	,cast(rpurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpurat_conv
	,cast(rpurab as [DECIMAL](38, 4)) as rpurab
	,cast(rpurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpurab_conv
	,rpurrf as rpurrf
	,ltrim(rtrim(rpurrf)) as rpurrf_conv
	,cast(rprnid as [DECIMAL](38, 4)) as rprnid
	,cast(rpppdi as [INT]) as rpppdi
	,case when cast(rpppdi as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpppdi as [INT]) %1000, dateadd(year, cast(rpppdi as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpppdi_conv
	,rptorg as rptorg
	,ltrim(rtrim(rptorg)) as rptorg_conv
	,rpuser as rpuser
	,ltrim(rtrim(rpuser)) as rpuser_conv
	,cast(rpjcl as [INT]) as rpjcl
	,case when cast(rpjcl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpjcl as [INT]) %1000, dateadd(year, cast(rpjcl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpjcl_conv
	,rppid as rppid
	,ltrim(rtrim(rppid)) as rppid_conv
	,cast(rpupmj as [INT]) as rpupmj
	,case when cast(rpupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpupmj as [INT]) %1000, dateadd(year, cast(rpupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpupmj_conv
	,cast(rpupmt as [DECIMAL](38, 4)) as rpupmt
	,cast(rpddex as [NVARCHAR](2)) as rpddex
	,rpjobn as rpjobn
	,ltrim(rtrim(rpjobn)) as rpjobn_conv
	,cast(rphcrr as [DECIMAL](38, 4)) as rphcrr
	,cast(rphdgj as [INT]) as rphdgj
	,case when cast(rphdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rphdgj as [INT]) %1000, dateadd(year, cast(rphdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rphdgj_conv
	,cast(rpshpn as [DECIMAL](38, 4)) as rpshpn
	,rpdtxs as rpdtxs
	,ltrim(rtrim(rpdtxs)) as rpdtxs_conv
	,cast(rpomod as [NVARCHAR](1)) as rpomod
	,cast(rpclmg as [NVARCHAR](10)) as rpclmg
	,cast(rpcmgr as [NVARCHAR](10)) as rpcmgr
	,cast(rpatad as [DECIMAL](38, 4)) as rpatad
	,cast(rpatad as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpatad_conv
	,cast(rpctad as [DECIMAL](38, 4)) as rpctad
	,cast(rpctad as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpctad_conv
	,cast(rpnrta as [DECIMAL](38, 4)) as rpnrta
	,cast(rpnrta as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpnrta_conv
	,cast(rpfnrt as [DECIMAL](38, 4)) as rpfnrt
	,cast(rpfnrt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpfnrt_conv
	,rpprgf as rpprgf
	,ltrim(rtrim(rpprgf)) as rpprgf_conv
	,rpgfl1 as rpgfl1
	,ltrim(rtrim(rpgfl1)) as rpgfl1_conv
	,rpgfl2 as rpgfl2
	,ltrim(rtrim(rpgfl2)) as rpgfl2_conv
	,cast(rpdoco as [DECIMAL](38, 4)) as rpdoco
	,rpkcoo as rpkcoo
	,ltrim(rtrim(rpkcoo)) as rpkcoo_conv
	,rpsotf as rpsotf
	,ltrim(rtrim(rpsotf)) as rpsotf_conv
	,cast(rpdtpb as [INT]) as rpdtpb
	,case when cast(rpdtpb as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdtpb as [INT]) %1000, dateadd(year, cast(rpdtpb as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdtpb_conv
	,cast(rperdj as [INT]) as rperdj
	,case when cast(rperdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rperdj as [INT]) %1000, dateadd(year, cast(rperdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rperdj_conv
	,cast(rppwpg as [DECIMAL](38, 4)) as rppwpg
	,cast(rppwpg as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rppwpg_conv
	,cast(rpnettcid as [DECIMAL](38, 4)) as rpnettcid
	,cast(rpnettcid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpnettcid_conv
	,cast(rpnetdoc as [DECIMAL](38, 4)) as rpnetdoc
	,cast(rpnetdoc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpnetdoc_conv
	,cast(rpnetrc5 as [DECIMAL](38, 4)) as rpnetrc5
	,cast(rpnetrc5 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpnetrc5_conv
	,cast(rpnetst as [NVARCHAR](1)) as rpnetst
	,cast(rpajcl as [INT]) as rpajcl
	,case when cast(rpajcl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpajcl as [INT]) %1000, dateadd(year, cast(rpajcl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpajcl_conv
	,rprmr1 as rprmr1
	,ltrim(rtrim(rprmr1)) as rprmr1_conv
FROM (
    SELECT 
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
	,rpsfx
	,rpan8
	,rpdgj
	,rpdivj
	,rpicut
	,rpicu
	,rpdicj
	,rpfy
	,rpctry
	,rppn
	,rpco
	,rpglc
	,rpaid
	,rppa8
	,rpan8j
	,rppyr
	,rppost
	,rpistr
	,rpbalj
	,rppst
	,rpag
	,rpaap
	,rpadsc
	,rpadsa
	,rpatxa
	,rpatxn
	,rpstam
	,rpbcrc
	,rpcrrm
	,rpcrcd
	,rpcrr
	,rpdmcd
	,rpacr
	,rpfap
	,rpcds
	,rpcdsa
	,rpctxa
	,rpctxn
	,rpctam
	,rptxa1
	,rpexr1
	,rpdsvj
	,rpglba
	,rpam
	,rpaid2
	,rpam2
	,rpmcu
	,rpobj
	,rpsub
	,rpsblt
	,rpsbl
	,rpptc
	,rpddj
	,rpddnj
	,rprddj
	,rprdsj
	,rplfcj
	,rpsmtj
	,rpnbrr
	,rprdrl
	,rprmds
	,rpcoll
	,rpcorc
	,rpafc
	,rpdnlt
	,rprsco
	,rpodoc
	,rpodct
	,rpokco
	,rposfx
	,rpvinv
	,rppo
	,rppdct
	,rppkco
	,rpdcto
	,rplnid
	,rpsdoc
	,rpsdct
	,rpskco
	,rpsfxo
	,rpvldt
	,rpcmc1
	,rpvr01
	,rpunit
	,rpmcu2
	,rprmk
	,rpalph
	,rprf
	,rpdrf
	,rpctl
	,rpfnlp
	,rpitm
	,rpu
	,rpum
	,rpalt6
	,rpryin
	,rpvdgj
	,rpvod
	,rprp1
	,rprp2
	,rprp3
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
	,rppyid
	,rpurc1
	,rpurdt
	,rpurat
	,rpurab
	,rpurrf
	,rprnid
	,rpppdi
	,rptorg
	,rpuser
	,rpjcl
	,rppid
	,rpupmj
	,rpupmt
	,rpddex
	,rpjobn
	,rphcrr
	,rphdgj
	,rpshpn
	,rpdtxs
	,rpomod
	,rpclmg
	,rpcmgr
	,rpatad
	,rpctad
	,rpnrta
	,rpfnrt
	,rpprgf
	,rpgfl1
	,rpgfl2
	,rpdoco
	,rpkcoo
	,rpsotf
	,rpdtpb
	,rperdj
	,rppwpg
	,rpnettcid
	,rpnetdoc
	,rpnetrc5
	,rpnetst
	,rpajcl
	,rprmr1,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f03b11_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f03b11_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f03b11_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f03b11_cdc(sys_integration_id); 
