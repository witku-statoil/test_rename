-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f0411_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f0411_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f0411_cdc
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
	,rpkco as rpkco
	,ltrim(rtrim(rpkco)) as rpkco_conv
	,cast(rpdoc as [DECIMAL](38, 4)) as rpdoc
	,cast(rpdct as [NVARCHAR](2)) as rpdct
	,rpsfx as rpsfx
	,ltrim(rtrim(rpsfx)) as rpsfx_conv
	,cast(rpsfxe as [DECIMAL](38, 4)) as rpsfxe
	,cast(rpdcta as [NVARCHAR](2)) as rpdcta
	,cast(rpan8 as [DECIMAL](38, 4)) as rpan8
	,cast(rppye as [DECIMAL](38, 4)) as rppye
	,cast(rpsnto as [DECIMAL](38, 4)) as rpsnto
	,cast(rpdivj as [INT]) as rpdivj
	,case when cast(rpdivj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdivj as [INT]) %1000, dateadd(year, cast(rpdivj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdivj_conv
	,cast(rpdsvj as [INT]) as rpdsvj
	,case when cast(rpdsvj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdsvj as [INT]) %1000, dateadd(year, cast(rpdsvj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdsvj_conv
	,cast(rpddj as [INT]) as rpddj
	,case when cast(rpddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpddj as [INT]) %1000, dateadd(year, cast(rpddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpddj_conv
	,cast(rpddnj as [INT]) as rpddnj
	,case when cast(rpddnj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpddnj as [INT]) %1000, dateadd(year, cast(rpddnj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpddnj_conv
	,cast(rpdgj as [INT]) as rpdgj
	,case when cast(rpdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdgj as [INT]) %1000, dateadd(year, cast(rpdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdgj_conv
	,cast(rpfy as [DECIMAL](38, 4)) as rpfy
	,cast(rpfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpfy_conv
	,cast(rpctry as [DECIMAL](38, 4)) as rpctry
	,cast(rppn as [DECIMAL](38, 4)) as rppn
	,rpco as rpco
	,ltrim(rtrim(rpco)) as rpco_conv
	,cast(rpicu as [DECIMAL](38, 4)) as rpicu
	,cast(rpicut as [NVARCHAR](2)) as rpicut
	,cast(rpdicj as [INT]) as rpdicj
	,case when cast(rpdicj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpdicj as [INT]) %1000, dateadd(year, cast(rpdicj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpdicj_conv
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
	,rptxa1 as rptxa1
	,ltrim(rtrim(rptxa1)) as rptxa1_conv
	,cast(rpexr1 as [NVARCHAR](2)) as rpexr1
	,cast(rpcrrm as [NVARCHAR](1)) as rpcrrm
	,rpcrcd as rpcrcd
	,ltrim(rtrim(rpcrcd)) as rpcrcd_conv
	,cast(rpcrr as [DECIMAL](38, 4)) as rpcrr
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
	,rpglc as rpglc
	,ltrim(rtrim(rpglc)) as rpglc_conv
	,rpglba as rpglba
	,ltrim(rtrim(rpglba)) as rpglba_conv
	,rppost as rppost
	,ltrim(rtrim(rppost)) as rppost_conv
	,cast(rpam as [NVARCHAR](1)) as rpam
	,rpaid2 as rpaid2
	,ltrim(rtrim(rpaid2)) as rpaid2_conv
	,rpmcu as rpmcu
	,ltrim(rtrim(rpmcu)) as rpmcu_conv
	,rpobj as rpobj
	,ltrim(rtrim(rpobj)) as rpobj_conv
	,rpsub as rpsub
	,ltrim(rtrim(rpsub)) as rpsub_conv
	,cast(rpsblt as [NVARCHAR](1)) as rpsblt
	,rpsbl as rpsbl
	,ltrim(rtrim(rpsbl)) as rpsbl_conv
	,rpbaid as rpbaid
	,ltrim(rtrim(rpbaid)) as rpbaid_conv
	,rpptc as rpptc
	,ltrim(rtrim(rpptc)) as rpptc_conv
	,rpvod as rpvod
	,ltrim(rtrim(rpvod)) as rpvod_conv
	,rpokco as rpokco
	,ltrim(rtrim(rpokco)) as rpokco_conv
	,cast(rpodct as [NVARCHAR](2)) as rpodct
	,cast(rpodoc as [DECIMAL](38, 4)) as rpodoc
	,rposfx as rposfx
	,ltrim(rtrim(rposfx)) as rposfx_conv
	,cast(rpcrc as [NVARCHAR](3)) as rpcrc
	,rpvinv as rpvinv
	,ltrim(rtrim(rpvinv)) as rpvinv_conv
	,rppkco as rppkco
	,ltrim(rtrim(rppkco)) as rppkco_conv
	,rppo as rppo
	,ltrim(rtrim(rppo)) as rppo_conv
	,cast(rppdct as [NVARCHAR](2)) as rppdct
	,cast(rplnid as [DECIMAL](38, 4)) as rplnid
	,cast(rplnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as rplnid_conv
	,rpsfxo as rpsfxo
	,ltrim(rtrim(rpsfxo)) as rpsfxo_conv
	,cast(rpopsq as [DECIMAL](38, 4)) as rpopsq
	,cast(rpopsq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpopsq_conv
	,rpvr01 as rpvr01
	,ltrim(rtrim(rpvr01)) as rpvr01_conv
	,rpunit as rpunit
	,ltrim(rtrim(rpunit)) as rpunit_conv
	,rpmcu2 as rpmcu2
	,ltrim(rtrim(rpmcu2)) as rpmcu2_conv
	,rprmk as rprmk
	,ltrim(rtrim(rprmk)) as rprmk_conv
	,cast(rprf as [NVARCHAR](2)) as rprf
	,cast(rpdrf as [DECIMAL](38, 4)) as rpdrf
	,rpctl as rpctl
	,ltrim(rtrim(rpctl)) as rpctl_conv
	,rpfnlp as rpfnlp
	,ltrim(rtrim(rpfnlp)) as rpfnlp_conv
	,cast(rpu as [DECIMAL](38, 4)) as rpu
	,cast(rpu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpu_conv
	,cast(rpum as [NVARCHAR](2)) as rpum
	,cast(rppyin as [NVARCHAR](1)) as rppyin
	,rptxa3 as rptxa3
	,ltrim(rtrim(rptxa3)) as rptxa3_conv
	,cast(rpexr3 as [NVARCHAR](2)) as rpexr3
	,rprp1 as rprp1
	,ltrim(rtrim(rprp1)) as rprp1_conv
	,rprp2 as rprp2
	,ltrim(rtrim(rprp2)) as rprp2_conv
	,rprp3 as rprp3
	,ltrim(rtrim(rprp3)) as rprp3_conv
	,cast(rpac07 as [NVARCHAR](3)) as rpac07
	,rptnn as rptnn
	,ltrim(rtrim(rptnn)) as rptnn_conv
	,rpdmcd as rpdmcd
	,ltrim(rtrim(rpdmcd)) as rpdmcd_conv
	,cast(rpitm as [DECIMAL](38, 4)) as rpitm
	,cast(rphcrr as [DECIMAL](38, 4)) as rphcrr
	,cast(rphdgj as [INT]) as rphdgj
	,case when cast(rphdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rphdgj as [INT]) %1000, dateadd(year, cast(rphdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rphdgj_conv
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
	,rptorg as rptorg
	,ltrim(rtrim(rptorg)) as rptorg_conv
	,rpuser as rpuser
	,ltrim(rtrim(rpuser)) as rpuser_conv
	,rppid as rppid
	,ltrim(rtrim(rppid)) as rppid_conv
	,cast(rpupmj as [INT]) as rpupmj
	,case when cast(rpupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rpupmj as [INT]) %1000, dateadd(year, cast(rpupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rpupmj_conv
	,cast(rpupmt as [DECIMAL](38, 4)) as rpupmt
	,rpjobn as rpjobn
	,ltrim(rtrim(rpjobn)) as rpjobn_conv
	,rptnst as rptnst
	,ltrim(rtrim(rptnst)) as rptnst_conv
	,cast(rpyc01 as [NVARCHAR](3)) as rpyc01
	,cast(rpyc02 as [NVARCHAR](3)) as rpyc02
	,cast(rpyc03 as [NVARCHAR](3)) as rpyc03
	,cast(rpyc04 as [NVARCHAR](3)) as rpyc04
	,cast(rpyc05 as [NVARCHAR](3)) as rpyc05
	,cast(rpyc06 as [NVARCHAR](3)) as rpyc06
	,cast(rpyc07 as [NVARCHAR](3)) as rpyc07
	,cast(rpyc08 as [NVARCHAR](3)) as rpyc08
	,cast(rpyc09 as [NVARCHAR](3)) as rpyc09
	,cast(rpyc10 as [NVARCHAR](3)) as rpyc10
	,rpdtxs as rpdtxs
	,ltrim(rtrim(rpdtxs)) as rpdtxs_conv
	,rpbcrc as rpbcrc
	,ltrim(rtrim(rpbcrc)) as rpbcrc_conv
	,cast(rpatad as [DECIMAL](38, 4)) as rpatad
	,cast(rpatad as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpatad_conv
	,cast(rpctad as [DECIMAL](38, 4)) as rpctad
	,cast(rpctad as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpctad_conv
	,cast(rpnrta as [DECIMAL](38, 4)) as rpnrta
	,cast(rpnrta as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpnrta_conv
	,cast(rpfnrt as [DECIMAL](38, 4)) as rpfnrt
	,cast(rpfnrt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpfnrt_conv
	,rptaxp as rptaxp
	,ltrim(rtrim(rptaxp)) as rptaxp_conv
	,rpprgf as rpprgf
	,ltrim(rtrim(rpprgf)) as rpprgf_conv
	,rpgfl5 as rpgfl5
	,ltrim(rtrim(rpgfl5)) as rpgfl5_conv
	,rpgfl6 as rpgfl6
	,ltrim(rtrim(rpgfl6)) as rpgfl6_conv
	,cast(rpgam1 as [DECIMAL](38, 4)) as rpgam1
	,cast(rpgam1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpgam1_conv
	,cast(rpgam2 as [DECIMAL](38, 4)) as rpgam2
	,cast(rpgam2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpgam2_conv
	,rpgen4 as rpgen4
	,ltrim(rtrim(rpgen4)) as rpgen4_conv
	,rpgen5 as rpgen5
	,ltrim(rtrim(rpgen5)) as rpgen5_conv
	,cast(rpwtad as [DECIMAL](38, 4)) as rpwtad
	,cast(rpwtad as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpwtad_conv
	,cast(rpwtaf as [DECIMAL](38, 4)) as rpwtaf
	,cast(rpwtaf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rpwtaf_conv
	,rpsmmf as rpsmmf
	,ltrim(rtrim(rpsmmf)) as rpsmmf_conv
	,rppywp as rppywp
	,ltrim(rtrim(rppywp)) as rppywp_conv
	,cast(rppwpg as [DECIMAL](38, 4)) as rppwpg
	,cast(rppwpg as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rppwpg_conv
	,cast(rpnettcid as [DECIMAL](38, 4)) as rpnettcid
	,cast(rpnettcid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpnettcid_conv
	,cast(rpnetdoc as [DECIMAL](38, 4)) as rpnetdoc
	,cast(rpnetdoc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpnetdoc_conv
	,cast(rpnetrc5 as [DECIMAL](38, 4)) as rpnetrc5
	,cast(rpnetrc5 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpnetrc5_conv
	,cast(rpnetst as [NVARCHAR](1)) as rpnetst
	,cast(rpcntrtid as [DECIMAL](38, 4)) as rpcntrtid
	,cast(rpcntrtid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpcntrtid_conv
	,rpcntrtcd as rpcntrtcd
	,ltrim(rtrim(rpcntrtcd)) as rpcntrtcd_conv
	,cast(rpwvid as [DECIMAL](38, 4)) as rpwvid
	,cast(rpwvid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rpwvid_conv
	,rpblscd2 as rpblscd2
	,ltrim(rtrim(rpblscd2)) as rpblscd2_conv
	,rpharper as rpharper
	,ltrim(rtrim(rpharper)) as rpharper_conv
	,rpharsfx as rpharsfx
	,ltrim(rtrim(rpharsfx)) as rpharsfx_conv
	,rpddrl as rpddrl
	,ltrim(rtrim(rpddrl)) as rpddrl_conv
	,cast(rpseqn as [DECIMAL](38, 4)) as rpseqn
	,cast(rpclass01 as [NVARCHAR](3)) as rpclass01
	,cast(rpclass02 as [NVARCHAR](3)) as rpclass02
	,cast(rpclass03 as [NVARCHAR](3)) as rpclass03
	,cast(rpclass04 as [NVARCHAR](3)) as rpclass04
	,cast(rpclass05 as [NVARCHAR](3)) as rpclass05
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
	,rpkco
	,rpdoc
	,rpdct
	,rpsfx
	,rpsfxe
	,rpdcta
	,rpan8
	,rppye
	,rpsnto
	,rpdivj
	,rpdsvj
	,rpddj
	,rpddnj
	,rpdgj
	,rpfy
	,rpctry
	,rppn
	,rpco
	,rpicu
	,rpicut
	,rpdicj
	,rpbalj
	,rppst
	,rpag
	,rpaap
	,rpadsc
	,rpadsa
	,rpatxa
	,rpatxn
	,rpstam
	,rptxa1
	,rpexr1
	,rpcrrm
	,rpcrcd
	,rpcrr
	,rpacr
	,rpfap
	,rpcds
	,rpcdsa
	,rpctxa
	,rpctxn
	,rpctam
	,rpglc
	,rpglba
	,rppost
	,rpam
	,rpaid2
	,rpmcu
	,rpobj
	,rpsub
	,rpsblt
	,rpsbl
	,rpbaid
	,rpptc
	,rpvod
	,rpokco
	,rpodct
	,rpodoc
	,rposfx
	,rpcrc
	,rpvinv
	,rppkco
	,rppo
	,rppdct
	,rplnid
	,rpsfxo
	,rpopsq
	,rpvr01
	,rpunit
	,rpmcu2
	,rprmk
	,rprf
	,rpdrf
	,rpctl
	,rpfnlp
	,rpu
	,rpum
	,rppyin
	,rptxa3
	,rpexr3
	,rprp1
	,rprp2
	,rprp3
	,rpac07
	,rptnn
	,rpdmcd
	,rpitm
	,rphcrr
	,rphdgj
	,rpurc1
	,rpurdt
	,rpurat
	,rpurab
	,rpurrf
	,rptorg
	,rpuser
	,rppid
	,rpupmj
	,rpupmt
	,rpjobn
	,rptnst
	,rpyc01
	,rpyc02
	,rpyc03
	,rpyc04
	,rpyc05
	,rpyc06
	,rpyc07
	,rpyc08
	,rpyc09
	,rpyc10
	,rpdtxs
	,rpbcrc
	,rpatad
	,rpctad
	,rpnrta
	,rpfnrt
	,rptaxp
	,rpprgf
	,rpgfl5
	,rpgfl6
	,rpgam1
	,rpgam2
	,rpgen4
	,rpgen5
	,rpwtad
	,rpwtaf
	,rpsmmf
	,rppywp
	,rppwpg
	,rpnettcid
	,rpnetdoc
	,rpnetrc5
	,rpnetst
	,rpcntrtid
	,rpcntrtcd
	,rpwvid
	,rpblscd2
	,rpharper
	,rpharsfx
	,rpddrl
	,rpseqn
	,rpclass01
	,rpclass02
	,rpclass03
	,rpclass04
	,rpclass05,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f0411_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f0411_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0411_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f0411_cdc(sys_integration_id); 
