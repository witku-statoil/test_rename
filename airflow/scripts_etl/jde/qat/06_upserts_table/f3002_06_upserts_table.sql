-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f3002_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f3002_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f3002_cdc
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
	,cast(ixtbm as [NVARCHAR](3)) as ixtbm
	,cast(ixkit as [DECIMAL](38, 4)) as ixkit
	,ixkitl as ixkitl
	,ltrim(rtrim(ixkitl)) as ixkitl_conv
	,ixkita as ixkita
	,ltrim(rtrim(ixkita)) as ixkita_conv
	,ixmmcu as ixmmcu
	,ltrim(rtrim(ixmmcu)) as ixmmcu_conv
	,cast(ixitm as [DECIMAL](38, 4)) as ixitm
	,ixlitm as ixlitm
	,ltrim(rtrim(ixlitm)) as ixlitm_conv
	,ixaitm as ixaitm
	,ltrim(rtrim(ixaitm)) as ixaitm_conv
	,ixcmcu as ixcmcu
	,ltrim(rtrim(ixcmcu)) as ixcmcu_conv
	,cast(ixcpnt as [DECIMAL](38, 4)) as ixcpnt
	,cast(ixsbnt as [DECIMAL](38, 4)) as ixsbnt
	,cast(ixsbnt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixsbnt_conv
	,ixprta as ixprta
	,ltrim(rtrim(ixprta)) as ixprta_conv
	,cast(ixqnty as [DECIMAL](38, 4)) as ixqnty
	,cast(ixqnty as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ixqnty_conv
	,cast(ixum as [NVARCHAR](2)) as ixum
	,cast(ixbqty as [DECIMAL](38, 4)) as ixbqty
	,cast(ixbqty as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ixbqty_conv
	,cast(ixuom as [NVARCHAR](2)) as ixuom
	,ixfvbt as ixfvbt
	,ltrim(rtrim(ixfvbt)) as ixfvbt_conv
	,cast(ixefff as [INT]) as ixefff
	,case when cast(ixefff as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ixefff as [INT]) %1000, dateadd(year, cast(ixefff as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ixefff_conv
	,cast(ixefft as [INT]) as ixefft
	,case when cast(ixefft as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ixefft as [INT]) %1000, dateadd(year, cast(ixefft as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ixefft_conv
	,ixfser as ixfser
	,ltrim(rtrim(ixfser)) as ixfser_conv
	,ixtser as ixtser
	,ltrim(rtrim(ixtser)) as ixtser_conv
	,cast(ixitc as [NVARCHAR](1)) as ixitc
	,ixftrc as ixftrc
	,ltrim(rtrim(ixftrc)) as ixftrc_conv
	,cast(ixoptk as [NVARCHAR](1)) as ixoptk
	,ixforv as ixforv
	,ltrim(rtrim(ixforv)) as ixforv_conv
	,cast(ixcstm as [NVARCHAR](1)) as ixcstm
	,ixcsmp as ixcsmp
	,ltrim(rtrim(ixcsmp)) as ixcsmp_conv
	,ixordw as ixordw
	,ltrim(rtrim(ixordw)) as ixordw_conv
	,cast(ixforq as [NVARCHAR](1)) as ixforq
	,cast(ixcoby as [NVARCHAR](1)) as ixcoby
	,cast(ixcoty as [NVARCHAR](1)) as ixcoty
	,cast(ixfrmp as [DECIMAL](38, 4)) as ixfrmp
	,cast(ixfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ixfrmp_conv
	,cast(ixthrp as [DECIMAL](38, 4)) as ixthrp
	,cast(ixthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ixthrp_conv
	,cast(ixfrgd as [NVARCHAR](3)) as ixfrgd
	,cast(ixthgd as [NVARCHAR](3)) as ixthgd
	,cast(ixopsq as [DECIMAL](38, 4)) as ixopsq
	,cast(ixopsq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixopsq_conv
	,cast(ixbseq as [DECIMAL](38, 4)) as ixbseq
	,cast(ixbseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ixbseq_conv
	,cast(ixftrp as [DECIMAL](38, 4)) as ixftrp
	,cast(ixftrp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixftrp_conv
	,cast(ixf_2rp as [DECIMAL](38, 4)) as ixf_2rp
	,cast(ixf_2rp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixf_2rp_conv
	,cast(ixrscp as [DECIMAL](38, 4)) as ixrscp
	,cast(ixrscp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixrscp_conv
	,cast(ixscrp as [DECIMAL](38, 4)) as ixscrp
	,cast(ixscrp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixscrp_conv
	,cast(ixrewp as [DECIMAL](38, 4)) as ixrewp
	,cast(ixrewp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixrewp_conv
	,cast(ixasip as [DECIMAL](38, 4)) as ixasip
	,cast(ixasip as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixasip_conv
	,cast(ixcpyp as [DECIMAL](38, 4)) as ixcpyp
	,cast(ixcpyp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixcpyp_conv
	,cast(ixstpp as [DECIMAL](38, 4)) as ixstpp
	,cast(ixstpp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixstpp_conv
	,cast(ixlovd as [DECIMAL](38, 4)) as ixlovd
	,cast(ixlovd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ixlovd_conv
	,ixeco as ixeco
	,ltrim(rtrim(ixeco)) as ixeco_conv
	,cast(ixecty as [NVARCHAR](2)) as ixecty
	,cast(ixecod as [INT]) as ixecod
	,case when cast(ixecod as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ixecod as [INT]) %1000, dateadd(year, cast(ixecod as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ixecod_conv
	,ixdsc1 as ixdsc1
	,ltrim(rtrim(ixdsc1)) as ixdsc1_conv
	,ixlnty as ixlnty
	,ltrim(rtrim(ixlnty)) as ixlnty_conv
	,cast(ixpric as [DECIMAL](38, 4)) as ixpric
	,cast(ixpric as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ixpric_conv
	,cast(ixuncs as [DECIMAL](38, 4)) as ixuncs
	,cast(ixuncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ixuncs_conv
	,cast(ixpctk as [DECIMAL](38, 4)) as ixpctk
	,cast(ixpctk as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ixpctk_conv
	,ixshno as ixshno
	,ltrim(rtrim(ixshno)) as ixshno_conv
	,ixomcu as ixomcu
	,ltrim(rtrim(ixomcu)) as ixomcu_conv
	,ixobj as ixobj
	,ltrim(rtrim(ixobj)) as ixobj_conv
	,ixsub as ixsub
	,ltrim(rtrim(ixsub)) as ixsub_conv
	,ixbrev as ixbrev
	,ltrim(rtrim(ixbrev)) as ixbrev_conv
	,ixcmrv as ixcmrv
	,ltrim(rtrim(ixcmrv)) as ixcmrv_conv
	,ixrvno as ixrvno
	,ltrim(rtrim(ixrvno)) as ixrvno_conv
	,cast(ixuupg as [DECIMAL](38, 4)) as ixuupg
	,ixurcd as ixurcd
	,ltrim(rtrim(ixurcd)) as ixurcd_conv
	,cast(ixurdt as [INT]) as ixurdt
	,case when cast(ixurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ixurdt as [INT]) %1000, dateadd(year, cast(ixurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ixurdt_conv
	,cast(ixurat as [DECIMAL](38, 4)) as ixurat
	,cast(ixurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixurat_conv
	,ixurrf as ixurrf
	,ltrim(rtrim(ixurrf)) as ixurrf_conv
	,cast(ixurab as [DECIMAL](38, 4)) as ixurab
	,cast(ixurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ixurab_conv
	,ixuser as ixuser
	,ltrim(rtrim(ixuser)) as ixuser_conv
	,ixpid as ixpid
	,ltrim(rtrim(ixpid)) as ixpid_conv
	,ixjobn as ixjobn
	,ltrim(rtrim(ixjobn)) as ixjobn_conv
	,cast(ixupmj as [INT]) as ixupmj
	,case when cast(ixupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ixupmj as [INT]) %1000, dateadd(year, cast(ixupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ixupmj_conv
	,cast(ixtday as [DECIMAL](38, 4)) as ixtday
	,ixaing as ixaing
	,ltrim(rtrim(ixaing)) as ixaing_conv
	,cast(ixsuco as [INT]) as ixsuco
	,cast(ixsuco as [INT]) / cast(1 as [INT]) as ixsuco_conv
	,cast(ixstrc as [DECIMAL](38, 4)) as ixstrc
	,cast(ixstrc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ixstrc_conv
	,cast(ixendc as [DECIMAL](38, 4)) as ixendc
	,cast(ixendc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ixendc_conv
	,cast(ixapsc as [NVARCHAR](1)) as ixapsc
	,cast(ixcpnb as [DECIMAL](38, 4)) as ixcpnb
	,cast(ixcpnb as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ixcpnb_conv
	,ixbseqan as ixbseqan
	,ltrim(rtrim(ixbseqan)) as ixbseqan_conv
	,ixbchar as ixbchar
	,ltrim(rtrim(ixbchar)) as ixbchar_conv
	,cast(ixbostr as [NVARCHAR](4)) as ixbostr
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
	,ixtbm
	,ixkit
	,ixkitl
	,ixkita
	,ixmmcu
	,ixitm
	,ixlitm
	,ixaitm
	,ixcmcu
	,ixcpnt
	,ixsbnt
	,ixprta
	,ixqnty
	,ixum
	,ixbqty
	,ixuom
	,ixfvbt
	,ixefff
	,ixefft
	,ixfser
	,ixtser
	,ixitc
	,ixftrc
	,ixoptk
	,ixforv
	,ixcstm
	,ixcsmp
	,ixordw
	,ixforq
	,ixcoby
	,ixcoty
	,ixfrmp
	,ixthrp
	,ixfrgd
	,ixthgd
	,ixopsq
	,ixbseq
	,ixftrp
	,ixf_2rp
	,ixrscp
	,ixscrp
	,ixrewp
	,ixasip
	,ixcpyp
	,ixstpp
	,ixlovd
	,ixeco
	,ixecty
	,ixecod
	,ixdsc1
	,ixlnty
	,ixpric
	,ixuncs
	,ixpctk
	,ixshno
	,ixomcu
	,ixobj
	,ixsub
	,ixbrev
	,ixcmrv
	,ixrvno
	,ixuupg
	,ixurcd
	,ixurdt
	,ixurat
	,ixurrf
	,ixurab
	,ixuser
	,ixpid
	,ixjobn
	,ixupmj
	,ixtday
	,ixaing
	,ixsuco
	,ixstrc
	,ixendc
	,ixapsc
	,ixcpnb
	,ixbseqan
	,ixbchar
	,ixbostr,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f3002_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f3002_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f3002_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f3002_cdc(sys_integration_id); 
