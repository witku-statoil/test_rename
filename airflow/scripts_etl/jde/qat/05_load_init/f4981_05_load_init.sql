-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f4981_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f4981_new


CREATE TABLE rep_jde_qat.f4981_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
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
	,cast(fhuk01 as [DECIMAL](38, 4)) as fhuk01
	,cast(fhshpn as [DECIMAL](38, 4)) as fhshpn
	,cast(fhrssn as [DECIMAL](38, 4)) as fhrssn
	,fhvmcu as fhvmcu
	,ltrim(rtrim(fhvmcu)) as fhvmcu_conv
	,cast(fhldnm as [DECIMAL](38, 4)) as fhldnm
	,cast(fhdlno as [DECIMAL](38, 4)) as fhdlno
	,cast(fhoseq as [DECIMAL](38, 4)) as fhoseq
	,cast(fhoseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fhoseq_conv
	,cast(fhnmfc as [NVARCHAR](4)) as fhnmfc
	,cast(fhdsgp as [NVARCHAR](3)) as fhdsgp
	,cast(fhfrt1 as [NVARCHAR](6)) as fhfrt1
	,cast(fhfrt2 as [NVARCHAR](6)) as fhfrt2
	,cast(fhorgn as [DECIMAL](38, 4)) as fhorgn
	,fhnmcu as fhnmcu
	,ltrim(rtrim(fhnmcu)) as fhnmcu_conv
	,fhblpb as fhblpb
	,ltrim(rtrim(fhblpb)) as fhblpb_conv
	,cast(fhovfg as [NVARCHAR](1)) as fhovfg
	,fhovfh as fhovfh
	,ltrim(rtrim(fhovfh)) as fhovfh_conv
	,cast(fhmot as [NVARCHAR](3)) as fhmot
	,cast(fhcars as [DECIMAL](38, 4)) as fhcars
	,cast(fhcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fhcars_conv
	,fhcamd as fhcamd
	,ltrim(rtrim(fhcamd)) as fhcamd_conv
	,cast(fhwgts as [DECIMAL](38, 4)) as fhwgts
	,cast(fhwgts as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fhwgts_conv
	,cast(fhwtum as [NVARCHAR](2)) as fhwtum
	,cast(fhscvl as [DECIMAL](38, 4)) as fhscvl
	,cast(fhscvl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fhscvl_conv
	,cast(fhvlum as [NVARCHAR](2)) as fhvlum
	,cast(fhfrsc as [NVARCHAR](8)) as fhfrsc
	,cast(fhrtnm as [NVARCHAR](10)) as fhrtnm
	,cast(fhsdsq as [DECIMAL](38, 4)) as fhsdsq
	,cast(fhsdsq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fhsdsq_conv
	,cast(fhscsn as [DECIMAL](38, 4)) as fhscsn
	,cast(fhscsn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fhscsn_conv
	,cast(fhrtgb as [NVARCHAR](1)) as fhrtgb
	,cast(fhrtdq as [DECIMAL](38, 4)) as fhrtdq
	,cast(fhrtdq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as fhrtdq_conv
	,cast(fhuom as [NVARCHAR](2)) as fhuom
	,cast(fhcgc1 as [NVARCHAR](3)) as fhcgc1
	,cast(fhag as [DECIMAL](38, 4)) as fhag
	,cast(fhag as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhag_conv
	,cast(fhfaa as [DECIMAL](38, 4)) as fhfaa
	,cast(fhfaa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhfaa_conv
	,cast(fhnamt as [DECIMAL](38, 4)) as fhnamt
	,cast(fhnamt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhnamt_conv
	,fhcrdc as fhcrdc
	,ltrim(rtrim(fhcrdc)) as fhcrdc_conv
	,cast(fhnamf as [DECIMAL](38, 4)) as fhnamf
	,cast(fhnamf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhnamf_conv
	,fhcrcd as fhcrcd
	,ltrim(rtrim(fhcrcd)) as fhcrcd_conv
	,cast(fhdoco as [DECIMAL](38, 4)) as fhdoco
	,cast(fhdcto as [NVARCHAR](2)) as fhdcto
	,fhkcoo as fhkcoo
	,ltrim(rtrim(fhkcoo)) as fhkcoo_conv
	,cast(fhlnid as [DECIMAL](38, 4)) as fhlnid
	,cast(fhlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as fhlnid_conv
	,cast(fhodoc as [DECIMAL](38, 4)) as fhodoc
	,cast(fhodct as [NVARCHAR](2)) as fhodct
	,fhokco as fhokco
	,ltrim(rtrim(fhokco)) as fhokco_conv
	,cast(fhjeln as [DECIMAL](38, 4)) as fhjeln
	,cast(fhdoc as [DECIMAL](38, 4)) as fhdoc
	,cast(fhdct as [NVARCHAR](2)) as fhdct
	,fhkco as fhkco
	,ltrim(rtrim(fhkco)) as fhkco_conv
	,cast(fhdgj as [INT]) as fhdgj
	,case when cast(fhdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fhdgj as [INT]) %1000, dateadd(year, cast(fhdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fhdgj_conv
	,fhvinv as fhvinv
	,ltrim(rtrim(fhvinv)) as fhvinv_conv
	,cast(fhrefq as [NVARCHAR](2)) as fhrefq
	,fhrefn as fhrefn
	,ltrim(rtrim(fhrefn)) as fhrefn_conv
	,cast(fhshan as [DECIMAL](38, 4)) as fhshan
	,fhcty1 as fhcty1
	,ltrim(rtrim(fhcty1)) as fhcty1_conv
	,cast(fhadds as [NVARCHAR](3)) as fhadds
	,fhaddz as fhaddz
	,ltrim(rtrim(fhaddz)) as fhaddz_conv
	,cast(fhzon as [NVARCHAR](3)) as fhzon
	,cast(fhczon as [NVARCHAR](10)) as fhczon
	,cast(fhfrth as [NVARCHAR](3)) as fhfrth
	,cast(fhrtn as [DECIMAL](38, 4)) as fhrtn
	,cast(fhaddj as [INT]) as fhaddj
	,case when cast(fhaddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fhaddj as [INT]) %1000, dateadd(year, cast(fhaddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fhaddj_conv
	,cast(fhukid as [DECIMAL](38, 4)) as fhukid
	,fhurcd as fhurcd
	,ltrim(rtrim(fhurcd)) as fhurcd_conv
	,cast(fhurdt as [INT]) as fhurdt
	,case when cast(fhurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fhurdt as [INT]) %1000, dateadd(year, cast(fhurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fhurdt_conv
	,cast(fhurat as [DECIMAL](38, 4)) as fhurat
	,cast(fhurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhurat_conv
	,cast(fhurab as [DECIMAL](38, 4)) as fhurab
	,cast(fhurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as fhurab_conv
	,fhurrf as fhurrf
	,ltrim(rtrim(fhurrf)) as fhurrf_conv
	,fhuser as fhuser
	,ltrim(rtrim(fhuser)) as fhuser_conv
	,fhpid as fhpid
	,ltrim(rtrim(fhpid)) as fhpid_conv
	,fhjobn as fhjobn
	,ltrim(rtrim(fhjobn)) as fhjobn_conv
	,cast(fhupmj as [INT]) as fhupmj
	,case when cast(fhupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(fhupmj as [INT]) %1000, dateadd(year, cast(fhupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as fhupmj_conv
	,cast(fhtday as [DECIMAL](38, 4)) as fhtday
	,cast(fhfrsn as [NVARCHAR](3)) as fhfrsn
	,fhani as fhani
	,ltrim(rtrim(fhani)) as fhani_conv
	,fhtxa1 as fhtxa1
	,ltrim(rtrim(fhtxa1)) as fhtxa1_conv
	,cast(fhexr1 as [NVARCHAR](2)) as fhexr1
	,fhctyo as fhctyo
	,ltrim(rtrim(fhctyo)) as fhctyo_conv
	,cast(fhadso as [NVARCHAR](3)) as fhadso
	,fhadzo as fhadzo
	,ltrim(rtrim(fhadzo)) as fhadzo_conv
	,cast(fhctro as [NVARCHAR](3)) as fhctro
	,fhsct1 as fhsct1
	,ltrim(rtrim(fhsct1)) as fhsct1_conv
	,fhsct2 as fhsct2
	,ltrim(rtrim(fhsct2)) as fhsct2_conv
	,fhsct3 as fhsct3
	,ltrim(rtrim(fhsct3)) as fhsct3_conv
	,fhsc1o as fhsc1o
	,ltrim(rtrim(fhsc1o)) as fhsc1o_conv
	,fhsc2o as fhsc2o
	,ltrim(rtrim(fhsc2o)) as fhsc2o_conv
	,fhsc3o as fhsc3o
	,ltrim(rtrim(fhsc3o)) as fhsc3o_conv
	,cast(fhlnmb as [DECIMAL](38, 4)) as fhlnmb
	,fhcnmr as fhcnmr
	,ltrim(rtrim(fhcnmr)) as fhcnmr_conv
	,cast(fhstam as [DECIMAL](38, 4)) as fhstam
	,cast(fhstam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhstam_conv
	,cast(fhctam as [DECIMAL](38, 4)) as fhctam
	,cast(fhctam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhctam_conv
	,cast(fhtx as [NVARCHAR](1)) as fhtx
	,fhovrtax as fhovrtax
	,ltrim(rtrim(fhovrtax)) as fhovrtax_conv
	,cast(fhatxa as [DECIMAL](38, 4)) as fhatxa
	,cast(fhatxa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhatxa_conv
	,cast(fhctxa as [DECIMAL](38, 4)) as fhctxa
	,cast(fhctxa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as fhctxa_conv
FROM 
    stg_jde_qat.tmp_f4981_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f4981_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4981_sys_integration_id ON rep_jde_qat.f4981_new(sys_integration_id); 
