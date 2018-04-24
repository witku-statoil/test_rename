-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4215_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4215_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4215_cdc
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
	,cast(xhshpn as [DECIMAL](38, 4)) as xhshpn
	,cast(xhssts as [NVARCHAR](2)) as xhssts
	,xhpnid as xhpnid
	,ltrim(rtrim(xhpnid)) as xhpnid_conv
	,xhhlcf as xhhlcf
	,ltrim(rtrim(xhhlcf)) as xhhlcf_conv
	,cast(xhpkcd as [NVARCHAR](5)) as xhpkcd
	,cast(xhwgq as [NVARCHAR](2)) as xhwgq
	,cast(xhwgts as [DECIMAL](38, 4)) as xhwgts
	,cast(xhwgts as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as xhwgts_conv
	,cast(xhwgtu as [NVARCHAR](2)) as xhwgtu
	,cast(xhidq1 as [NVARCHAR](2)) as xhidq1
	,xhid1 as xhid1
	,ltrim(rtrim(xhid1)) as xhid1_conv
	,cast(xhidq2 as [NVARCHAR](2)) as xhidq2
	,xhid2 as xhid2
	,ltrim(rtrim(xhid2)) as xhid2_conv
	,cast(xhmot as [NVARCHAR](3)) as xhmot
	,xhrote as xhrote
	,ltrim(rtrim(xhrote)) as xhrote_conv
	,cast(xheqcd as [NVARCHAR](2)) as xheqcd
	,xheqin as xheqin
	,ltrim(rtrim(xheqin)) as xheqin_conv
	,xheqnb as xheqnb
	,ltrim(rtrim(xheqnb)) as xheqnb_conv
	,cast(xhasna as [NVARCHAR](2)) as xhasna
	,cast(xhasnd as [INT]) as xhasnd
	,case when cast(xhasnd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(xhasnd as [INT]) %1000, dateadd(year, cast(xhasnd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as xhasnd_conv
	,cast(xhasnt as [DECIMAL](38, 4)) as xhasnt
	,xhmcu as xhmcu
	,ltrim(rtrim(xhmcu)) as xhmcu_conv
	,xhnmcu as xhnmcu
	,ltrim(rtrim(xhnmcu)) as xhnmcu_conv
	,cast(xhorgn as [DECIMAL](38, 4)) as xhorgn
	,cast(xhsrco as [NVARCHAR](1)) as xhsrco
	,cast(xhbpfg as [NVARCHAR](1)) as xhbpfg
	,cast(xhaexp as [DECIMAL](38, 4)) as xhaexp
	,cast(xhaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as xhaexp_conv
	,cast(xhecst as [DECIMAL](38, 4)) as xhecst
	,cast(xhecst as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as xhecst_conv
	,cast(xhdrqj as [INT]) as xhdrqj
	,case when cast(xhdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(xhdrqj as [INT]) %1000, dateadd(year, cast(xhdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as xhdrqj_conv
	,cast(xhdrqt as [DECIMAL](38, 4)) as xhdrqt
	,cast(xhrsdj as [INT]) as xhrsdj
	,case when cast(xhrsdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(xhrsdj as [INT]) %1000, dateadd(year, cast(xhrsdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as xhrsdj_conv
	,cast(xhrsdt as [DECIMAL](38, 4)) as xhrsdt
	,xhpdov as xhpdov
	,ltrim(rtrim(xhpdov)) as xhpdov_conv
	,cast(xhan8 as [DECIMAL](38, 4)) as xhan8
	,cast(xhshan as [DECIMAL](38, 4)) as xhshan
	,xhcty1 as xhcty1
	,ltrim(rtrim(xhcty1)) as xhcty1_conv
	,cast(xhadds as [NVARCHAR](3)) as xhadds
	,xhaddz as xhaddz
	,ltrim(rtrim(xhaddz)) as xhaddz_conv
	,cast(xhctr as [NVARCHAR](3)) as xhctr
	,cast(xhzon as [NVARCHAR](3)) as xhzon
	,xhsct1 as xhsct1
	,ltrim(rtrim(xhsct1)) as xhsct1_conv
	,xhsct2 as xhsct2
	,ltrim(rtrim(xhsct2)) as xhsct2_conv
	,xhsct3 as xhsct3
	,ltrim(rtrim(xhsct3)) as xhsct3_conv
	,cast(xhcar1 as [DECIMAL](38, 4)) as xhcar1
	,cast(xhcar1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as xhcar1_conv
	,cast(xhcar2 as [DECIMAL](38, 4)) as xhcar2
	,cast(xhcar2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as xhcar2_conv
	,cast(xhcar3 as [DECIMAL](38, 4)) as xhcar3
	,cast(xhcar3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as xhcar3_conv
	,xhilel as xhilel
	,ltrim(rtrim(xhilel)) as xhilel_conv
	,cast(xhfrth as [NVARCHAR](3)) as xhfrth
	,cast(xhfrsc as [NVARCHAR](8)) as xhfrsc
	,cast(xhdllv as [NVARCHAR](1)) as xhdllv
	,cast(xhrslt as [NVARCHAR](1)) as xhrslt
	,xhbfsd as xhbfsd
	,ltrim(rtrim(xhbfsd)) as xhbfsd_conv
	,cast(xhdstn as [DECIMAL](38, 4)) as xhdstn
	,cast(xhumd1 as [NVARCHAR](2)) as xhumd1
	,cast(xhnrts as [DECIMAL](38, 4)) as xhnrts
	,xhuser as xhuser
	,ltrim(rtrim(xhuser)) as xhuser_conv
	,xhpid as xhpid
	,ltrim(rtrim(xhpid)) as xhpid_conv
	,xhjobn as xhjobn
	,ltrim(rtrim(xhjobn)) as xhjobn_conv
	,cast(xhupmj as [INT]) as xhupmj
	,case when cast(xhupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(xhupmj as [INT]) %1000, dateadd(year, cast(xhupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as xhupmj_conv
	,cast(xhtday as [DECIMAL](38, 4)) as xhtday
	,xhctyo as xhctyo
	,ltrim(rtrim(xhctyo)) as xhctyo_conv
	,cast(xhadso as [NVARCHAR](3)) as xhadso
	,xhadzo as xhadzo
	,ltrim(rtrim(xhadzo)) as xhadzo_conv
	,cast(xhctro as [NVARCHAR](3)) as xhctro
	,xhsc1o as xhsc1o
	,ltrim(rtrim(xhsc1o)) as xhsc1o_conv
	,xhsc2o as xhsc2o
	,ltrim(rtrim(xhsc2o)) as xhsc2o_conv
	,xhsc3o as xhsc3o
	,ltrim(rtrim(xhsc3o)) as xhsc3o_conv
	,xhaetc as xhaetc
	,ltrim(rtrim(xhaetc)) as xhaetc_conv
	,cast(xhetrsc as [NVARCHAR](3)) as xhetrsc
	,cast(xhetrc as [NVARCHAR](3)) as xhetrc
	,xhurcd as xhurcd
	,ltrim(rtrim(xhurcd)) as xhurcd_conv
	,cast(xhurdt as [INT]) as xhurdt
	,case when cast(xhurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(xhurdt as [INT]) %1000, dateadd(year, cast(xhurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as xhurdt_conv
	,cast(xhurat as [DECIMAL](38, 4)) as xhurat
	,cast(xhurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as xhurat_conv
	,cast(xhurab as [DECIMAL](38, 4)) as xhurab
	,cast(xhurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as xhurab_conv
	,xhurrf as xhurrf
	,ltrim(rtrim(xhurrf)) as xhurrf_conv
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
	,xhshpn
	,xhssts
	,xhpnid
	,xhhlcf
	,xhpkcd
	,xhwgq
	,xhwgts
	,xhwgtu
	,xhidq1
	,xhid1
	,xhidq2
	,xhid2
	,xhmot
	,xhrote
	,xheqcd
	,xheqin
	,xheqnb
	,xhasna
	,xhasnd
	,xhasnt
	,xhmcu
	,xhnmcu
	,xhorgn
	,xhsrco
	,xhbpfg
	,xhaexp
	,xhecst
	,xhdrqj
	,xhdrqt
	,xhrsdj
	,xhrsdt
	,xhpdov
	,xhan8
	,xhshan
	,xhcty1
	,xhadds
	,xhaddz
	,xhctr
	,xhzon
	,xhsct1
	,xhsct2
	,xhsct3
	,xhcar1
	,xhcar2
	,xhcar3
	,xhilel
	,xhfrth
	,xhfrsc
	,xhdllv
	,xhrslt
	,xhbfsd
	,xhdstn
	,xhumd1
	,xhnrts
	,xhuser
	,xhpid
	,xhjobn
	,xhupmj
	,xhtday
	,xhctyo
	,xhadso
	,xhadzo
	,xhctro
	,xhsc1o
	,xhsc2o
	,xhsc3o
	,xhaetc
	,xhetrsc
	,xhetrc
	,xhurcd
	,xhurdt
	,xhurat
	,xhurab
	,xhurrf,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4215_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4215_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4215_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4215_cdc(sys_integration_id); 
