-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4931_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4931_cdc


CREATE TABLE stg_jde.tmp_upsert_f4931_cdc
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
	,vgvtyp as vgvtyp
	,ltrim(rtrim(vgvtyp)) as vgvtyp_conv
	,vgdl01 as vgdl01
	,ltrim(rtrim(vgdl01)) as vgdl01_conv
	,vgmcu as vgmcu
	,ltrim(rtrim(vgmcu)) as vgmcu_conv
	,cast(vgmot as [NVARCHAR](3)) as vgmot
	,cast(vgdsgp as [NVARCHAR](3)) as vgdsgp
	,cast(vgdsgs as [NVARCHAR](3)) as vgdsgs
	,cast(vgnce as [DECIMAL](38, 4)) as vgnce
	,cast(vgnce as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vgnce_conv
	,cast(vgwtem as [DECIMAL](38, 4)) as vgwtem
	,cast(vgwtem as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgwtem_conv
	,cast(vgwtca as [DECIMAL](38, 4)) as vgwtca
	,cast(vgwtca as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgwtca_conv
	,cast(vgwtum as [NVARCHAR](2)) as vgwtum
	,cast(vgvlcp as [DECIMAL](38, 4)) as vgvlcp
	,cast(vgvlcp as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgvlcp_conv
	,cast(vgvlcs as [DECIMAL](38, 4)) as vgvlcs
	,cast(vgvlcs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgvlcs_conv
	,cast(vgvlum as [NVARCHAR](2)) as vgvlum
	,cast(vgcvol as [DECIMAL](38, 4)) as vgcvol
	,cast(vgcvol as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgcvol_conv
	,cast(vgcvum as [NVARCHAR](2)) as vgcvum
	,cast(vglcnt as [DECIMAL](38, 4)) as vglcnt
	,cast(vgaxle as [DECIMAL](38, 4)) as vgaxle
	,cast(vgaxle as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vgaxle_conv
	,cast(vgwtax as [DECIMAL](38, 4)) as vgwtax
	,cast(vgseal as [DECIMAL](38, 4)) as vgseal
	,cast(vgseal as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vgseal_conv
	,cast(vglnle as [DECIMAL](38, 4)) as vglnle
	,cast(vglnle as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnle_conv
	,cast(vglnwe as [DECIMAL](38, 4)) as vglnwe
	,cast(vglnwe as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnwe_conv
	,cast(vglnhe as [DECIMAL](38, 4)) as vglnhe
	,cast(vglnhe as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnhe_conv
	,cast(vglnli as [DECIMAL](38, 4)) as vglnli
	,cast(vglnli as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnli_conv
	,cast(vglnwi as [DECIMAL](38, 4)) as vglnwi
	,cast(vglnwi as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnwi_conv
	,cast(vglnhr as [DECIMAL](38, 4)) as vglnhr
	,cast(vglnhr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnhr_conv
	,cast(vglnhc as [DECIMAL](38, 4)) as vglnhc
	,cast(vglnhc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnhc_conv
	,cast(vglnhf as [DECIMAL](38, 4)) as vglnhf
	,cast(vglnhf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnhf_conv
	,cast(vglhrd as [DECIMAL](38, 4)) as vglhrd
	,cast(vglhrd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglhrd_conv
	,cast(vglwrd as [DECIMAL](38, 4)) as vglwrd
	,cast(vglwrd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglwrd_conv
	,cast(vglhsd as [DECIMAL](38, 4)) as vglhsd
	,cast(vglhsd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglhsd_conv
	,cast(vglwsd as [DECIMAL](38, 4)) as vglwsd
	,cast(vglwsd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglwsd_conv
	,cast(vglnvf as [DECIMAL](38, 4)) as vglnvf
	,cast(vglnvf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vglnvf_conv
	,cast(vgluom as [NVARCHAR](2)) as vgluom
	,vgcpfg as vgcpfg
	,ltrim(rtrim(vgcpfg)) as vgcpfg_conv
	,cast(vgbpfg as [NVARCHAR](1)) as vgbpfg
	,vgmlln as vgmlln
	,ltrim(rtrim(vgmlln)) as vgmlln_conv
	,vgurcd as vgurcd
	,ltrim(rtrim(vgurcd)) as vgurcd_conv
	,cast(vgurdt as [INT]) as vgurdt
	,case when cast(vgurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vgurdt as [INT]) %1000, dateadd(year, cast(vgurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vgurdt_conv
	,cast(vgurab as [DECIMAL](38, 4)) as vgurab
	,cast(vgurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vgurab_conv
	,vgurrf as vgurrf
	,ltrim(rtrim(vgurrf)) as vgurrf_conv
	,vguser as vguser
	,ltrim(rtrim(vguser)) as vguser_conv
	,vgpid as vgpid
	,ltrim(rtrim(vgpid)) as vgpid_conv
	,vgjobn as vgjobn
	,ltrim(rtrim(vgjobn)) as vgjobn_conv
	,cast(vgupmj as [INT]) as vgupmj
	,case when cast(vgupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vgupmj as [INT]) %1000, dateadd(year, cast(vgupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vgupmj_conv
	,cast(vgtday as [DECIMAL](38, 4)) as vgtday
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
	,vgvtyp
	,vgdl01
	,vgmcu
	,vgmot
	,vgdsgp
	,vgdsgs
	,vgnce
	,vgwtem
	,vgwtca
	,vgwtum
	,vgvlcp
	,vgvlcs
	,vgvlum
	,vgcvol
	,vgcvum
	,vglcnt
	,vgaxle
	,vgwtax
	,vgseal
	,vglnle
	,vglnwe
	,vglnhe
	,vglnli
	,vglnwi
	,vglnhr
	,vglnhc
	,vglnhf
	,vglhrd
	,vglwrd
	,vglhsd
	,vglwsd
	,vglnvf
	,vgluom
	,vgcpfg
	,vgbpfg
	,vgmlln
	,vgurcd
	,vgurdt
	,vgurab
	,vgurrf
	,vguser
	,vgpid
	,vgjobn
	,vgupmj
	,vgtday,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4931_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4931_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4931_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4931_cdc(sys_integration_id); 
