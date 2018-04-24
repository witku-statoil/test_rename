-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f41514_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f41514_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f41514_cdc
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
	,rhmcu as rhmcu
	,ltrim(rtrim(rhmcu)) as rhmcu_conv
	,cast(rhitm as [DECIMAL](38, 4)) as rhitm
	,rhtkid as rhtkid
	,ltrim(rtrim(rhtkid)) as rhtkid_conv
	,cast(rhopdt as [INT]) as rhopdt
	,case when cast(rhopdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rhopdt as [INT]) %1000, dateadd(year, cast(rhopdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rhopdt_conv
	,cast(rhrttm as [DECIMAL](38, 4)) as rhrttm
	,cast(rhincn as [DECIMAL](38, 4)) as rhincn
	,cast(rhincn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhincn_conv
	,cast(rhbum0 as [NVARCHAR](2)) as rhbum0
	,cast(rhoutg as [DECIMAL](38, 4)) as rhoutg
	,cast(rhoutg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhoutg_conv
	,cast(rhbum1 as [NVARCHAR](2)) as rhbum1
	,cast(rhopns as [DECIMAL](38, 4)) as rhopns
	,cast(rhopns as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhopns_conv
	,cast(rhbum2 as [NVARCHAR](2)) as rhbum2
	,cast(rhclos as [DECIMAL](38, 4)) as rhclos
	,cast(rhclos as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhclos_conv
	,cast(rhbum3 as [NVARCHAR](2)) as rhbum3
	,cast(rhglqt as [DECIMAL](38, 4)) as rhglqt
	,cast(rhglqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhglqt_conv
	,cast(rhbum4 as [NVARCHAR](2)) as rhbum4
	,rhuser as rhuser
	,ltrim(rtrim(rhuser)) as rhuser_conv
	,rhpid as rhpid
	,ltrim(rtrim(rhpid)) as rhpid_conv
	,rhjobn as rhjobn
	,ltrim(rtrim(rhjobn)) as rhjobn_conv
	,cast(rhupmj as [INT]) as rhupmj
	,case when cast(rhupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rhupmj as [INT]) %1000, dateadd(year, cast(rhupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rhupmj_conv
	,cast(rhtday as [DECIMAL](38, 4)) as rhtday
	,cast(rhinam as [DECIMAL](38, 4)) as rhinam
	,cast(rhinam as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhinam_conv
	,cast(rhhum1 as [NVARCHAR](2)) as rhhum1
	,cast(rhinwg as [DECIMAL](38, 4)) as rhinwg
	,cast(rhinwg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhinwg_conv
	,cast(rhhum2 as [NVARCHAR](2)) as rhhum2
	,cast(rhogam as [DECIMAL](38, 4)) as rhogam
	,cast(rhogam as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhogam_conv
	,cast(rhhum3 as [NVARCHAR](2)) as rhhum3
	,cast(rhogwg as [DECIMAL](38, 4)) as rhogwg
	,cast(rhogwg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhogwg_conv
	,cast(rhhum4 as [NVARCHAR](2)) as rhhum4
	,cast(rhosam as [DECIMAL](38, 4)) as rhosam
	,cast(rhosam as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhosam_conv
	,cast(rhhum5 as [NVARCHAR](2)) as rhhum5
	,cast(rhoswg as [DECIMAL](38, 4)) as rhoswg
	,cast(rhoswg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhoswg_conv
	,cast(rhhum6 as [NVARCHAR](2)) as rhhum6
	,cast(rhcsam as [DECIMAL](38, 4)) as rhcsam
	,cast(rhcsam as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhcsam_conv
	,cast(rhhum7 as [NVARCHAR](2)) as rhhum7
	,cast(rhcswg as [DECIMAL](38, 4)) as rhcswg
	,cast(rhcswg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhcswg_conv
	,cast(rhhum8 as [NVARCHAR](2)) as rhhum8
	,cast(rhglam as [DECIMAL](38, 4)) as rhglam
	,cast(rhglam as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhglam_conv
	,cast(rhhum9 as [NVARCHAR](2)) as rhhum9
	,cast(rhglwg as [DECIMAL](38, 4)) as rhglwg
	,cast(rhglwg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as rhglwg_conv
	,cast(rhhuma as [NVARCHAR](2)) as rhhuma
	,rhurrf as rhurrf
	,ltrim(rtrim(rhurrf)) as rhurrf_conv
	,cast(rhurdt as [INT]) as rhurdt
	,case when cast(rhurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rhurdt as [INT]) %1000, dateadd(year, cast(rhurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rhurdt_conv
	,rhurcd as rhurcd
	,ltrim(rtrim(rhurcd)) as rhurcd_conv
	,cast(rhurat as [DECIMAL](38, 4)) as rhurat
	,cast(rhurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rhurat_conv
	,cast(rhurab as [DECIMAL](38, 4)) as rhurab
	,cast(rhurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rhurab_conv
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
	,rhmcu
	,rhitm
	,rhtkid
	,rhopdt
	,rhrttm
	,rhincn
	,rhbum0
	,rhoutg
	,rhbum1
	,rhopns
	,rhbum2
	,rhclos
	,rhbum3
	,rhglqt
	,rhbum4
	,rhuser
	,rhpid
	,rhjobn
	,rhupmj
	,rhtday
	,rhinam
	,rhhum1
	,rhinwg
	,rhhum2
	,rhogam
	,rhhum3
	,rhogwg
	,rhhum4
	,rhosam
	,rhhum5
	,rhoswg
	,rhhum6
	,rhcsam
	,rhhum7
	,rhcswg
	,rhhum8
	,rhglam
	,rhhum9
	,rhglwg
	,rhhuma
	,rhurrf
	,rhurdt
	,rhurcd
	,rhurat
	,rhurab,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f41514_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f41514_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f41514_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f41514_cdc(sys_integration_id); 
