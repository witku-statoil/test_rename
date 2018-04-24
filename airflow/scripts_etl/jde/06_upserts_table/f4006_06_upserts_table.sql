-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4006_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4006_cdc


CREATE TABLE stg_jde.tmp_upsert_f4006_cdc
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
	,cast(oadoco as [DECIMAL](38, 4)) as oadoco
	,cast(oadcto as [NVARCHAR](2)) as oadcto
	,oakcoo as oakcoo
	,ltrim(rtrim(oakcoo)) as oakcoo_conv
	,cast(oaanty as [NVARCHAR](1)) as oaanty
	,oamlnm as oamlnm
	,ltrim(rtrim(oamlnm)) as oamlnm_conv
	,oaadd1 as oaadd1
	,ltrim(rtrim(oaadd1)) as oaadd1_conv
	,oaadd2 as oaadd2
	,ltrim(rtrim(oaadd2)) as oaadd2_conv
	,oaadd3 as oaadd3
	,ltrim(rtrim(oaadd3)) as oaadd3_conv
	,oaadd4 as oaadd4
	,ltrim(rtrim(oaadd4)) as oaadd4_conv
	,oaaddz as oaaddz
	,ltrim(rtrim(oaaddz)) as oaaddz_conv
	,oacty1 as oacty1
	,ltrim(rtrim(oacty1)) as oacty1_conv
	,cast(oacoun as [NVARCHAR](25)) as oacoun
	,cast(oaadds as [NVARCHAR](3)) as oaadds
	,oacrte as oacrte
	,ltrim(rtrim(oacrte)) as oacrte_conv
	,oabkml as oabkml
	,ltrim(rtrim(oabkml)) as oabkml_conv
	,cast(oactr as [NVARCHAR](3)) as oactr
	,oauser as oauser
	,ltrim(rtrim(oauser)) as oauser_conv
	,oapid as oapid
	,ltrim(rtrim(oapid)) as oapid_conv
	,cast(oaupmj as [INT]) as oaupmj
	,case when cast(oaupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(oaupmj as [INT]) %1000, dateadd(year, cast(oaupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as oaupmj_conv
	,oajobn as oajobn
	,ltrim(rtrim(oajobn)) as oajobn_conv
	,cast(oaupmt as [DECIMAL](38, 4)) as oaupmt
	,cast(oalnid as [DECIMAL](38, 4)) as oalnid
	,cast(oalnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as oalnid_conv
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
	,oadoco
	,oadcto
	,oakcoo
	,oaanty
	,oamlnm
	,oaadd1
	,oaadd2
	,oaadd3
	,oaadd4
	,oaaddz
	,oacty1
	,oacoun
	,oaadds
	,oacrte
	,oabkml
	,oactr
	,oauser
	,oapid
	,oaupmj
	,oajobn
	,oaupmt
	,oalnid,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4006_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4006_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4006_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4006_cdc(sys_integration_id); 
