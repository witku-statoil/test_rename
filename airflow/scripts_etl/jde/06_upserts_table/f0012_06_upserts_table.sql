-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f0012_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f0012_cdc


CREATE TABLE stg_jde.tmp_upsert_f0012_cdc
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
	,kgitem as kgitem
	,ltrim(rtrim(kgitem)) as kgitem_conv
	,kgco as kgco
	,ltrim(rtrim(kgco)) as kgco_conv
	,kgmcu as kgmcu
	,ltrim(rtrim(kgmcu)) as kgmcu_conv
	,kgobj as kgobj
	,ltrim(rtrim(kgobj)) as kgobj_conv
	,kgsub as kgsub
	,ltrim(rtrim(kgsub)) as kgsub_conv
	,kgdl01 as kgdl01
	,ltrim(rtrim(kgdl01)) as kgdl01_conv
	,kgdl02 as kgdl02
	,ltrim(rtrim(kgdl02)) as kgdl02_conv
	,kgdl03 as kgdl03
	,ltrim(rtrim(kgdl03)) as kgdl03_conv
	,kgdl04 as kgdl04
	,ltrim(rtrim(kgdl04)) as kgdl04_conv
	,kgdl05 as kgdl05
	,ltrim(rtrim(kgdl05)) as kgdl05_conv
	,cast(kgmopt as [NVARCHAR](1)) as kgmopt
	,cast(kgoopt as [NVARCHAR](1)) as kgoopt
	,cast(kgsopt as [NVARCHAR](1)) as kgsopt
	,cast(kgsy as [NVARCHAR](4)) as kgsy
	,cast(kgseqn as [DECIMAL](38, 4)) as kgseqn
	,kguser as kguser
	,ltrim(rtrim(kguser)) as kguser_conv
	,kgpid as kgpid
	,ltrim(rtrim(kgpid)) as kgpid_conv
	,cast(kgupmj as [INT]) as kgupmj
	,case when cast(kgupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(kgupmj as [INT]) %1000, dateadd(year, cast(kgupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as kgupmj_conv
	,kgjobn as kgjobn
	,ltrim(rtrim(kgjobn)) as kgjobn_conv
	,cast(kgupmt as [DECIMAL](38, 4)) as kgupmt
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
	,kgitem
	,kgco
	,kgmcu
	,kgobj
	,kgsub
	,kgdl01
	,kgdl02
	,kgdl03
	,kgdl04
	,kgdl05
	,kgmopt
	,kgoopt
	,kgsopt
	,kgsy
	,kgseqn
	,kguser
	,kgpid
	,kgupmj
	,kgjobn
	,kgupmt,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f0012_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f0012_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f0012_cdc_sys_integration_id ON stg_jde.tmp_upsert_f0012_cdc(sys_integration_id); 
