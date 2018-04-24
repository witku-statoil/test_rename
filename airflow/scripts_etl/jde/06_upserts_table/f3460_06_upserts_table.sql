-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f3460_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f3460_cdc


CREATE TABLE stg_jde.tmp_upsert_f3460_cdc
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
	,cast(mfitm as [DECIMAL](38, 4)) as mfitm
	,mflitm as mflitm
	,ltrim(rtrim(mflitm)) as mflitm_conv
	,mfaitm as mfaitm
	,ltrim(rtrim(mfaitm)) as mfaitm_conv
	,mfmcu as mfmcu
	,ltrim(rtrim(mfmcu)) as mfmcu_conv
	,cast(mfdrqj as [INT]) as mfdrqj
	,case when cast(mfdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfdrqj as [INT]) %1000, dateadd(year, cast(mfdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfdrqj_conv
	,cast(mfan8 as [DECIMAL](38, 4)) as mfan8
	,cast(mfuorg as [DECIMAL](38, 4)) as mfuorg
	,cast(mfuorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfuorg_conv
	,cast(mfaexp as [DECIMAL](38, 4)) as mfaexp
	,cast(mfaexp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mfaexp_conv
	,cast(mffam as [DECIMAL](38, 4)) as mffam
	,cast(mffam as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mffam_conv
	,cast(mffqt as [DECIMAL](38, 4)) as mffqt
	,cast(mffqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mffqt_conv
	,cast(mftypf as [NVARCHAR](2)) as mftypf
	,cast(mfdcto as [NVARCHAR](2)) as mfdcto
	,mfbpfc as mfbpfc
	,ltrim(rtrim(mfbpfc)) as mfbpfc_conv
	,mfrvis as mfrvis
	,ltrim(rtrim(mfrvis)) as mfrvis_conv
	,cast(mfupmj as [INT]) as mfupmj
	,case when cast(mfupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfupmj as [INT]) %1000, dateadd(year, cast(mfupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfupmj_conv
	,mfuser as mfuser
	,ltrim(rtrim(mfuser)) as mfuser_conv
	,mfjobn as mfjobn
	,ltrim(rtrim(mfjobn)) as mfjobn_conv
	,mfpid as mfpid
	,ltrim(rtrim(mfpid)) as mfpid_conv
	,cast(mfyr as [DECIMAL](38, 4)) as mfyr
	,cast(mfyr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mfyr_conv
	,cast(mftday as [DECIMAL](38, 4)) as mftday
	,mfpmpn as mfpmpn
	,ltrim(rtrim(mfpmpn)) as mfpmpn_conv
	,cast(mfpns as [DECIMAL](38, 4)) as mfpns
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
	,mfitm
	,mflitm
	,mfaitm
	,mfmcu
	,mfdrqj
	,mfan8
	,mfuorg
	,mfaexp
	,mffam
	,mffqt
	,mftypf
	,mfdcto
	,mfbpfc
	,mfrvis
	,mfupmj
	,mfuser
	,mfjobn
	,mfpid
	,mfyr
	,mftday
	,mfpmpn
	,mfpns,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f3460_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f3460_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f3460_cdc_sys_integration_id ON stg_jde.tmp_upsert_f3460_cdc(sys_integration_id); 
