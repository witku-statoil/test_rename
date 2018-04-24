-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f554915_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f554915_cdc


CREATE TABLE stg_jde.tmp_upsert_f554915_cdc
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
	,cast(vgy55vg as [NVARCHAR](10)) as vgy55vg
	,vgvehi as vgvehi
	,ltrim(rtrim(vgvehi)) as vgvehi_conv
	,vgdsc1 as vgdsc1
	,ltrim(rtrim(vgdsc1)) as vgdsc1_conv
	,vgdsc2 as vgdsc2
	,ltrim(rtrim(vgdsc2)) as vgdsc2_conv
	,vg98iddesc as vg98iddesc
	,ltrim(rtrim(vg98iddesc)) as vg98iddesc_conv
	,vgdl011 as vgdl011
	,ltrim(rtrim(vgdl011)) as vgdl011_conv
	,vgy55char1 as vgy55char1
	,ltrim(rtrim(vgy55char1)) as vgy55char1_conv
	,vgy55char2 as vgy55char2
	,ltrim(rtrim(vgy55char2)) as vgy55char2_conv
	,vgy55strg1 as vgy55strg1
	,ltrim(rtrim(vgy55strg1)) as vgy55strg1_conv
	,vgy55strg2 as vgy55strg2
	,ltrim(rtrim(vgy55strg2)) as vgy55strg2_conv
	,vgy55strg3 as vgy55strg3
	,ltrim(rtrim(vgy55strg3)) as vgy55strg3_conv
	,vgy55strg4 as vgy55strg4
	,ltrim(rtrim(vgy55strg4)) as vgy55strg4_conv
	,vgy55strg5 as vgy55strg5
	,ltrim(rtrim(vgy55strg5)) as vgy55strg5_conv
	,vgy55strg6 as vgy55strg6
	,ltrim(rtrim(vgy55strg6)) as vgy55strg6_conv
	,vgy55strg7 as vgy55strg7
	,ltrim(rtrim(vgy55strg7)) as vgy55strg7_conv
	,vgy55strg8 as vgy55strg8
	,ltrim(rtrim(vgy55strg8)) as vgy55strg8_conv
	,vgy55strg9 as vgy55strg9
	,ltrim(rtrim(vgy55strg9)) as vgy55strg9_conv
	,vgy55strg10 as vgy55strg10
	,ltrim(rtrim(vgy55strg10)) as vgy55strg10_conv
	,cast(vgy55num1 as [DECIMAL](38, 4)) as vgy55num1
	,cast(vgy55num1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgy55num1_conv
	,cast(vgy55num2 as [DECIMAL](38, 4)) as vgy55num2
	,cast(vgy55num2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgy55num2_conv
	,cast(vgy55num3 as [DECIMAL](38, 4)) as vgy55num3
	,cast(vgy55num3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgy55num3_conv
	,cast(vgy55num4 as [DECIMAL](38, 4)) as vgy55num4
	,cast(vgy55num4 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as vgy55num4_conv
	,cast(vgy55date1 as [INT]) as vgy55date1
	,case when cast(vgy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vgy55date1 as [INT]) %1000, dateadd(year, cast(vgy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vgy55date1_conv
	,cast(vgy55date2 as [INT]) as vgy55date2
	,case when cast(vgy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vgy55date2 as [INT]) %1000, dateadd(year, cast(vgy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vgy55date2_conv
	,cast(vgy55amnt1 as [DECIMAL](38, 4)) as vgy55amnt1
	,cast(vgy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vgy55amnt1_conv
	,cast(vgy55amnt2 as [DECIMAL](38, 4)) as vgy55amnt2
	,cast(vgy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vgy55amnt2_conv
	,vgurrf as vgurrf
	,ltrim(rtrim(vgurrf)) as vgurrf_conv
	,vgurcd as vgurcd
	,ltrim(rtrim(vgurcd)) as vgurcd_conv
	,cast(vgurat as [DECIMAL](38, 4)) as vgurat
	,cast(vgurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as vgurat_conv
	,cast(vgurab as [DECIMAL](38, 4)) as vgurab
	,cast(vgurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as vgurab_conv
	,vguser as vguser
	,ltrim(rtrim(vguser)) as vguser_conv
	,vgpid as vgpid
	,ltrim(rtrim(vgpid)) as vgpid_conv
	,vgjobn as vgjobn
	,ltrim(rtrim(vgjobn)) as vgjobn_conv
	,cast(vgupmj as [INT]) as vgupmj
	,case when cast(vgupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(vgupmj as [INT]) %1000, dateadd(year, cast(vgupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as vgupmj_conv
	,cast(vgupmt as [DECIMAL](38, 4)) as vgupmt
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
	,vgy55vg
	,vgvehi
	,vgdsc1
	,vgdsc2
	,vg98iddesc
	,vgdl011
	,vgy55char1
	,vgy55char2
	,vgy55strg1
	,vgy55strg2
	,vgy55strg3
	,vgy55strg4
	,vgy55strg5
	,vgy55strg6
	,vgy55strg7
	,vgy55strg8
	,vgy55strg9
	,vgy55strg10
	,vgy55num1
	,vgy55num2
	,vgy55num3
	,vgy55num4
	,vgy55date1
	,vgy55date2
	,vgy55amnt1
	,vgy55amnt2
	,vgurrf
	,vgurcd
	,vgurat
	,vgurab
	,vguser
	,vgpid
	,vgjobn
	,vgupmj
	,vgupmt,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f554915_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f554915_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554915_cdc_sys_integration_id ON stg_jde.tmp_upsert_f554915_cdc(sys_integration_id); 
