-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4104_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4104_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4104_cdc
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
	,cast(ivan8 as [DECIMAL](38, 4)) as ivan8
	,cast(ivxrt as [NVARCHAR](2)) as ivxrt
	,cast(ivitm as [DECIMAL](38, 4)) as ivitm
	,cast(ivexdj as [INT]) as ivexdj
	,case when cast(ivexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ivexdj as [INT]) %1000, dateadd(year, cast(ivexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ivexdj_conv
	,cast(iveftj as [INT]) as iveftj
	,case when cast(iveftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(iveftj as [INT]) %1000, dateadd(year, cast(iveftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as iveftj_conv
	,ivmcu as ivmcu
	,ltrim(rtrim(ivmcu)) as ivmcu_conv
	,ivcitm as ivcitm
	,ltrim(rtrim(ivcitm)) as ivcitm_conv
	,ivdsc1 as ivdsc1
	,ltrim(rtrim(ivdsc1)) as ivdsc1_conv
	,ivdsc2 as ivdsc2
	,ltrim(rtrim(ivdsc2)) as ivdsc2_conv
	,ivaln as ivaln
	,ltrim(rtrim(ivaln)) as ivaln_conv
	,ivlitm as ivlitm
	,ltrim(rtrim(ivlitm)) as ivlitm_conv
	,ivaitm as ivaitm
	,ltrim(rtrim(ivaitm)) as ivaitm_conv
	,ivurcd as ivurcd
	,ltrim(rtrim(ivurcd)) as ivurcd_conv
	,cast(ivurdt as [INT]) as ivurdt
	,case when cast(ivurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ivurdt as [INT]) %1000, dateadd(year, cast(ivurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ivurdt_conv
	,cast(ivurat as [DECIMAL](38, 4)) as ivurat
	,cast(ivurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ivurat_conv
	,cast(ivurab as [DECIMAL](38, 4)) as ivurab
	,cast(ivurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ivurab_conv
	,ivurrf as ivurrf
	,ltrim(rtrim(ivurrf)) as ivurrf_conv
	,ivuser as ivuser
	,ltrim(rtrim(ivuser)) as ivuser_conv
	,ivpid as ivpid
	,ltrim(rtrim(ivpid)) as ivpid_conv
	,ivjobn as ivjobn
	,ltrim(rtrim(ivjobn)) as ivjobn_conv
	,cast(ivupmj as [INT]) as ivupmj
	,case when cast(ivupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ivupmj as [INT]) %1000, dateadd(year, cast(ivupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ivupmj_conv
	,cast(ivtday as [DECIMAL](38, 4)) as ivtday
	,cast(ivrato as [DECIMAL](38, 4)) as ivrato
	,cast(ivrato as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ivrato_conv
	,cast(ivapsp as [DECIMAL](38, 4)) as ivapsp
	,cast(ivapsp as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ivapsp_conv
	,ividem as ividem
	,ltrim(rtrim(ividem)) as ividem_conv
	,ivapss as ivapss
	,ltrim(rtrim(ivapss)) as ivapss_conv
	,ivcirv as ivcirv
	,ltrim(rtrim(ivcirv)) as ivcirv_conv
	,cast(ivadind as [NVARCHAR](1)) as ivadind
	,cast(ivbpind as [NVARCHAR](1)) as ivbpind
	,cast(ivcardno as [NVARCHAR](4)) as ivcardno
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
	,ivan8
	,ivxrt
	,ivitm
	,ivexdj
	,iveftj
	,ivmcu
	,ivcitm
	,ivdsc1
	,ivdsc2
	,ivaln
	,ivlitm
	,ivaitm
	,ivurcd
	,ivurdt
	,ivurat
	,ivurab
	,ivurrf
	,ivuser
	,ivpid
	,ivjobn
	,ivupmj
	,ivtday
	,ivrato
	,ivapsp
	,ividem
	,ivapss
	,ivcirv
	,ivadind
	,ivbpind
	,ivcardno,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4104_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4104_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4104_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4104_cdc(sys_integration_id); 
