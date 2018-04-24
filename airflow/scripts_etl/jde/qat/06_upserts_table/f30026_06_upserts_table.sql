-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f30026_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f30026_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f30026_cdc
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
	,cast(ieitm as [DECIMAL](38, 4)) as ieitm
	,ielitm as ielitm
	,ltrim(rtrim(ielitm)) as ielitm_conv
	,ieaitm as ieaitm
	,ltrim(rtrim(ieaitm)) as ieaitm_conv
	,iemmcu as iemmcu
	,ltrim(rtrim(iemmcu)) as iemmcu_conv
	,ielocn as ielocn
	,ltrim(rtrim(ielocn)) as ielocn_conv
	,ielotn as ielotn
	,ltrim(rtrim(ielotn)) as ielotn_conv
	,cast(ieledg as [NVARCHAR](2)) as ieledg
	,cast(iecost as [NVARCHAR](3)) as iecost
	,cast(ielotg as [NVARCHAR](3)) as ielotg
	,cast(iestdc as [DECIMAL](38, 4)) as iestdc
	,cast(iestdc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iestdc_conv
	,cast(iexsmc as [DECIMAL](38, 4)) as iexsmc
	,cast(iexsmc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iexsmc_conv
	,cast(iecsl as [DECIMAL](38, 4)) as iecsl
	,cast(iecsl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iecsl_conv
	,cast(iexscr as [DECIMAL](38, 4)) as iexscr
	,cast(iexscr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iexscr_conv
	,iesctc as iesctc
	,ltrim(rtrim(iesctc)) as iesctc_conv
	,iexsfc as iexsfc
	,ltrim(rtrim(iexsfc)) as iexsfc_conv
	,cast(iestfc as [DECIMAL](38, 4)) as iestfc
	,cast(iestfc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iestfc_conv
	,cast(iexsf as [DECIMAL](38, 4)) as iexsf
	,cast(iexsf as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iexsf_conv
	,ierats as ierats
	,ltrim(rtrim(ierats)) as ierats_conv
	,iexsrc as iexsrc
	,ltrim(rtrim(iexsrc)) as iexsrc_conv
	,cast(iertsd as [DECIMAL](38, 4)) as iertsd
	,cast(iertsd as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iertsd_conv
	,cast(iexsr as [DECIMAL](38, 4)) as iexsr
	,cast(iexsr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iexsr_conv
	,cast(iepflg as [NVARCHAR](1)) as iepflg
	,ieuser as ieuser
	,ltrim(rtrim(ieuser)) as ieuser_conv
	,iepid as iepid
	,ltrim(rtrim(iepid)) as iepid_conv
	,iejobn as iejobn
	,ltrim(rtrim(iejobn)) as iejobn_conv
	,cast(ieupmj as [INT]) as ieupmj
	,case when cast(ieupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ieupmj as [INT]) %1000, dateadd(year, cast(ieupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ieupmj_conv
	,cast(ietday as [DECIMAL](38, 4)) as ietday
	,cast(ieopsq as [DECIMAL](38, 4)) as ieopsq
	,cast(ieopsq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ieopsq_conv
	,iemcul as iemcul
	,ltrim(rtrim(iemcul)) as iemcul_conv
	,iewmcu as iewmcu
	,ltrim(rtrim(iewmcu)) as iewmcu_conv
	,ielda as ielda
	,ltrim(rtrim(ielda)) as ielda_conv
	,cast(ietbm as [NVARCHAR](3)) as ietbm
	,cast(ieacq as [DECIMAL](38, 4)) as ieacq
	,cast(ieacq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ieacq_conv
	,ief as ief
	,ltrim(rtrim(ief)) as ief_conv
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
	,ieitm
	,ielitm
	,ieaitm
	,iemmcu
	,ielocn
	,ielotn
	,ieledg
	,iecost
	,ielotg
	,iestdc
	,iexsmc
	,iecsl
	,iexscr
	,iesctc
	,iexsfc
	,iestfc
	,iexsf
	,ierats
	,iexsrc
	,iertsd
	,iexsr
	,iepflg
	,ieuser
	,iepid
	,iejobn
	,ieupmj
	,ietday
	,ieopsq
	,iemcul
	,iewmcu
	,ielda
	,ietbm
	,ieacq
	,ief,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f30026_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f30026_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f30026_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f30026_cdc(sys_integration_id); 
