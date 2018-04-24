-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f554506_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f554506_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f554506_cdc
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
	,ismcu as ismcu
	,ltrim(rtrim(ismcu)) as ismcu_conv
	,cast(isitm as [DECIMAL](38, 4)) as isitm
	,cast(isy55qdte as [INT]) as isy55qdte
	,case when cast(isy55qdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isy55qdte as [INT]) %1000, dateadd(year, cast(isy55qdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isy55qdte_conv
	,cast(istme0 as [DECIMAL](38, 4)) as istme0
	,islitm as islitm
	,ltrim(rtrim(islitm)) as islitm_conv
	,isaitm as isaitm
	,ltrim(rtrim(isaitm)) as isaitm_conv
	,isy55qn as isy55qn
	,ltrim(rtrim(isy55qn)) as isy55qn_conv
	,isy55jdqn as isy55jdqn
	,ltrim(rtrim(isy55jdqn)) as isy55jdqn_conv
	,cast(isy55qt as [NVARCHAR](2)) as isy55qt
	,cast(isuom1 as [NVARCHAR](2)) as isuom1
	,cast(isuncs as [DECIMAL](38, 4)) as isuncs
	,cast(isuncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isuncs_conv
	,iscrcd as iscrcd
	,ltrim(rtrim(iscrcd)) as iscrcd_conv
	,cast(isy55usg as [DECIMAL](38, 4)) as isy55usg
	,cast(isy55usg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55usg_conv
	,isy55crcd1 as isy55crcd1
	,ltrim(rtrim(isy55crcd1)) as isy55crcd1_conv
	,cast(isy55uom1 as [NVARCHAR](2)) as isy55uom1
	,cast(isy55lsg as [DECIMAL](38, 4)) as isy55lsg
	,cast(isy55lsg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55lsg_conv
	,isy55crcd2 as isy55crcd2
	,ltrim(rtrim(isy55crcd2)) as isy55crcd2_conv
	,cast(isy55uom2 as [NVARCHAR](2)) as isy55uom2
	,cast(isy55acs as [DECIMAL](38, 4)) as isy55acs
	,cast(isy55acs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55acs_conv
	,isy55crcd3 as isy55crcd3
	,ltrim(rtrim(isy55crcd3)) as isy55crcd3_conv
	,cast(isy55uom3 as [NVARCHAR](2)) as isy55uom3
	,cast(isy55trc as [DECIMAL](38, 4)) as isy55trc
	,cast(isy55trc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55trc_conv
	,isy55crcd4 as isy55crcd4
	,ltrim(rtrim(isy55crcd4)) as isy55crcd4_conv
	,cast(isy55uom4 as [NVARCHAR](2)) as isy55uom4
	,cast(isy55esg as [DECIMAL](38, 4)) as isy55esg
	,cast(isy55esg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55esg_conv
	,isy55crcd6 as isy55crcd6
	,ltrim(rtrim(isy55crcd6)) as isy55crcd6_conv
	,cast(isy55uom6 as [NVARCHAR](2)) as isy55uom6
	,cast(isy55disc as [DECIMAL](38, 4)) as isy55disc
	,cast(isy55disc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55disc_conv
	,cast(isy55osl as [DECIMAL](38, 4)) as isy55osl
	,cast(isy55osl as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55osl_conv
	,cast(isy55tcst as [DECIMAL](38, 4)) as isy55tcst
	,cast(isy55tcst as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as isy55tcst_conv
	,isy55crcd5 as isy55crcd5
	,ltrim(rtrim(isy55crcd5)) as isy55crcd5_conv
	,cast(isy55uom5 as [NVARCHAR](2)) as isy55uom5
	,cast(isy55avin as [NVARCHAR](2)) as isy55avin
	,cast(isdte as [INT]) as isdte
	,case when cast(isdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isdte as [INT]) %1000, dateadd(year, cast(isdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isdte_conv
	,isflag as isflag
	,ltrim(rtrim(isflag)) as isflag_conv
	,isev01 as isev01
	,ltrim(rtrim(isev01)) as isev01_conv
	,isev02 as isev02
	,ltrim(rtrim(isev02)) as isev02_conv
	,isev03 as isev03
	,ltrim(rtrim(isev03)) as isev03_conv
	,isev04 as isev04
	,ltrim(rtrim(isev04)) as isev04_conv
	,isev05 as isev05
	,ltrim(rtrim(isev05)) as isev05_conv
	,isurrf as isurrf
	,ltrim(rtrim(isurrf)) as isurrf_conv
	,isurcd as isurcd
	,ltrim(rtrim(isurcd)) as isurcd_conv
	,cast(isurab as [DECIMAL](38, 4)) as isurab
	,cast(isurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as isurab_conv
	,cast(isurat as [DECIMAL](38, 4)) as isurat
	,cast(isurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isurat_conv
	,cast(isurdt as [INT]) as isurdt
	,case when cast(isurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isurdt as [INT]) %1000, dateadd(year, cast(isurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isurdt_conv
	,isuser as isuser
	,ltrim(rtrim(isuser)) as isuser_conv
	,ispid as ispid
	,ltrim(rtrim(ispid)) as ispid_conv
	,isjobn as isjobn
	,ltrim(rtrim(isjobn)) as isjobn_conv
	,cast(isupmt as [DECIMAL](38, 4)) as isupmt
	,cast(isupmj as [INT]) as isupmj
	,case when cast(isupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isupmj as [INT]) %1000, dateadd(year, cast(isupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isupmj_conv
	,isy55char1 as isy55char1
	,ltrim(rtrim(isy55char1)) as isy55char1_conv
	,isy55char2 as isy55char2
	,ltrim(rtrim(isy55char2)) as isy55char2_conv
	,cast(isy55date1 as [INT]) as isy55date1
	,case when cast(isy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isy55date1 as [INT]) %1000, dateadd(year, cast(isy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isy55date1_conv
	,cast(isy55date2 as [INT]) as isy55date2
	,case when cast(isy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(isy55date2 as [INT]) %1000, dateadd(year, cast(isy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as isy55date2_conv
	,isy55strg1 as isy55strg1
	,ltrim(rtrim(isy55strg1)) as isy55strg1_conv
	,isy55strg2 as isy55strg2
	,ltrim(rtrim(isy55strg2)) as isy55strg2_conv
	,cast(isy55time1 as [DECIMAL](38, 4)) as isy55time1
	,cast(isy55time2 as [DECIMAL](38, 4)) as isy55time2
	,cast(isy55amnt1 as [DECIMAL](38, 4)) as isy55amnt1
	,cast(isy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isy55amnt1_conv
	,cast(isy55amnt2 as [DECIMAL](38, 4)) as isy55amnt2
	,cast(isy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as isy55amnt2_conv
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
	,ismcu
	,isitm
	,isy55qdte
	,istme0
	,islitm
	,isaitm
	,isy55qn
	,isy55jdqn
	,isy55qt
	,isuom1
	,isuncs
	,iscrcd
	,isy55usg
	,isy55crcd1
	,isy55uom1
	,isy55lsg
	,isy55crcd2
	,isy55uom2
	,isy55acs
	,isy55crcd3
	,isy55uom3
	,isy55trc
	,isy55crcd4
	,isy55uom4
	,isy55esg
	,isy55crcd6
	,isy55uom6
	,isy55disc
	,isy55osl
	,isy55tcst
	,isy55crcd5
	,isy55uom5
	,isy55avin
	,isdte
	,isflag
	,isev01
	,isev02
	,isev03
	,isev04
	,isev05
	,isurrf
	,isurcd
	,isurab
	,isurat
	,isurdt
	,isuser
	,ispid
	,isjobn
	,isupmt
	,isupmj
	,isy55char1
	,isy55char2
	,isy55date1
	,isy55date2
	,isy55strg1
	,isy55strg2
	,isy55time1
	,isy55time2
	,isy55amnt1
	,isy55amnt2,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f554506_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f554506_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f554506_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f554506_cdc(sys_integration_id); 
