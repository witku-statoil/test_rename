-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f550006_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f550006_cdc


CREATE TABLE stg_jde.tmp_upsert_f550006_cdc
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
	,pmy55prmcd as pmy55prmcd
	,stg_jde.prc_decode_f0005_00('00','SS',ltrim(rtrim(pmy55prmcd)))  as pmy55prmcd_desc
	,cast(pmprnumb as [DECIMAL](38, 4)) as pmprnumb
	,pmy55prmty as pmy55prmty
	,stg_jde.prc_decode_f0005_55('55','PT',ltrim(rtrim(pmy55prmty)))  as pmy55prmty_desc
	,pmy55cmpcd as pmy55cmpcd
	,stg_jde.prc_decode_f0005_00('00','SS',ltrim(rtrim(pmy55cmpcd)))  as pmy55cmpcd_desc
	,cast(pmcmpid as [DECIMAL](38, 4)) as pmcmpid
	,cast(pmcmpid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pmcmpid_conv
	,pmdl01 as pmdl01
	,ltrim(rtrim(pmdl01)) as pmdl01_conv
	,pmlngp1 as pmlngp1
	,stg_jde.prc_decode_f0005_01('01','LP',ltrim(rtrim(pmlngp1)))  as pmlngp1_desc
	,cast(pmstrt as [INT]) as pmstrt
	,case when cast(pmstrt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmstrt as [INT]) %1000, dateadd(year, cast(pmstrt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmstrt_conv
	,cast(pmwwast as [DECIMAL](38, 4)) as pmwwast
	,cast(pmwwast as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pmwwast_conv
	,cast(pmwwaet as [DECIMAL](38, 4)) as pmwwaet
	,cast(pmwwaet as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pmwwaet_conv
	,cast(pmenddj as [INT]) as pmenddj
	,case when cast(pmenddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmenddj as [INT]) %1000, dateadd(year, cast(pmenddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmenddj_conv
	,pmctr as pmctr
	,stg_jde.prc_decode_f0005_00('00','CN',ltrim(rtrim(pmctr)))  as pmctr_desc
	,pmcrcd as pmcrcd
	,ltrim(rtrim(pmcrcd)) as pmcrcd_conv
	,pmtxky as pmtxky
	,ltrim(rtrim(pmtxky)) as pmtxky_conv
	,pmy55dur as pmy55dur
	,stg_jde.prc_decode_f0005_00('00','SS',ltrim(rtrim(pmy55dur)))  as pmy55dur_desc
	,cast(pmy55hrs as [DECIMAL](38, 4)) as pmy55hrs
	,pmy55prity as pmy55prity
	,stg_jde.prc_decode_f0005_55('55','PY',ltrim(rtrim(pmy55prity)))  as pmy55prity_desc
	,pmy55plcmt as pmy55plcmt
	,stg_jde.prc_decode_f0005_55('55','PS',ltrim(rtrim(pmy55plcmt)))  as pmy55plcmt_desc
	,cast(pmy55sldts as [INT]) as pmy55sldts
	,case when cast(pmy55sldts as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55sldts as [INT]) %1000, dateadd(year, cast(pmy55sldts as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55sldts_conv
	,cast(pmy55sldte as [INT]) as pmy55sldte
	,case when cast(pmy55sldte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55sldte as [INT]) %1000, dateadd(year, cast(pmy55sldte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55sldte_conv
	,cast(pmy55prdts as [INT]) as pmy55prdts
	,case when cast(pmy55prdts as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55prdts as [INT]) %1000, dateadd(year, cast(pmy55prdts as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55prdts_conv
	,cast(pmy55prdte as [INT]) as pmy55prdte
	,case when cast(pmy55prdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55prdte as [INT]) %1000, dateadd(year, cast(pmy55prdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55prdte_conv
	,pmy55cpflg as pmy55cpflg
	,ltrim(rtrim(pmy55cpflg)) as pmy55cpflg_conv
	,pmy55cptyp as pmy55cptyp
	,stg_jde.prc_decode_f0005_55('55','CP',ltrim(rtrim(pmy55cptyp)))  as pmy55cptyp_desc
	,pmy55srst as pmy55srst
	,stg_jde.prc_decode_f0005_55('55','PC',ltrim(rtrim(pmy55srst)))  as pmy55srst_desc
	,pmy55ntst as pmy55ntst
	,stg_jde.prc_decode_f0005_55('55','PC',ltrim(rtrim(pmy55ntst)))  as pmy55ntst_desc
	,cast(pmy55psusd as [INT]) as pmy55psusd
	,case when cast(pmy55psusd as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55psusd as [INT]) %1000, dateadd(year, cast(pmy55psusd as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55psusd_conv
	,cast(pmy55pcand as [INT]) as pmy55pcand
	,case when cast(pmy55pcand as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55pcand as [INT]) %1000, dateadd(year, cast(pmy55pcand as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55pcand_conv
	,cast(pmurab as [DECIMAL](38, 4)) as pmurab
	,cast(pmurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pmurab_conv
	,pmurrf as pmurrf
	,ltrim(rtrim(pmurrf)) as pmurrf_conv
	,cast(pmurdt as [INT]) as pmurdt
	,case when cast(pmurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmurdt as [INT]) %1000, dateadd(year, cast(pmurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmurdt_conv
	,cast(pmurat as [DECIMAL](38, 4)) as pmurat
	,cast(pmurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pmurat_conv
	,pmurcd as pmurcd
	,ltrim(rtrim(pmurcd)) as pmurcd_conv
	,pmcrtu as pmcrtu
	,ltrim(rtrim(pmcrtu)) as pmcrtu_conv
	,cast(pmcrdj as [INT]) as pmcrdj
	,case when cast(pmcrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmcrdj as [INT]) %1000, dateadd(year, cast(pmcrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmcrdj_conv
	,pmc9crp as pmc9crp
	,ltrim(rtrim(pmc9crp)) as pmc9crp_conv
	,cast(pmupmj as [INT]) as pmupmj
	,case when cast(pmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmupmj as [INT]) %1000, dateadd(year, cast(pmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmupmj_conv
	,cast(pmupmt as [DECIMAL](38, 4)) as pmupmt
	,pmjobn as pmjobn
	,ltrim(rtrim(pmjobn)) as pmjobn_conv
	,pmpid as pmpid
	,ltrim(rtrim(pmpid)) as pmpid_conv
	,pmuser as pmuser
	,ltrim(rtrim(pmuser)) as pmuser_conv
	,pmy55alapr as pmy55alapr
	,ltrim(rtrim(pmy55alapr)) as pmy55alapr_conv
	,pmy55strg1 as pmy55strg1
	,ltrim(rtrim(pmy55strg1)) as pmy55strg1_conv
	,pmy55char1 as pmy55char1
	,ltrim(rtrim(pmy55char1)) as pmy55char1_conv
	,pmy55char2 as pmy55char2
	,ltrim(rtrim(pmy55char2)) as pmy55char2_conv
	,pmy55strg2 as pmy55strg2
	,ltrim(rtrim(pmy55strg2)) as pmy55strg2_conv
	,cast(pmy55amnt1 as [DECIMAL](38, 4)) as pmy55amnt1
	,cast(pmy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pmy55amnt1_conv
	,cast(pmy55amnt2 as [DECIMAL](38, 4)) as pmy55amnt2
	,cast(pmy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pmy55amnt2_conv
	,cast(pmy55time1 as [DECIMAL](38, 4)) as pmy55time1
	,cast(pmy55time2 as [DECIMAL](38, 4)) as pmy55time2
	,cast(pmy55date1 as [INT]) as pmy55date1
	,case when cast(pmy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55date1 as [INT]) %1000, dateadd(year, cast(pmy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55date1_conv
	,cast(pmy55date2 as [INT]) as pmy55date2
	,case when cast(pmy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pmy55date2 as [INT]) %1000, dateadd(year, cast(pmy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pmy55date2_conv
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
	,pmy55prmcd
	,pmprnumb
	,pmy55prmty
	,pmy55cmpcd
	,pmcmpid
	,pmdl01
	,pmlngp1
	,pmstrt
	,pmwwast
	,pmwwaet
	,pmenddj
	,pmctr
	,pmcrcd
	,pmtxky
	,pmy55dur
	,pmy55hrs
	,pmy55prity
	,pmy55plcmt
	,pmy55sldts
	,pmy55sldte
	,pmy55prdts
	,pmy55prdte
	,pmy55cpflg
	,pmy55cptyp
	,pmy55srst
	,pmy55ntst
	,pmy55psusd
	,pmy55pcand
	,pmurab
	,pmurrf
	,pmurdt
	,pmurat
	,pmurcd
	,pmcrtu
	,pmcrdj
	,pmc9crp
	,pmupmj
	,pmupmt
	,pmjobn
	,pmpid
	,pmuser
	,pmy55alapr
	,pmy55strg1
	,pmy55char1
	,pmy55char2
	,pmy55strg2
	,pmy55amnt1
	,pmy55amnt2
	,pmy55time1
	,pmy55time2
	,pmy55date1
	,pmy55date2,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f550006_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f550006_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f550006_cdc_sys_integration_id ON stg_jde.tmp_upsert_f550006_cdc(sys_integration_id); 
