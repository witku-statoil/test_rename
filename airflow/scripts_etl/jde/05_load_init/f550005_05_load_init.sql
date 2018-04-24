-- Create rep new table for init
IF OBJECT_ID('rep_jde.f550005_new') IS NOT NULL
    DROP TABLE rep_jde.f550005_new


CREATE TABLE rep_jde.f550005_new
WITH 
(
	HEAP,
    DISTRIBUTION = HASH(sys_integration_id) 
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
	,cmy55cmpcd as cmy55cmpcd
	,stg_jde.prc_decode_f0005_00('00','SS',ltrim(rtrim(cmy55cmpcd)))  as cmy55cmpcd_desc
	,cast(cmcmpid as [DECIMAL](38, 4)) as cmcmpid
	,cast(cmcmpid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as cmcmpid_conv
	,cmy55cmpty as cmy55cmpty
	,stg_jde.prc_decode_f0005_55('55','CM',ltrim(rtrim(cmy55cmpty)))  as cmy55cmpty_desc
	,cmtxky as cmtxky
	,ltrim(rtrim(cmtxky)) as cmtxky_conv
	,cast(cmsdca as [INT]) as cmsdca
	,case when cast(cmsdca as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmsdca as [INT]) %1000, dateadd(year, cast(cmsdca as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmsdca_conv
	,cast(cmedca as [INT]) as cmedca
	,case when cast(cmedca as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmedca as [INT]) %1000, dateadd(year, cast(cmedca as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmedca_conv
	,cmctr as cmctr
	,stg_jde.prc_decode_f0005_00('00','CN',ltrim(rtrim(cmctr)))  as cmctr_desc
	,cmy55theme as cmy55theme
	,stg_jde.prc_decode_f0005_55('55','TH',ltrim(rtrim(cmy55theme)))  as cmy55theme_desc
	,cmcrcd as cmcrcd
	,ltrim(rtrim(cmcrcd)) as cmcrcd_conv
	,cmexdt0 as cmexdt0
	,cmexdt0 as cmexdt0_conv
	,cmexdt1 as cmexdt1
	,cmexdt1 as cmexdt1_conv
	,cast(cmurab as [DECIMAL](38, 4)) as cmurab
	,cast(cmurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as cmurab_conv
	,cmurrf as cmurrf
	,ltrim(rtrim(cmurrf)) as cmurrf_conv
	,cast(cmurdt as [INT]) as cmurdt
	,case when cast(cmurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmurdt as [INT]) %1000, dateadd(year, cast(cmurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmurdt_conv
	,cast(cmurat as [DECIMAL](38, 4)) as cmurat
	,cast(cmurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as cmurat_conv
	,cmurcd as cmurcd
	,ltrim(rtrim(cmurcd)) as cmurcd_conv
	,cmcrtu as cmcrtu
	,ltrim(rtrim(cmcrtu)) as cmcrtu_conv
	,cast(cmcrdj as [INT]) as cmcrdj
	,case when cast(cmcrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmcrdj as [INT]) %1000, dateadd(year, cast(cmcrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmcrdj_conv
	,cmc9crp as cmc9crp
	,ltrim(rtrim(cmc9crp)) as cmc9crp_conv
	,cast(cmupmj as [INT]) as cmupmj
	,case when cast(cmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmupmj as [INT]) %1000, dateadd(year, cast(cmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmupmj_conv
	,cast(cmupmt as [DECIMAL](38, 4)) as cmupmt
	,cmjobn as cmjobn
	,ltrim(rtrim(cmjobn)) as cmjobn_conv
	,cmpid as cmpid
	,ltrim(rtrim(cmpid)) as cmpid_conv
	,cmuser as cmuser
	,ltrim(rtrim(cmuser)) as cmuser_conv
	,cast(cmy55lckdt as [INT]) as cmy55lckdt
	,case when cast(cmy55lckdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmy55lckdt as [INT]) %1000, dateadd(year, cast(cmy55lckdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmy55lckdt_conv
	,cmy55alapr as cmy55alapr
	,ltrim(rtrim(cmy55alapr)) as cmy55alapr_conv
	,cmy55strg1 as cmy55strg1
	,ltrim(rtrim(cmy55strg1)) as cmy55strg1_conv
	,cmy55strg2 as cmy55strg2
	,ltrim(rtrim(cmy55strg2)) as cmy55strg2_conv
	,cast(cmy55amnt1 as [DECIMAL](38, 4)) as cmy55amnt1
	,cast(cmy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as cmy55amnt1_conv
	,cast(cmy55amnt2 as [DECIMAL](38, 4)) as cmy55amnt2
	,cast(cmy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as cmy55amnt2_conv
	,cmy55char1 as cmy55char1
	,ltrim(rtrim(cmy55char1)) as cmy55char1_conv
	,cmy55char2 as cmy55char2
	,ltrim(rtrim(cmy55char2)) as cmy55char2_conv
	,cast(cmy55date1 as [INT]) as cmy55date1
	,case when cast(cmy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmy55date1 as [INT]) %1000, dateadd(year, cast(cmy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmy55date1_conv
	,cast(cmy55date2 as [INT]) as cmy55date2
	,case when cast(cmy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmy55date2 as [INT]) %1000, dateadd(year, cast(cmy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmy55date2_conv
	,cast(cmy55time1 as [DECIMAL](38, 4)) as cmy55time1
	,cast(cmy55time2 as [DECIMAL](38, 4)) as cmy55time2
FROM 
    stg_jde.tmp_f550005_init
OPTION ( LABEL = 'CREATE_rep_jde.f550005_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f550005_sys_integration_id ON rep_jde.f550005_new(sys_integration_id); 
