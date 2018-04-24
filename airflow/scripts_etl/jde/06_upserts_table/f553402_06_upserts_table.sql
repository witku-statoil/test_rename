-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f553402_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f553402_cdc


CREATE TABLE stg_jde.tmp_upsert_f553402_cdc
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
	,mfy55fornu as mfy55fornu
	,ltrim(rtrim(mfy55fornu)) as mfy55fornu_conv
	,cast(mfitm as [DECIMAL](38, 4)) as mfitm
	,cast(mfan8 as [DECIMAL](38, 4)) as mfan8
	,mfky as mfky
	,ltrim(rtrim(mfky)) as mfky_conv
	,mfalph as mfalph
	,ltrim(rtrim(mfalph)) as mfalph_conv
	,mfbncv as mfbncv
	,ltrim(rtrim(mfbncv)) as mfbncv_conv
	,cast(mfc9dtpr as [INT]) as mfc9dtpr
	,case when cast(mfc9dtpr as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfc9dtpr as [INT]) %1000, dateadd(year, cast(mfc9dtpr as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfc9dtpr_conv
	,cast(mfc9perfrom as [INT]) as mfc9perfrom
	,case when cast(mfc9perfrom as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfc9perfrom as [INT]) %1000, dateadd(year, cast(mfc9perfrom as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfc9perfrom_conv
	,cast(mfc9perthru as [INT]) as mfc9perthru
	,case when cast(mfc9perthru as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfc9perthru as [INT]) %1000, dateadd(year, cast(mfc9perthru as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfc9perthru_conv
	,cast(mfbcnt as [DECIMAL](38, 4)) as mfbcnt
	,cast(mfbcnt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mfbcnt_conv
	,mfalph2 as mfalph2
	,ltrim(rtrim(mfalph2)) as mfalph2_conv
	,mfemailfrom as mfemailfrom
	,ltrim(rtrim(mfemailfrom)) as mfemailfrom_conv
	,mfalph3 as mfalph3
	,ltrim(rtrim(mfalph3)) as mfalph3_conv
	,mfemal as mfemal
	,ltrim(rtrim(mfemal)) as mfemal_conv
	,mfmcu as mfmcu
	,ltrim(rtrim(mfmcu)) as mfmcu_conv
	,cast(mfcnr1 as [DECIMAL](38, 4)) as mfcnr1
	,cast(mfcnr2 as [DECIMAL](38, 4)) as mfcnr2
	,cast(mfcntf as [DECIMAL](38, 4)) as mfcntf
	,cast(mfcntf as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mfcntf_conv
	,cast(mfrrqj as [INT]) as mfrrqj
	,case when cast(mfrrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfrrqj as [INT]) %1000, dateadd(year, cast(mfrrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfrrqj_conv
	,mfyn as mfyn
	,ltrim(rtrim(mfyn)) as mfyn_conv
	,mfdl01 as mfdl01
	,ltrim(rtrim(mfdl01)) as mfdl01_conv
	,mfansif as mfansif
	,ltrim(rtrim(mfansif)) as mfansif_conv
	,mfdl02 as mfdl02
	,ltrim(rtrim(mfdl02)) as mfdl02_conv
	,mfdl03 as mfdl03
	,ltrim(rtrim(mfdl03)) as mfdl03_conv
	,mfdl04 as mfdl04
	,ltrim(rtrim(mfdl04)) as mfdl04_conv
	,cast(mfabcs as [NVARCHAR](1)) as mfabcs
	,mflitm as mflitm
	,ltrim(rtrim(mflitm)) as mflitm_conv
	,mfdsc1 as mfdsc1
	,ltrim(rtrim(mfdsc1)) as mfdsc1_conv
	,mfcitm as mfcitm
	,ltrim(rtrim(mfcitm)) as mfcitm_conv
	,cast(mflnid as [DECIMAL](38, 4)) as mflnid
	,cast(mflnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as mflnid_conv
	,cast(mfuom as [NVARCHAR](2)) as mfuom
	,cast(mfy55plqty1 as [DECIMAL](38, 4)) as mfy55plqty1
	,cast(mfy55plqty1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty1_conv
	,cast(mfy55cmqty1 as [DECIMAL](38, 4)) as mfy55cmqty1
	,cast(mfy55cmqty1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty1_conv
	,cast(mfy55plqty2 as [DECIMAL](38, 4)) as mfy55plqty2
	,cast(mfy55plqty2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty2_conv
	,cast(mfy55cmqty2 as [DECIMAL](38, 4)) as mfy55cmqty2
	,cast(mfy55cmqty2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty2_conv
	,cast(mfy55plqty3 as [DECIMAL](38, 4)) as mfy55plqty3
	,cast(mfy55plqty3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty3_conv
	,cast(mfy55cmqty3 as [DECIMAL](38, 4)) as mfy55cmqty3
	,cast(mfy55cmqty3 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty3_conv
	,cast(mfy55plqty4 as [DECIMAL](38, 4)) as mfy55plqty4
	,cast(mfy55plqty4 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty4_conv
	,cast(mfy55cmqty4 as [DECIMAL](38, 4)) as mfy55cmqty4
	,cast(mfy55cmqty4 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty4_conv
	,cast(mfy55plqty5 as [DECIMAL](38, 4)) as mfy55plqty5
	,cast(mfy55plqty5 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty5_conv
	,cast(mfy55cmqty5 as [DECIMAL](38, 4)) as mfy55cmqty5
	,cast(mfy55cmqty5 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty5_conv
	,cast(mfy55plqty6 as [DECIMAL](38, 4)) as mfy55plqty6
	,cast(mfy55plqty6 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty6_conv
	,cast(mfy55cmqty6 as [DECIMAL](38, 4)) as mfy55cmqty6
	,cast(mfy55cmqty6 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty6_conv
	,cast(mfy55plqty7 as [DECIMAL](38, 4)) as mfy55plqty7
	,cast(mfy55plqty7 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty7_conv
	,cast(mfy55cmqty7 as [DECIMAL](38, 4)) as mfy55cmqty7
	,cast(mfy55cmqty7 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty7_conv
	,cast(mfy55plqty8 as [DECIMAL](38, 4)) as mfy55plqty8
	,cast(mfy55plqty8 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty8_conv
	,cast(mfy55cmqty8 as [DECIMAL](38, 4)) as mfy55cmqty8
	,cast(mfy55cmqty8 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty8_conv
	,cast(mfy55plqty9 as [DECIMAL](38, 4)) as mfy55plqty9
	,cast(mfy55plqty9 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty9_conv
	,cast(mfy55cmqty9 as [DECIMAL](38, 4)) as mfy55cmqty9
	,cast(mfy55cmqty9 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty9_conv
	,cast(mfy55plqty10 as [DECIMAL](38, 4)) as mfy55plqty10
	,cast(mfy55plqty10 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty10_conv
	,cast(mfy55cmqty10 as [DECIMAL](38, 4)) as mfy55cmqty10
	,cast(mfy55cmqty10 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty10_conv
	,cast(mfy55plqty11 as [DECIMAL](38, 4)) as mfy55plqty11
	,cast(mfy55plqty11 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty11_conv
	,cast(mfy55cmqty11 as [DECIMAL](38, 4)) as mfy55cmqty11
	,cast(mfy55cmqty11 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty11_conv
	,cast(mfy55plqty12 as [DECIMAL](38, 4)) as mfy55plqty12
	,cast(mfy55plqty12 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55plqty12_conv
	,cast(mfy55cmqty12 as [DECIMAL](38, 4)) as mfy55cmqty12
	,cast(mfy55cmqty12 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfy55cmqty12_conv
	,cast(mfplqty as [DECIMAL](38, 4)) as mfplqty
	,cast(mfplqty as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as mfplqty_conv
	,mfflag as mfflag
	,ltrim(rtrim(mfflag)) as mfflag_conv
	,mfnotiflg as mfnotiflg
	,ltrim(rtrim(mfnotiflg)) as mfnotiflg_conv
	,mfcomments as mfcomments
	,ltrim(rtrim(mfcomments)) as mfcomments_conv
	,mfurcd as mfurcd
	,ltrim(rtrim(mfurcd)) as mfurcd_conv
	,cast(mfurdt as [INT]) as mfurdt
	,case when cast(mfurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfurdt as [INT]) %1000, dateadd(year, cast(mfurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfurdt_conv
	,cast(mfurat as [DECIMAL](38, 4)) as mfurat
	,cast(mfurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as mfurat_conv
	,cast(mfurab as [DECIMAL](38, 4)) as mfurab
	,cast(mfurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as mfurab_conv
	,mfurrf as mfurrf
	,ltrim(rtrim(mfurrf)) as mfurrf_conv
	,mfuser as mfuser
	,ltrim(rtrim(mfuser)) as mfuser_conv
	,mfpid as mfpid
	,ltrim(rtrim(mfpid)) as mfpid_conv
	,cast(mfupmj as [INT]) as mfupmj
	,case when cast(mfupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(mfupmj as [INT]) %1000, dateadd(year, cast(mfupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as mfupmj_conv
	,mfjobn as mfjobn
	,ltrim(rtrim(mfjobn)) as mfjobn_conv
	,cast(mfupmt as [DECIMAL](38, 4)) as mfupmt
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
	,mfy55fornu
	,mfitm
	,mfan8
	,mfky
	,mfalph
	,mfbncv
	,mfc9dtpr
	,mfc9perfrom
	,mfc9perthru
	,mfbcnt
	,mfalph2
	,mfemailfrom
	,mfalph3
	,mfemal
	,mfmcu
	,mfcnr1
	,mfcnr2
	,mfcntf
	,mfrrqj
	,mfyn
	,mfdl01
	,mfansif
	,mfdl02
	,mfdl03
	,mfdl04
	,mfabcs
	,mflitm
	,mfdsc1
	,mfcitm
	,mflnid
	,mfuom
	,mfy55plqty1
	,mfy55cmqty1
	,mfy55plqty2
	,mfy55cmqty2
	,mfy55plqty3
	,mfy55cmqty3
	,mfy55plqty4
	,mfy55cmqty4
	,mfy55plqty5
	,mfy55cmqty5
	,mfy55plqty6
	,mfy55cmqty6
	,mfy55plqty7
	,mfy55cmqty7
	,mfy55plqty8
	,mfy55cmqty8
	,mfy55plqty9
	,mfy55cmqty9
	,mfy55plqty10
	,mfy55cmqty10
	,mfy55plqty11
	,mfy55cmqty11
	,mfy55plqty12
	,mfy55cmqty12
	,mfplqty
	,mfflag
	,mfnotiflg
	,mfcomments
	,mfurcd
	,mfurdt
	,mfurat
	,mfurab
	,mfurrf
	,mfuser
	,mfpid
	,mfupmj
	,mfjobn
	,mfupmt,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f553402_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f553402_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f553402_cdc_sys_integration_id ON stg_jde.tmp_upsert_f553402_cdc(sys_integration_id); 
