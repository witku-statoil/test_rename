-- Create rep new table for init
IF OBJECT_ID('rep_jde.f554103_new') IS NOT NULL
    DROP TABLE rep_jde.f554103_new


CREATE TABLE rep_jde.f554103_new
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
	,cast(alresc as [DECIMAL](38, 4)) as alresc
	,cast(alresc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as alresc_conv
	,cast(alukid as [DECIMAL](38, 4)) as alukid
	,altrmnum as altrmnum
	,ltrim(rtrim(altrmnum)) as altrmnum_conv
	,alfrto as alfrto
	,ltrim(rtrim(alfrto)) as alfrto_conv
	,aledbt as aledbt
	,ltrim(rtrim(aledbt)) as aledbt_conv
	,aledtn as aledtn
	,ltrim(rtrim(aledtn)) as aledtn_conv
	,alfrsq as alfrsq
	,ltrim(rtrim(alfrsq)) as alfrsq_conv
	,cast(aly55sttsl as [NVARCHAR](2)) as aly55sttsl
	,almcu as almcu
	,ltrim(rtrim(almcu)) as almcu_conv
	,cast(alddday as [INT]) as alddday
	,case when cast(alddday as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(alddday as [INT]) %1000, dateadd(year, cast(alddday as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as alddday_conv
	,cast(altday as [DECIMAL](38, 4)) as altday
	,alvehi as alvehi
	,ltrim(rtrim(alvehi)) as alvehi_conv
	,aly55owid as aly55owid
	,ltrim(rtrim(aly55owid)) as aly55owid_conv
	,cast(alitm as [DECIMAL](38, 4)) as alitm
	,allitm as allitm
	,ltrim(rtrim(allitm)) as allitm_conv
	,alaitm as alaitm
	,ltrim(rtrim(alaitm)) as alaitm_conv
	,cast(alambr as [DECIMAL](38, 4)) as alambr
	,cast(alambr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as alambr_conv
	,cast(aluom1 as [NVARCHAR](2)) as aluom1
	,cast(album3 as [NVARCHAR](2)) as album3
	,alyshipty as alyshipty
	,ltrim(rtrim(alyshipty)) as alyshipty_conv
	,allocn as allocn
	,ltrim(rtrim(allocn)) as allocn_conv
	,cast(aly55aglc as [NVARCHAR](2)) as aly55aglc
	,cast(aly55agto as [NVARCHAR](2)) as aly55agto
	,alsid as alsid
	,ltrim(rtrim(alsid)) as alsid_conv
	,cast(alstok as [DECIMAL](38, 4)) as alstok
	,cast(alstok as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as alstok_conv
	,cast(aluom2 as [NVARCHAR](2)) as aluom2
	,cast(album4 as [NVARCHAR](2)) as album4
	,cast(alanby as [DECIMAL](38, 4)) as alanby
	,cast(alancc as [DECIMAL](38, 4)) as alancc
	,cast(alan8 as [DECIMAL](38, 4)) as alan8
	,cast(alshan as [DECIMAL](38, 4)) as alshan
	,aly55wbno as aly55wbno
	,ltrim(rtrim(aly55wbno)) as aly55wbno_conv
	,cast(alldty as [NVARCHAR](2)) as alldty
	,altkid as altkid
	,ltrim(rtrim(altkid)) as altkid_conv
	,cast(altemp as [DECIMAL](38, 4)) as altemp
	,cast(altemp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as altemp_conv
	,cast(aluom3 as [NVARCHAR](2)) as aluom3
	,cast(alstpu as [NVARCHAR](1)) as alstpu
	,cast(alvlc1 as [DECIMAL](38, 4)) as alvlc1
	,cast(alvlc1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alvlc1_conv
	,cast(aldnty as [DECIMAL](38, 4)) as aldnty
	,cast(aldnty as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aldnty_conv
	,cast(aldend as [DECIMAL](38, 4)) as aldend
	,cast(aldend as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as aldend_conv
	,cast(aldntp as [NVARCHAR](1)) as aldntp
	,almetn as almetn
	,ltrim(rtrim(almetn)) as almetn_conv
	,cast(aly55cpnm as [NVARCHAR](10)) as aly55cpnm
	,cast(aly55crdid as [NVARCHAR](10)) as aly55crdid
	,aldsc1 as aldsc1
	,ltrim(rtrim(aldsc1)) as aldsc1_conv
	,alflag as alflag
	,ltrim(rtrim(alflag)) as alflag_conv
	,aly55txt1 as aly55txt1
	,ltrim(rtrim(aly55txt1)) as aly55txt1_conv
	,alev01 as alev01
	,ltrim(rtrim(alev01)) as alev01_conv
	,alev02 as alev02
	,ltrim(rtrim(alev02)) as alev02_conv
	,alev03 as alev03
	,ltrim(rtrim(alev03)) as alev03_conv
	,alev04 as alev04
	,ltrim(rtrim(alev04)) as alev04_conv
	,alev05 as alev05
	,ltrim(rtrim(alev05)) as alev05_conv
	,alkcoo as alkcoo
	,ltrim(rtrim(alkcoo)) as alkcoo_conv
	,cast(aldcto as [NVARCHAR](2)) as aldcto
	,cast(aldoco as [DECIMAL](38, 4)) as aldoco
	,cast(allnid as [DECIMAL](38, 4)) as allnid
	,cast(allnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as allnid_conv
	,alrkco as alrkco
	,ltrim(rtrim(alrkco)) as alrkco_conv
	,cast(alrcto as [NVARCHAR](2)) as alrcto
	,alrorn as alrorn
	,ltrim(rtrim(alrorn)) as alrorn_conv
	,cast(alxlin as [DECIMAL](38, 4)) as alxlin
	,cast(alxlin as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as alxlin_conv
	,alkco as alkco
	,ltrim(rtrim(alkco)) as alkco_conv
	,cast(aldct as [NVARCHAR](2)) as aldct
	,cast(aldoc as [DECIMAL](38, 4)) as aldoc
	,cast(allinn as [DECIMAL](38, 4)) as allinn
	,cast(allinn as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as allinn_conv
	,alokco as alokco
	,ltrim(rtrim(alokco)) as alokco_conv
	,cast(alocto as [NVARCHAR](2)) as alocto
	,aloorn as aloorn
	,ltrim(rtrim(aloorn)) as aloorn_conv
	,cast(alogno as [DECIMAL](38, 4)) as alogno
	,cast(alogno as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as alogno_conv
	,alskco as alskco
	,ltrim(rtrim(alskco)) as alskco_conv
	,cast(alsdct as [NVARCHAR](2)) as alsdct
	,cast(alsdoc as [DECIMAL](38, 4)) as alsdoc
	,cast(alslne as [DECIMAL](38, 4)) as alslne
	,cast(alslne as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as alslne_conv
	,cast(alicu as [DECIMAL](38, 4)) as alicu
	,altd1 as altd1
	,ltrim(rtrim(altd1)) as altd1_conv
	,aluser as aluser
	,ltrim(rtrim(aluser)) as aluser_conv
	,alpid as alpid
	,ltrim(rtrim(alpid)) as alpid_conv
	,aljobn as aljobn
	,ltrim(rtrim(aljobn)) as aljobn_conv
	,cast(alupmj as [INT]) as alupmj
	,case when cast(alupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(alupmj as [INT]) %1000, dateadd(year, cast(alupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as alupmj_conv
	,cast(alupmt as [DECIMAL](38, 4)) as alupmt
	,als1fu as als1fu
	,ltrim(rtrim(als1fu)) as als1fu_conv
	,als2fu as als2fu
	,ltrim(rtrim(als2fu)) as als2fu_conv
	,cast(alfuf9 as [DECIMAL](38, 4)) as alfuf9
	,cast(alfuf9 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as alfuf9_conv
	,cast(alpstm as [DECIMAL](38, 4)) as alpstm
	,alcburst1 as alcburst1
	,ltrim(rtrim(alcburst1)) as alcburst1_conv
	,alcburst2 as alcburst2
	,ltrim(rtrim(alcburst2)) as alcburst2_conv
	,aleaurst1 as aleaurst1
	,ltrim(rtrim(aleaurst1)) as aleaurst1_conv
	,cast(aly55amnt1 as [DECIMAL](38, 4)) as aly55amnt1
	,cast(aly55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aly55amnt1_conv
	,cast(aly55amnt2 as [DECIMAL](38, 4)) as aly55amnt2
	,cast(aly55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as aly55amnt2_conv
	,cast(aly55date1 as [INT]) as aly55date1
	,case when cast(aly55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aly55date1 as [INT]) %1000, dateadd(year, cast(aly55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aly55date1_conv
	,cast(aly55date2 as [INT]) as aly55date2
	,case when cast(aly55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(aly55date2 as [INT]) %1000, dateadd(year, cast(aly55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as aly55date2_conv
	,cast(aly55time1 as [DECIMAL](38, 4)) as aly55time1
	,aly55strg1 as aly55strg1
	,ltrim(rtrim(aly55strg1)) as aly55strg1_conv
	,aly55strg2 as aly55strg2
	,ltrim(rtrim(aly55strg2)) as aly55strg2_conv
	,aly55char1 as aly55char1
	,ltrim(rtrim(aly55char1)) as aly55char1_conv
	,aly55char2 as aly55char2
	,ltrim(rtrim(aly55char2)) as aly55char2_conv
FROM 
    stg_jde.tmp_f554103_init
OPTION ( LABEL = 'CREATE_rep_jde.f554103_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554103_sys_integration_id ON rep_jde.f554103_new(sys_integration_id); 
