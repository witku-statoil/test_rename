-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4229_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4229_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4229_cdc
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
	,cast(ssdcto as [NVARCHAR](2)) as ssdcto
	,sslnty as sslnty
	,ltrim(rtrim(sslnty)) as sslnty_conv
	,cast(ssan8 as [DECIMAL](38, 4)) as ssan8
	,cast(ssitm as [DECIMAL](38, 4)) as ssitm
	,sslitm as sslitm
	,ltrim(rtrim(sslitm)) as sslitm_conv
	,ssaitm as ssaitm
	,ltrim(rtrim(ssaitm)) as ssaitm_conv
	,ssmcu as ssmcu
	,ltrim(rtrim(ssmcu)) as ssmcu_conv
	,cast(sssrp1 as [NVARCHAR](3)) as sssrp1
	,cast(sssrp2 as [NVARCHAR](3)) as sssrp2
	,cast(sssrp3 as [NVARCHAR](3)) as sssrp3
	,cast(sssrp4 as [NVARCHAR](3)) as sssrp4
	,cast(sssrp5 as [NVARCHAR](3)) as sssrp5
	,cast(ssctry as [DECIMAL](38, 4)) as ssctry
	,cast(ssfy as [DECIMAL](38, 4)) as ssfy
	,cast(ssfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ssfy_conv
	,cast(ssfq as [NVARCHAR](4)) as ssfq
	,cast(sscms as [DECIMAL](38, 4)) as sscms
	,cast(sscms as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sscms_conv
	,cast(sscmc as [DECIMAL](38, 4)) as sscmc
	,cast(sscmc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sscmc_conv
	,cast(sscys as [DECIMAL](38, 4)) as sscys
	,cast(sscys as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sscys_conv
	,cast(sscyc as [DECIMAL](38, 4)) as sscyc
	,cast(sscyc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sscyc_conv
	,cast(sspyes as [DECIMAL](38, 4)) as sspyes
	,cast(sspyes as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sspyes_conv
	,cast(sspyec as [DECIMAL](38, 4)) as sspyec
	,cast(sspyec as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as sspyec_conv
	,cast(ssqs01 as [DECIMAL](38, 4)) as ssqs01
	,cast(ssqs01 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs01_conv
	,cast(ssqs02 as [DECIMAL](38, 4)) as ssqs02
	,cast(ssqs02 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs02_conv
	,cast(ssqs03 as [DECIMAL](38, 4)) as ssqs03
	,cast(ssqs03 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs03_conv
	,cast(ssqs04 as [DECIMAL](38, 4)) as ssqs04
	,cast(ssqs04 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs04_conv
	,cast(ssqs05 as [DECIMAL](38, 4)) as ssqs05
	,cast(ssqs05 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs05_conv
	,cast(ssqs06 as [DECIMAL](38, 4)) as ssqs06
	,cast(ssqs06 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs06_conv
	,cast(ssqs07 as [DECIMAL](38, 4)) as ssqs07
	,cast(ssqs07 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs07_conv
	,cast(ssqs08 as [DECIMAL](38, 4)) as ssqs08
	,cast(ssqs08 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs08_conv
	,cast(ssqs09 as [DECIMAL](38, 4)) as ssqs09
	,cast(ssqs09 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs09_conv
	,cast(ssqs10 as [DECIMAL](38, 4)) as ssqs10
	,cast(ssqs10 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs10_conv
	,cast(ssqs11 as [DECIMAL](38, 4)) as ssqs11
	,cast(ssqs11 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs11_conv
	,cast(ssqs12 as [DECIMAL](38, 4)) as ssqs12
	,cast(ssqs12 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs12_conv
	,cast(ssqs13 as [DECIMAL](38, 4)) as ssqs13
	,cast(ssqs13 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs13_conv
	,cast(ssqs14 as [DECIMAL](38, 4)) as ssqs14
	,cast(ssqs14 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ssqs14_conv
	,cast(ssci01 as [DECIMAL](38, 4)) as ssci01
	,cast(ssci01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci01_conv
	,cast(ssci02 as [DECIMAL](38, 4)) as ssci02
	,cast(ssci02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci02_conv
	,cast(ssci03 as [DECIMAL](38, 4)) as ssci03
	,cast(ssci03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci03_conv
	,cast(ssci04 as [DECIMAL](38, 4)) as ssci04
	,cast(ssci04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci04_conv
	,cast(ssci05 as [DECIMAL](38, 4)) as ssci05
	,cast(ssci05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci05_conv
	,cast(ssci06 as [DECIMAL](38, 4)) as ssci06
	,cast(ssci06 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci06_conv
	,cast(ssci07 as [DECIMAL](38, 4)) as ssci07
	,cast(ssci07 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci07_conv
	,cast(ssci08 as [DECIMAL](38, 4)) as ssci08
	,cast(ssci08 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci08_conv
	,cast(ssci09 as [DECIMAL](38, 4)) as ssci09
	,cast(ssci09 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci09_conv
	,cast(ssci10 as [DECIMAL](38, 4)) as ssci10
	,cast(ssci10 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci10_conv
	,cast(ssci11 as [DECIMAL](38, 4)) as ssci11
	,cast(ssci11 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci11_conv
	,cast(ssci12 as [DECIMAL](38, 4)) as ssci12
	,cast(ssci12 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci12_conv
	,cast(ssci13 as [DECIMAL](38, 4)) as ssci13
	,cast(ssci13 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci13_conv
	,cast(ssci14 as [DECIMAL](38, 4)) as ssci14
	,cast(ssci14 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssci14_conv
	,cast(ssas01 as [DECIMAL](38, 4)) as ssas01
	,cast(ssas01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas01_conv
	,cast(ssas02 as [DECIMAL](38, 4)) as ssas02
	,cast(ssas02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas02_conv
	,cast(ssas03 as [DECIMAL](38, 4)) as ssas03
	,cast(ssas03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas03_conv
	,cast(ssas04 as [DECIMAL](38, 4)) as ssas04
	,cast(ssas04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas04_conv
	,cast(ssas05 as [DECIMAL](38, 4)) as ssas05
	,cast(ssas05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas05_conv
	,cast(ssas06 as [DECIMAL](38, 4)) as ssas06
	,cast(ssas06 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas06_conv
	,cast(ssas07 as [DECIMAL](38, 4)) as ssas07
	,cast(ssas07 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas07_conv
	,cast(ssas08 as [DECIMAL](38, 4)) as ssas08
	,cast(ssas08 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas08_conv
	,cast(ssas09 as [DECIMAL](38, 4)) as ssas09
	,cast(ssas09 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas09_conv
	,cast(ssas10 as [DECIMAL](38, 4)) as ssas10
	,cast(ssas10 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas10_conv
	,cast(ssas11 as [DECIMAL](38, 4)) as ssas11
	,cast(ssas11 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas11_conv
	,cast(ssas12 as [DECIMAL](38, 4)) as ssas12
	,cast(ssas12 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas12_conv
	,cast(ssas13 as [DECIMAL](38, 4)) as ssas13
	,cast(ssas13 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas13_conv
	,cast(ssas14 as [DECIMAL](38, 4)) as ssas14
	,cast(ssas14 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ssas14_conv
	,cast(ssdlij as [INT]) as ssdlij
	,case when cast(ssdlij as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ssdlij as [INT]) %1000, dateadd(year, cast(ssdlij as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ssdlij_conv
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
	,ssdcto
	,sslnty
	,ssan8
	,ssitm
	,sslitm
	,ssaitm
	,ssmcu
	,sssrp1
	,sssrp2
	,sssrp3
	,sssrp4
	,sssrp5
	,ssctry
	,ssfy
	,ssfq
	,sscms
	,sscmc
	,sscys
	,sscyc
	,sspyes
	,sspyec
	,ssqs01
	,ssqs02
	,ssqs03
	,ssqs04
	,ssqs05
	,ssqs06
	,ssqs07
	,ssqs08
	,ssqs09
	,ssqs10
	,ssqs11
	,ssqs12
	,ssqs13
	,ssqs14
	,ssci01
	,ssci02
	,ssci03
	,ssci04
	,ssci05
	,ssci06
	,ssci07
	,ssci08
	,ssci09
	,ssci10
	,ssci11
	,ssci12
	,ssci13
	,ssci14
	,ssas01
	,ssas02
	,ssas03
	,ssas04
	,ssas05
	,ssas06
	,ssas07
	,ssas08
	,ssas09
	,ssas10
	,ssas11
	,ssas12
	,ssas13
	,ssas14
	,ssdlij,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4229_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4229_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4229_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4229_cdc(sys_integration_id); 
