-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f0902_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f0902_new


CREATE TABLE rep_jde_qat.f0902_new
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
	,gbaid as gbaid
	,ltrim(rtrim(gbaid)) as gbaid_conv
	,cast(gbctry as [DECIMAL](38, 4)) as gbctry
	,cast(gbfy as [DECIMAL](38, 4)) as gbfy
	,cast(gbfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as gbfy_conv
	,cast(gbfq as [NVARCHAR](4)) as gbfq
	,cast(gblt as [NVARCHAR](2)) as gblt
	,gbsbl as gbsbl
	,ltrim(rtrim(gbsbl)) as gbsbl_conv
	,gbco as gbco
	,ltrim(rtrim(gbco)) as gbco_conv
	,cast(gbapyc as [DECIMAL](38, 4)) as gbapyc
	,cast(gbapyc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbapyc_conv
	,cast(gban01 as [DECIMAL](38, 4)) as gban01
	,cast(gban01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban01_conv
	,cast(gban02 as [DECIMAL](38, 4)) as gban02
	,cast(gban02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban02_conv
	,cast(gban03 as [DECIMAL](38, 4)) as gban03
	,cast(gban03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban03_conv
	,cast(gban04 as [DECIMAL](38, 4)) as gban04
	,cast(gban04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban04_conv
	,cast(gban05 as [DECIMAL](38, 4)) as gban05
	,cast(gban05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban05_conv
	,cast(gban06 as [DECIMAL](38, 4)) as gban06
	,cast(gban06 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban06_conv
	,cast(gban07 as [DECIMAL](38, 4)) as gban07
	,cast(gban07 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban07_conv
	,cast(gban08 as [DECIMAL](38, 4)) as gban08
	,cast(gban08 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban08_conv
	,cast(gban09 as [DECIMAL](38, 4)) as gban09
	,cast(gban09 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban09_conv
	,cast(gban10 as [DECIMAL](38, 4)) as gban10
	,cast(gban10 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban10_conv
	,cast(gban11 as [DECIMAL](38, 4)) as gban11
	,cast(gban11 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban11_conv
	,cast(gban12 as [DECIMAL](38, 4)) as gban12
	,cast(gban12 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban12_conv
	,cast(gban13 as [DECIMAL](38, 4)) as gban13
	,cast(gban13 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban13_conv
	,cast(gban14 as [DECIMAL](38, 4)) as gban14
	,cast(gban14 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gban14_conv
	,cast(gbapyn as [DECIMAL](38, 4)) as gbapyn
	,cast(gbapyn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbapyn_conv
	,cast(gbawtd as [DECIMAL](38, 4)) as gbawtd
	,cast(gbawtd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbawtd_conv
	,cast(gbborg as [DECIMAL](38, 4)) as gbborg
	,cast(gbborg as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbborg_conv
	,cast(gbpou as [DECIMAL](38, 4)) as gbpou
	,cast(gbpou as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbpou_conv
	,cast(gbpc as [DECIMAL](38, 4)) as gbpc
	,cast(gbtker as [DECIMAL](38, 4)) as gbtker
	,cast(gbtker as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbtker_conv
	,cast(gbbreq as [DECIMAL](38, 4)) as gbbreq
	,cast(gbbreq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbbreq_conv
	,cast(gbbapr as [DECIMAL](38, 4)) as gbbapr
	,cast(gbbapr as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gbbapr_conv
	,gbmcu as gbmcu
	,ltrim(rtrim(gbmcu)) as gbmcu_conv
	,gbobj as gbobj
	,ltrim(rtrim(gbobj)) as gbobj_conv
	,gbsub as gbsub
	,ltrim(rtrim(gbsub)) as gbsub_conv
	,gbuser as gbuser
	,ltrim(rtrim(gbuser)) as gbuser_conv
	,gbpid as gbpid
	,ltrim(rtrim(gbpid)) as gbpid_conv
	,cast(gbupmj as [INT]) as gbupmj
	,case when cast(gbupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(gbupmj as [INT]) %1000, dateadd(year, cast(gbupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as gbupmj_conv
	,gbjobn as gbjobn
	,ltrim(rtrim(gbjobn)) as gbjobn_conv
	,cast(gbsblt as [NVARCHAR](1)) as gbsblt
	,cast(gbupmt as [DECIMAL](38, 4)) as gbupmt
	,gbcrcd as gbcrcd
	,ltrim(rtrim(gbcrcd)) as gbcrcd_conv
	,gbcrcx as gbcrcx
	,ltrim(rtrim(gbcrcx)) as gbcrcx_conv
	,gbprgf as gbprgf
	,ltrim(rtrim(gbprgf)) as gbprgf_conv
	,cast(gband01 as [DECIMAL](38, 4)) as gband01
	,cast(gband01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband01_conv
	,cast(gband02 as [DECIMAL](38, 4)) as gband02
	,cast(gband02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband02_conv
	,cast(gband03 as [DECIMAL](38, 4)) as gband03
	,cast(gband03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband03_conv
	,cast(gband04 as [DECIMAL](38, 4)) as gband04
	,cast(gband04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband04_conv
	,cast(gband05 as [DECIMAL](38, 4)) as gband05
	,cast(gband05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband05_conv
	,cast(gband06 as [DECIMAL](38, 4)) as gband06
	,cast(gband06 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband06_conv
	,cast(gband07 as [DECIMAL](38, 4)) as gband07
	,cast(gband07 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband07_conv
	,cast(gband08 as [DECIMAL](38, 4)) as gband08
	,cast(gband08 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband08_conv
	,cast(gband09 as [DECIMAL](38, 4)) as gband09
	,cast(gband09 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband09_conv
	,cast(gband10 as [DECIMAL](38, 4)) as gband10
	,cast(gband10 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband10_conv
	,cast(gband11 as [DECIMAL](38, 4)) as gband11
	,cast(gband11 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband11_conv
	,cast(gband12 as [DECIMAL](38, 4)) as gband12
	,cast(gband12 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband12_conv
	,cast(gband13 as [DECIMAL](38, 4)) as gband13
	,cast(gband13 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband13_conv
	,cast(gband14 as [DECIMAL](38, 4)) as gband14
	,cast(gband14 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as gband14_conv
FROM 
    stg_jde_qat.tmp_f0902_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f0902_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0902_sys_integration_id ON rep_jde_qat.f0902_new(sys_integration_id); 
