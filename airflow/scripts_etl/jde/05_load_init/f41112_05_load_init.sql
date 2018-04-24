-- Create rep new table for init
IF OBJECT_ID('rep_jde.f41112_new') IS NOT NULL
    DROP TABLE rep_jde.f41112_new


CREATE TABLE rep_jde.f41112_new
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
	,cast(indct as [NVARCHAR](2)) as indct
	,cast(infy as [DECIMAL](38, 4)) as infy
	,cast(infy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as infy_conv
	,cast(inctry as [DECIMAL](38, 4)) as inctry
	,cast(initm as [DECIMAL](38, 4)) as initm
	,inlitm as inlitm
	,ltrim(rtrim(inlitm)) as inlitm_conv
	,inaitm as inaitm
	,ltrim(rtrim(inaitm)) as inaitm_conv
	,inmcu as inmcu
	,ltrim(rtrim(inmcu)) as inmcu_conv
	,inlocn as inlocn
	,ltrim(rtrim(inlocn)) as inlocn_conv
	,inlotn as inlotn
	,ltrim(rtrim(inlotn)) as inlotn_conv
	,cast(inglpt as [NVARCHAR](4)) as inglpt
	,cast(inan8 as [DECIMAL](38, 4)) as inan8
	,cast(inshan as [DECIMAL](38, 4)) as inshan
	,cast(innq01 as [DECIMAL](38, 4)) as innq01
	,cast(innq01 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq01_conv
	,cast(innq02 as [DECIMAL](38, 4)) as innq02
	,cast(innq02 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq02_conv
	,cast(innq03 as [DECIMAL](38, 4)) as innq03
	,cast(innq03 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq03_conv
	,cast(innq04 as [DECIMAL](38, 4)) as innq04
	,cast(innq04 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq04_conv
	,cast(innq05 as [DECIMAL](38, 4)) as innq05
	,cast(innq05 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq05_conv
	,cast(innq06 as [DECIMAL](38, 4)) as innq06
	,cast(innq06 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq06_conv
	,cast(innq07 as [DECIMAL](38, 4)) as innq07
	,cast(innq07 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq07_conv
	,cast(innq08 as [DECIMAL](38, 4)) as innq08
	,cast(innq08 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq08_conv
	,cast(innq09 as [DECIMAL](38, 4)) as innq09
	,cast(innq09 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq09_conv
	,cast(innq10 as [DECIMAL](38, 4)) as innq10
	,cast(innq10 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq10_conv
	,cast(innq11 as [DECIMAL](38, 4)) as innq11
	,cast(innq11 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq11_conv
	,cast(innq12 as [DECIMAL](38, 4)) as innq12
	,cast(innq12 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq12_conv
	,cast(innq13 as [DECIMAL](38, 4)) as innq13
	,cast(innq13 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq13_conv
	,cast(innq14 as [DECIMAL](38, 4)) as innq14
	,cast(innq14 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as innq14_conv
	,cast(inan01 as [DECIMAL](38, 4)) as inan01
	,cast(inan01 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan01_conv
	,cast(inan02 as [DECIMAL](38, 4)) as inan02
	,cast(inan02 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan02_conv
	,cast(inan03 as [DECIMAL](38, 4)) as inan03
	,cast(inan03 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan03_conv
	,cast(inan04 as [DECIMAL](38, 4)) as inan04
	,cast(inan04 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan04_conv
	,cast(inan05 as [DECIMAL](38, 4)) as inan05
	,cast(inan05 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan05_conv
	,cast(inan06 as [DECIMAL](38, 4)) as inan06
	,cast(inan06 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan06_conv
	,cast(inan07 as [DECIMAL](38, 4)) as inan07
	,cast(inan07 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan07_conv
	,cast(inan08 as [DECIMAL](38, 4)) as inan08
	,cast(inan08 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan08_conv
	,cast(inan09 as [DECIMAL](38, 4)) as inan09
	,cast(inan09 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan09_conv
	,cast(inan10 as [DECIMAL](38, 4)) as inan10
	,cast(inan10 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan10_conv
	,cast(inan11 as [DECIMAL](38, 4)) as inan11
	,cast(inan11 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan11_conv
	,cast(inan12 as [DECIMAL](38, 4)) as inan12
	,cast(inan12 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan12_conv
	,cast(inan13 as [DECIMAL](38, 4)) as inan13
	,cast(inan13 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan13_conv
	,cast(inan14 as [DECIMAL](38, 4)) as inan14
	,cast(inan14 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as inan14_conv
	,cast(incuma as [DECIMAL](38, 4)) as incuma
	,cast(incuma as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as incuma_conv
	,cast(incmqt as [DECIMAL](38, 4)) as incmqt
	,cast(incmqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as incmqt_conv
	,inaisl as inaisl
	,ltrim(rtrim(inaisl)) as inaisl_conv
	,inbin as inbin
	,ltrim(rtrim(inbin)) as inbin_conv
	,cast(intday as [DECIMAL](38, 4)) as intday
	,inuser as inuser
	,ltrim(rtrim(inuser)) as inuser_conv
	,inpid as inpid
	,ltrim(rtrim(inpid)) as inpid_conv
	,cast(inupmj as [INT]) as inupmj
	,case when cast(inupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(inupmj as [INT]) %1000, dateadd(year, cast(inupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as inupmj_conv
	,injobn as injobn
	,ltrim(rtrim(injobn)) as injobn_conv
FROM 
    stg_jde.tmp_f41112_init
OPTION ( LABEL = 'CREATE_rep_jde.f41112_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f41112_sys_integration_id ON rep_jde.f41112_new(sys_integration_id); 
