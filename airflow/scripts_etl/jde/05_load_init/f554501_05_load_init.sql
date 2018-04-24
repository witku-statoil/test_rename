-- Create rep new table for init
IF OBJECT_ID('rep_jde.f554501_new') IS NOT NULL
    DROP TABLE rep_jde.f554501_new


CREATE TABLE rep_jde.f554501_new
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
	,cast(qmukid as [DECIMAL](38, 4)) as qmukid
	,qmy55qn as qmy55qn
	,ltrim(rtrim(qmy55qn)) as qmy55qn_conv
	,qmdsc1 as qmdsc1
	,ltrim(rtrim(qmdsc1)) as qmdsc1_conv
	,cast(qmy55qs as [NVARCHAR](2)) as qmy55qs
	,cast(qmy55qt as [NVARCHAR](2)) as qmy55qt
	,qmy55jdqn as qmy55jdqn
	,ltrim(rtrim(qmy55jdqn)) as qmy55jdqn_conv
	,qmdsc2 as qmdsc2
	,ltrim(rtrim(qmdsc2)) as qmdsc2_conv
	,qmcrcd as qmcrcd
	,ltrim(rtrim(qmcrcd)) as qmcrcd_conv
	,cast(qmuom as [NVARCHAR](2)) as qmuom
	,cast(qmdend as [DECIMAL](38, 4)) as qmdend
	,cast(qmdend as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as qmdend_conv
	,cast(qmdntp as [NVARCHAR](1)) as qmdntp
	,cast(qmdte as [INT]) as qmdte
	,case when cast(qmdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(qmdte as [INT]) %1000, dateadd(year, cast(qmdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as qmdte_conv
	,cast(qmy55avin as [NVARCHAR](2)) as qmy55avin
	,qmco as qmco
	,ltrim(rtrim(qmco)) as qmco_conv
	,qmmcu as qmmcu
	,ltrim(rtrim(qmmcu)) as qmmcu_conv
	,cast(qmprp4 as [NVARCHAR](3)) as qmprp4
	,qmcomments as qmcomments
	,ltrim(rtrim(qmcomments)) as qmcomments_conv
	,qmflag as qmflag
	,ltrim(rtrim(qmflag)) as qmflag_conv
	,qmurrf as qmurrf
	,ltrim(rtrim(qmurrf)) as qmurrf_conv
	,qmurcd as qmurcd
	,ltrim(rtrim(qmurcd)) as qmurcd_conv
	,cast(qmurab as [DECIMAL](38, 4)) as qmurab
	,cast(qmurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as qmurab_conv
	,cast(qmurat as [DECIMAL](38, 4)) as qmurat
	,cast(qmurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as qmurat_conv
	,cast(qmurdt as [INT]) as qmurdt
	,case when cast(qmurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(qmurdt as [INT]) %1000, dateadd(year, cast(qmurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as qmurdt_conv
	,qmuser as qmuser
	,ltrim(rtrim(qmuser)) as qmuser_conv
	,qmpid as qmpid
	,ltrim(rtrim(qmpid)) as qmpid_conv
	,qmjobn as qmjobn
	,ltrim(rtrim(qmjobn)) as qmjobn_conv
	,cast(qmupmt as [DECIMAL](38, 4)) as qmupmt
	,cast(qmupmj as [INT]) as qmupmj
	,case when cast(qmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(qmupmj as [INT]) %1000, dateadd(year, cast(qmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as qmupmj_conv
	,qmev01 as qmev01
	,ltrim(rtrim(qmev01)) as qmev01_conv
	,qmev02 as qmev02
	,ltrim(rtrim(qmev02)) as qmev02_conv
	,qmev03 as qmev03
	,ltrim(rtrim(qmev03)) as qmev03_conv
	,qmev04 as qmev04
	,ltrim(rtrim(qmev04)) as qmev04_conv
	,qmy55char1 as qmy55char1
	,ltrim(rtrim(qmy55char1)) as qmy55char1_conv
	,qmy55char2 as qmy55char2
	,ltrim(rtrim(qmy55char2)) as qmy55char2_conv
	,cast(qmy55date1 as [INT]) as qmy55date1
	,case when cast(qmy55date1 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(qmy55date1 as [INT]) %1000, dateadd(year, cast(qmy55date1 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as qmy55date1_conv
	,cast(qmy55date2 as [INT]) as qmy55date2
	,case when cast(qmy55date2 as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(qmy55date2 as [INT]) %1000, dateadd(year, cast(qmy55date2 as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as qmy55date2_conv
	,qmy55strg1 as qmy55strg1
	,ltrim(rtrim(qmy55strg1)) as qmy55strg1_conv
	,qmy55strg2 as qmy55strg2
	,ltrim(rtrim(qmy55strg2)) as qmy55strg2_conv
	,cast(qmy55time1 as [DECIMAL](38, 4)) as qmy55time1
	,cast(qmy55time2 as [DECIMAL](38, 4)) as qmy55time2
	,cast(qmy55amnt1 as [DECIMAL](38, 4)) as qmy55amnt1
	,cast(qmy55amnt1 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as qmy55amnt1_conv
	,cast(qmy55amnt2 as [DECIMAL](38, 4)) as qmy55amnt2
	,cast(qmy55amnt2 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as qmy55amnt2_conv
FROM 
    stg_jde.tmp_f554501_init
OPTION ( LABEL = 'CREATE_rep_jde.f554501_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f554501_sys_integration_id ON rep_jde.f554501_new(sys_integration_id); 
