-- Create rep new table for init
IF OBJECT_ID('rep_jde.f0012_new') IS NOT NULL
    DROP TABLE rep_jde.f0012_new


CREATE TABLE rep_jde.f0012_new
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
	,kgitem as kgitem
	,ltrim(rtrim(kgitem)) as kgitem_conv
	,kgco as kgco
	,ltrim(rtrim(kgco)) as kgco_conv
	,kgmcu as kgmcu
	,ltrim(rtrim(kgmcu)) as kgmcu_conv
	,kgobj as kgobj
	,ltrim(rtrim(kgobj)) as kgobj_conv
	,kgsub as kgsub
	,ltrim(rtrim(kgsub)) as kgsub_conv
	,kgdl01 as kgdl01
	,ltrim(rtrim(kgdl01)) as kgdl01_conv
	,kgdl02 as kgdl02
	,ltrim(rtrim(kgdl02)) as kgdl02_conv
	,kgdl03 as kgdl03
	,ltrim(rtrim(kgdl03)) as kgdl03_conv
	,kgdl04 as kgdl04
	,ltrim(rtrim(kgdl04)) as kgdl04_conv
	,kgdl05 as kgdl05
	,ltrim(rtrim(kgdl05)) as kgdl05_conv
	,cast(kgmopt as [NVARCHAR](1)) as kgmopt
	,cast(kgoopt as [NVARCHAR](1)) as kgoopt
	,cast(kgsopt as [NVARCHAR](1)) as kgsopt
	,cast(kgsy as [NVARCHAR](4)) as kgsy
	,cast(kgseqn as [DECIMAL](38, 4)) as kgseqn
	,kguser as kguser
	,ltrim(rtrim(kguser)) as kguser_conv
	,kgpid as kgpid
	,ltrim(rtrim(kgpid)) as kgpid_conv
	,cast(kgupmj as [INT]) as kgupmj
	,case when cast(kgupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(kgupmj as [INT]) %1000, dateadd(year, cast(kgupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as kgupmj_conv
	,kgjobn as kgjobn
	,ltrim(rtrim(kgjobn)) as kgjobn_conv
	,cast(kgupmt as [DECIMAL](38, 4)) as kgupmt
FROM 
    stg_jde.tmp_f0012_init
OPTION ( LABEL = 'CREATE_rep_jde.f0012_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f0012_sys_integration_id ON rep_jde.f0012_new(sys_integration_id); 
