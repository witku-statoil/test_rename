-- Create rep new table for init
IF OBJECT_ID('rep_jde.f4906_new') IS NOT NULL
    DROP TABLE rep_jde.f4906_new


CREATE TABLE rep_jde.f4906_new
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
	,cast(cmcars as [DECIMAL](38, 4)) as cmcars
	,cast(cmcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as cmcars_conv
	,cmscac as cmscac
	,ltrim(rtrim(cmscac)) as cmscac_conv
	,cmcamd as cmcamd
	,ltrim(rtrim(cmcamd)) as cmcamd_conv
	,cmstbf as cmstbf
	,ltrim(rtrim(cmstbf)) as cmstbf_conv
	,cast(cmstft as [NVARCHAR](3)) as cmstft
	,cast(cmrfq1 as [NVARCHAR](2)) as cmrfq1
	,cast(cmrfq2 as [NVARCHAR](2)) as cmrfq2
	,cast(cmrndn as [NVARCHAR](1)) as cmrndn
	,cast(cmdwfc as [DECIMAL](38, 4)) as cmdwfc
	,cast(cmdwfc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as cmdwfc_conv
	,cmrsla as cmrsla
	,ltrim(rtrim(cmrsla)) as cmrsla_conv
	,cmpfsd as cmpfsd
	,ltrim(rtrim(cmpfsd)) as cmpfsd_conv
	,cast(cmprfm as [DECIMAL](38, 4)) as cmprfm
	,cast(cmprfm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as cmprfm_conv
	,cmuser as cmuser
	,ltrim(rtrim(cmuser)) as cmuser_conv
	,cmpid as cmpid
	,ltrim(rtrim(cmpid)) as cmpid_conv
	,cmjobn as cmjobn
	,ltrim(rtrim(cmjobn)) as cmjobn_conv
	,cast(cmupmj as [INT]) as cmupmj
	,case when cast(cmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(cmupmj as [INT]) %1000, dateadd(year, cast(cmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as cmupmj_conv
	,cast(cmtday as [DECIMAL](38, 4)) as cmtday
FROM 
    stg_jde.tmp_f4906_init
OPTION ( LABEL = 'CREATE_rep_jde.f4906_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4906_sys_integration_id ON rep_jde.f4906_new(sys_integration_id); 
