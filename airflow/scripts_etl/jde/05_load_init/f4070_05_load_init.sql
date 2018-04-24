-- Create rep new table for init
IF OBJECT_ID('rep_jde.f4070_new') IS NOT NULL
    DROP TABLE rep_jde.f4070_new


CREATE TABLE rep_jde.f4070_new
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
	,cast(snasn as [NVARCHAR](8)) as snasn
	,cast(snoseq as [DECIMAL](38, 4)) as snoseq
	,cast(snoseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as snoseq_conv
	,cast(snanps as [DECIMAL](38, 4)) as snanps
	,cast(snanps as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as snanps_conv
	,cast(snast as [NVARCHAR](8)) as snast
	,snurcd as snurcd
	,ltrim(rtrim(snurcd)) as snurcd_conv
	,cast(snurdt as [INT]) as snurdt
	,case when cast(snurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(snurdt as [INT]) %1000, dateadd(year, cast(snurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as snurdt_conv
	,cast(snurat as [DECIMAL](38, 4)) as snurat
	,cast(snurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as snurat_conv
	,cast(snurab as [DECIMAL](38, 4)) as snurab
	,cast(snurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as snurab_conv
	,snurrf as snurrf
	,ltrim(rtrim(snurrf)) as snurrf_conv
	,cast(sneftj as [INT]) as sneftj
	,case when cast(sneftj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(sneftj as [INT]) %1000, dateadd(year, cast(sneftj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as sneftj_conv
	,cast(snexdj as [INT]) as snexdj
	,case when cast(snexdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(snexdj as [INT]) %1000, dateadd(year, cast(snexdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as snexdj_conv
	,sninht as sninht
	,ltrim(rtrim(sninht)) as sninht_conv
	,cast(sntier as [DECIMAL](38, 4)) as sntier
	,snuser as snuser
	,ltrim(rtrim(snuser)) as snuser_conv
	,snpid as snpid
	,ltrim(rtrim(snpid)) as snpid_conv
	,snjobn as snjobn
	,ltrim(rtrim(snjobn)) as snjobn_conv
	,cast(snupmj as [INT]) as snupmj
	,case when cast(snupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(snupmj as [INT]) %1000, dateadd(year, cast(snupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as snupmj_conv
	,cast(sntday as [DECIMAL](38, 4)) as sntday
	,cast(snsctype as [DECIMAL](38, 4)) as snsctype
	,cast(snsctype as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as snsctype_conv
	,snstprcf as snstprcf
	,ltrim(rtrim(snstprcf)) as snstprcf_conv
	,cast(snskipto as [DECIMAL](38, 4)) as snskipto
	,cast(snskipto as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as snskipto_conv
	,snskipend as snskipend
	,ltrim(rtrim(snskipend)) as snskipend_conv
FROM 
    stg_jde.tmp_f4070_init
OPTION ( LABEL = 'CREATE_rep_jde.f4070_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f4070_sys_integration_id ON rep_jde.f4070_new(sys_integration_id); 
