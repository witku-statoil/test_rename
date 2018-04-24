-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f41011_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f41011_new


CREATE TABLE rep_jde_qat.f41011_new
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
	,cast(pkitm as [DECIMAL](38, 4)) as pkitm
	,cast(pkpdgr as [NVARCHAR](3)) as pkpdgr
	,cast(pkdsgp as [NVARCHAR](3)) as pkdsgp
	,cast(pkdntb as [NVARCHAR](4)) as pkdntb
	,cast(pkstcn as [NVARCHAR](4)) as pkstcn
	,cast(pkrptm as [NVARCHAR](4)) as pkrptm
	,pkrqtc as pkrqtc
	,ltrim(rtrim(pkrqtc)) as pkrqtc_conv
	,pklpgp as pklpgp
	,ltrim(rtrim(pklpgp)) as pklpgp_conv
	,pkcavp as pkcavp
	,ltrim(rtrim(pkcavp)) as pkcavp_conv
	,cast(pkdnty as [DECIMAL](38, 4)) as pkdnty
	,cast(pkdnty as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pkdnty_conv
	,cast(pkdntp as [NVARCHAR](1)) as pkdntp
	,cast(pkdetp as [DECIMAL](38, 4)) as pkdetp
	,cast(pkdetp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pkdetp_conv
	,cast(pkdtpu as [NVARCHAR](1)) as pkdtpu
	,cast(pkcoex as [DECIMAL](38, 4)) as pkcoex
	,cast(pkcoex as [DECIMAL](38, 5)) / cast(100000 as [DECIMAL](38, 5)) as pkcoex_conv
	,cast(pktmmn as [DECIMAL](38, 4)) as pktmmn
	,cast(pktmmn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pktmmn_conv
	,cast(pktpum as [NVARCHAR](1)) as pktpum
	,cast(pktmmx as [DECIMAL](38, 4)) as pktmmx
	,cast(pktmmx as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pktmmx_conv
	,cast(pktpux as [NVARCHAR](1)) as pktpux
	,cast(pkdsmn as [DECIMAL](38, 4)) as pkdsmn
	,cast(pkdsmn as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pkdsmn_conv
	,cast(pkdntm as [NVARCHAR](1)) as pkdntm
	,cast(pkdnmx as [DECIMAL](38, 4)) as pkdnmx
	,cast(pkdnmx as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pkdnmx_conv
	,cast(pkdntx as [NVARCHAR](1)) as pkdntx
	,cast(pklpgv as [DECIMAL](38, 4)) as pklpgv
	,cast(pklpgv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pklpgv_conv
	,cast(pktpu1 as [NVARCHAR](1)) as pktpu1
	,cast(pkmnvc as [DECIMAL](38, 4)) as pkmnvc
	,cast(pkmnvc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pkmnvc_conv
	,cast(pkmxvc as [DECIMAL](38, 4)) as pkmxvc
	,cast(pkmxvc as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pkmxvc_conv
	,pkurcd as pkurcd
	,ltrim(rtrim(pkurcd)) as pkurcd_conv
	,cast(pkurat as [DECIMAL](38, 4)) as pkurat
	,cast(pkurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pkurat_conv
	,cast(pkurab as [DECIMAL](38, 4)) as pkurab
	,cast(pkurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pkurab_conv
	,pkurrf as pkurrf
	,ltrim(rtrim(pkurrf)) as pkurrf_conv
	,cast(pkurdt as [INT]) as pkurdt
	,case when cast(pkurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pkurdt as [INT]) %1000, dateadd(year, cast(pkurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pkurdt_conv
	,pkuser as pkuser
	,ltrim(rtrim(pkuser)) as pkuser_conv
	,pkpid as pkpid
	,ltrim(rtrim(pkpid)) as pkpid_conv
	,pkjobn as pkjobn
	,ltrim(rtrim(pkjobn)) as pkjobn_conv
	,cast(pkupmj as [INT]) as pkupmj
	,case when cast(pkupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(pkupmj as [INT]) %1000, dateadd(year, cast(pkupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as pkupmj_conv
	,cast(pktday as [DECIMAL](38, 4)) as pktday
	,cast(pkrtob as [NVARCHAR](1)) as pkrtob
FROM 
    stg_jde_qat.tmp_f41011_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f41011_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f41011_sys_integration_id ON rep_jde_qat.f41011_new(sys_integration_id); 
