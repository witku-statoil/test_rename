-- Create rep new table for init
IF OBJECT_ID('rep_jde.f41500_new') IS NOT NULL
    DROP TABLE rep_jde.f41500_new


CREATE TABLE rep_jde.f41500_new
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
	,ppmcu as ppmcu
	,ltrim(rtrim(ppmcu)) as ppmcu_conv
	,pptkid as pptkid
	,ltrim(rtrim(pptkid)) as pptkid_conv
	,ppdl01 as ppdl01
	,ltrim(rtrim(ppdl01)) as ppdl01_conv
	,cast(pptklo as [NVARCHAR](3)) as pptklo
	,cast(pptuse as [NVARCHAR](3)) as pptuse
	,cast(pptkty as [NVARCHAR](3)) as pptkty
	,cast(pptkcp as [DECIMAL](38, 4)) as pptkcp
	,cast(pptkcp as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pptkcp_conv
	,cast(ppbum1 as [NVARCHAR](2)) as ppbum1
	,pphttk as pphttk
	,ltrim(rtrim(pphttk)) as pphttk_conv
	,ppprez as ppprez
	,ltrim(rtrim(ppprez)) as ppprez_conv
	,cast(ppidte as [INT]) as ppidte
	,case when cast(ppidte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ppidte as [INT]) %1000, dateadd(year, cast(ppidte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ppidte_conv
	,cast(ppdtcl as [INT]) as ppdtcl
	,case when cast(ppdtcl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ppdtcl as [INT]) %1000, dateadd(year, cast(ppdtcl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ppdtcl_conv
	,cast(ppstrp as [NVARCHAR](2)) as ppstrp
	,cast(ppdipt as [NVARCHAR](1)) as ppdipt
	,cast(ppgamt as [NVARCHAR](1)) as ppgamt
	,cast(ppfltr as [NVARCHAR](1)) as ppfltr
	,cast(pptexp as [DECIMAL](38, 4)) as pptexp
	,cast(pptexp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pptexp_conv
	,cast(pptstu as [NVARCHAR](1)) as pptstu
	,ppcutk as ppcutk
	,ltrim(rtrim(ppcutk)) as ppcutk_conv
	,cast(ppscom as [NVARCHAR](1)) as ppscom
	,cast(pptdia as [DECIMAL](38, 4)) as pptdia
	,cast(pptdia as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pptdia_conv
	,cast(ppbum2 as [NVARCHAR](2)) as ppbum2
	,cast(ppthgt as [DECIMAL](38, 4)) as ppthgt
	,cast(ppthgt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ppthgt_conv
	,cast(ppbum3 as [NVARCHAR](2)) as ppbum3
	,cast(ppstem as [DECIMAL](38, 4)) as ppstem
	,cast(ppstem as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ppstem_conv
	,cast(ppstpu as [NVARCHAR](1)) as ppstpu
	,cast(pprefh as [DECIMAL](38, 4)) as pprefh
	,cast(pprefh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pprefh_conv
	,cast(ppbum4 as [NVARCHAR](2)) as ppbum4
	,cast(pprwgh as [DECIMAL](38, 4)) as pprwgh
	,cast(pprwgh as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as pprwgh_conv
	,cast(ppbum5 as [NVARCHAR](2)) as ppbum5
	,cast(ppflht as [DECIMAL](38, 4)) as ppflht
	,cast(ppflht as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ppflht_conv
	,cast(ppbum6 as [NVARCHAR](2)) as ppbum6
	,cast(ppunpv as [DECIMAL](38, 4)) as ppunpv
	,cast(ppunpv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ppunpv_conv
	,cast(ppbum7 as [NVARCHAR](2)) as ppbum7
	,cast(pppipv as [DECIMAL](38, 4)) as pppipv
	,cast(pppipv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pppipv_conv
	,cast(ppbum8 as [NVARCHAR](2)) as ppbum8
	,cast(ppdisv as [DECIMAL](38, 4)) as ppdisv
	,cast(ppdisv as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ppdisv_conv
	,cast(ppbum9 as [NVARCHAR](2)) as ppbum9
	,cast(ppdihr as [DECIMAL](38, 4)) as ppdihr
	,cast(ppdihr as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ppdihr_conv
	,cast(ppdhru as [NVARCHAR](2)) as ppdhru
	,cast(ppfirh as [DECIMAL](38, 4)) as ppfirh
	,cast(ppfirh as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ppfirh_conv
	,cast(ppfrhu as [NVARCHAR](2)) as ppfrhu
	,cast(pplswn as [DECIMAL](38, 4)) as pplswn
	,cast(pplswn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as pplswn_conv
	,cast(ppbum0 as [NVARCHAR](2)) as ppbum0
	,cast(pplosc as [DECIMAL](38, 4)) as pplosc
	,cast(pplosc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pplosc_conv
	,cast(pphisc as [DECIMAL](38, 4)) as pphisc
	,cast(pphisc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pphisc_conv
	,cast(ppitm as [DECIMAL](38, 4)) as ppitm
	,cast(ppitml as [DECIMAL](38, 4)) as ppitml
	,pppcsd as pppcsd
	,ltrim(rtrim(pppcsd)) as pppcsd_conv
	,cast(pprtob as [NVARCHAR](1)) as pprtob
	,ppurcd as ppurcd
	,ltrim(rtrim(ppurcd)) as ppurcd_conv
	,cast(ppurat as [DECIMAL](38, 4)) as ppurat
	,cast(ppurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ppurat_conv
	,cast(ppurab as [DECIMAL](38, 4)) as ppurab
	,cast(ppurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ppurab_conv
	,ppurrf as ppurrf
	,ltrim(rtrim(ppurrf)) as ppurrf_conv
	,cast(ppurdt as [INT]) as ppurdt
	,case when cast(ppurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ppurdt as [INT]) %1000, dateadd(year, cast(ppurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ppurdt_conv
	,ppuser as ppuser
	,ltrim(rtrim(ppuser)) as ppuser_conv
	,pppid as pppid
	,ltrim(rtrim(pppid)) as pppid_conv
	,ppjobn as ppjobn
	,ltrim(rtrim(ppjobn)) as ppjobn_conv
	,cast(ppupmj as [INT]) as ppupmj
	,case when cast(ppupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ppupmj as [INT]) %1000, dateadd(year, cast(ppupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ppupmj_conv
	,cast(pptday as [DECIMAL](38, 4)) as pptday
	,cast(pptnkn as [DECIMAL](38, 4)) as pptnkn
	,cast(pptnkn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as pptnkn_conv
	,cast(ppexpf as [DECIMAL](38, 4)) as ppexpf
	,cast(ppexpf as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as ppexpf_conv
FROM 
    stg_jde.tmp_f41500_init
OPTION ( LABEL = 'CREATE_rep_jde.f41500_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f41500_sys_integration_id ON rep_jde.f41500_new(sys_integration_id); 
