-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4111_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4111_cdc


CREATE TABLE stg_jde.tmp_upsert_f4111_cdc
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
	,cast(ilnlin as [DECIMAL](38, 4)) as ilnlin
	,cast(ilitm as [DECIMAL](38, 4)) as ilitm
	,illitm as illitm
	,ltrim(rtrim(illitm)) as illitm_conv
	,ilaitm as ilaitm
	,ltrim(rtrim(ilaitm)) as ilaitm_conv
	,ilmcu as ilmcu
	,ltrim(rtrim(ilmcu)) as ilmcu_conv
	,illocn as illocn
	,ltrim(rtrim(illocn)) as illocn_conv
	,illotn as illotn
	,ltrim(rtrim(illotn)) as illotn_conv
	,ilplot as ilplot
	,ltrim(rtrim(ilplot)) as ilplot_conv
	,cast(ilstun as [DECIMAL](38, 4)) as ilstun
	,cast(illdsq as [DECIMAL](38, 4)) as illdsq
	,cast(iltrno as [DECIMAL](38, 4)) as iltrno
	,ilfrto as ilfrto
	,ltrim(rtrim(ilfrto)) as ilfrto_conv
	,illmcx as illmcx
	,ltrim(rtrim(illmcx)) as illmcx_conv
	,cast(illots as [NVARCHAR](1)) as illots
	,cast(illotp as [DECIMAL](38, 4)) as illotp
	,cast(illotp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as illotp_conv
	,cast(illotg as [NVARCHAR](3)) as illotg
	,cast(ilkit as [DECIMAL](38, 4)) as ilkit
	,ilmmcu as ilmmcu
	,ltrim(rtrim(ilmmcu)) as ilmmcu_conv
	,ildmct as ildmct
	,ltrim(rtrim(ildmct)) as ildmct_conv
	,cast(ildmcs as [DECIMAL](38, 4)) as ildmcs
	,ilkco as ilkco
	,ltrim(rtrim(ilkco)) as ilkco_conv
	,cast(ildoc as [DECIMAL](38, 4)) as ildoc
	,cast(ildct as [NVARCHAR](2)) as ildct
	,ilsfx as ilsfx
	,ltrim(rtrim(ilsfx)) as ilsfx_conv
	,cast(iljeln as [DECIMAL](38, 4)) as iljeln
	,cast(ilicu as [DECIMAL](38, 4)) as ilicu
	,cast(ildgl as [INT]) as ildgl
	,case when cast(ildgl as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ildgl as [INT]) %1000, dateadd(year, cast(ildgl as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ildgl_conv
	,cast(ilglpt as [NVARCHAR](4)) as ilglpt
	,cast(ildcto as [NVARCHAR](2)) as ildcto
	,cast(ildoco as [DECIMAL](38, 4)) as ildoco
	,ilkcoo as ilkcoo
	,ltrim(rtrim(ilkcoo)) as ilkcoo_conv
	,cast(illnid as [DECIMAL](38, 4)) as illnid
	,cast(illnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as illnid_conv
	,cast(ilipcd as [NVARCHAR](1)) as ilipcd
	,cast(iltrdj as [INT]) as iltrdj
	,case when cast(iltrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(iltrdj as [INT]) %1000, dateadd(year, cast(iltrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as iltrdj_conv
	,cast(iltrum as [NVARCHAR](2)) as iltrum
	,cast(ilan8 as [DECIMAL](38, 4)) as ilan8
	,iltrex as iltrex
	,ltrim(rtrim(iltrex)) as iltrex_conv
	,iltref as iltref
	,ltrim(rtrim(iltref)) as iltref_conv
	,cast(ilrcd as [NVARCHAR](3)) as ilrcd
	,cast(iltrqt as [DECIMAL](38, 4)) as iltrqt
	,cast(iltrqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iltrqt_conv
	,cast(iluncs as [DECIMAL](38, 4)) as iluncs
	,cast(iluncs as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as iluncs_conv
	,cast(ilpaid as [DECIMAL](38, 4)) as ilpaid
	,cast(ilpaid as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ilpaid_conv
	,ilterm as ilterm
	,ltrim(rtrim(ilterm)) as ilterm_conv
	,cast(ilukid as [DECIMAL](38, 4)) as ilukid
	,cast(iltday as [DECIMAL](38, 4)) as iltday
	,iluser as iluser
	,ltrim(rtrim(iluser)) as iluser_conv
	,ilpid as ilpid
	,ltrim(rtrim(ilpid)) as ilpid_conv
	,cast(ilcrdj as [INT]) as ilcrdj
	,case when cast(ilcrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ilcrdj as [INT]) %1000, dateadd(year, cast(ilcrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ilcrdj_conv
	,ilaid as ilaid
	,ltrim(rtrim(ilaid)) as ilaid_conv
	,ilasid as ilasid
	,ltrim(rtrim(ilasid)) as ilasid_conv
	,ilmcuz as ilmcuz
	,ltrim(rtrim(ilmcuz)) as ilmcuz_conv
	,ilobj as ilobj
	,ltrim(rtrim(ilobj)) as ilobj_conv
	,ilsbl as ilsbl
	,ltrim(rtrim(ilsbl)) as ilsbl_conv
	,ilsub as ilsub
	,ltrim(rtrim(ilsub)) as ilsub_conv
	,cast(iluom2 as [NVARCHAR](2)) as iluom2
	,ilcmoo as ilcmoo
	,ltrim(rtrim(ilcmoo)) as ilcmoo_conv
	,cast(ilre as [NVARCHAR](1)) as ilre
	,cast(ilsblt as [NVARCHAR](1)) as ilsblt
	,cast(ilsqor as [DECIMAL](38, 4)) as ilsqor
	,cast(ilsqor as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ilsqor_conv
	,cast(ilvpej as [INT]) as ilvpej
	,case when cast(ilvpej as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ilvpej as [INT]) %1000, dateadd(year, cast(ilvpej as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ilvpej_conv
	,cast(ilhdgj as [INT]) as ilhdgj
	,case when cast(ilhdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ilhdgj as [INT]) %1000, dateadd(year, cast(ilhdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ilhdgj_conv
	,cast(ilshan as [DECIMAL](38, 4)) as ilshan
	,cast(ilopsq as [DECIMAL](38, 4)) as ilopsq
	,cast(ilopsq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ilopsq_conv
	,cast(ilrfln as [DECIMAL](38, 4)) as ilrfln
	,cast(ilrfln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ilrfln_conv
	,cast(iltgn as [DECIMAL](38, 4)) as iltgn
	,cast(iltgn as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as iltgn_conv
	,illotc as illotc
	,ltrim(rtrim(illotc)) as illotc_conv
	,cast(ilsvdt as [INT]) as ilsvdt
	,case when cast(ilsvdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ilsvdt as [INT]) %1000, dateadd(year, cast(ilsvdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ilsvdt_conv
	,cast(illrcd as [NVARCHAR](3)) as illrcd
	,ilrlot as ilrlot
	,ltrim(rtrim(ilrlot)) as ilrlot_conv
	,illpnu as illpnu
	,ltrim(rtrim(illpnu)) as illpnu_conv
	,ilpmpn as ilpmpn
	,ltrim(rtrim(ilpmpn)) as ilpmpn_conv
	,cast(ilpns as [DECIMAL](38, 4)) as ilpns
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
	,ilnlin
	,ilitm
	,illitm
	,ilaitm
	,ilmcu
	,illocn
	,illotn
	,ilplot
	,ilstun
	,illdsq
	,iltrno
	,ilfrto
	,illmcx
	,illots
	,illotp
	,illotg
	,ilkit
	,ilmmcu
	,ildmct
	,ildmcs
	,ilkco
	,ildoc
	,ildct
	,ilsfx
	,iljeln
	,ilicu
	,ildgl
	,ilglpt
	,ildcto
	,ildoco
	,ilkcoo
	,illnid
	,ilipcd
	,iltrdj
	,iltrum
	,ilan8
	,iltrex
	,iltref
	,ilrcd
	,iltrqt
	,iluncs
	,ilpaid
	,ilterm
	,ilukid
	,iltday
	,iluser
	,ilpid
	,ilcrdj
	,ilaid
	,ilasid
	,ilmcuz
	,ilobj
	,ilsbl
	,ilsub
	,iluom2
	,ilcmoo
	,ilre
	,ilsblt
	,ilsqor
	,ilvpej
	,ilhdgj
	,ilshan
	,ilopsq
	,ilrfln
	,iltgn
	,illotc
	,ilsvdt
	,illrcd
	,ilrlot
	,illpnu
	,ilpmpn
	,ilpns,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4111_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4111_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4111_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4111_cdc(sys_integration_id); 
