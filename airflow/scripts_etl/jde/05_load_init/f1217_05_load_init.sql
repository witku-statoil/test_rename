-- Create rep new table for init
IF OBJECT_ID('rep_jde.f1217_new') IS NOT NULL
    DROP TABLE rep_jde.f1217_new


CREATE TABLE rep_jde.f1217_new
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
	,cast(wrnumb as [DECIMAL](38, 4)) as wrnumb
	,cast(wrctr as [NVARCHAR](3)) as wrctr
	,wrcoown as wrcoown
	,ltrim(rtrim(wrcoown)) as wrcoown_conv
	,wrmmcu as wrmmcu
	,ltrim(rtrim(wrmmcu)) as wrmmcu_conv
	,cast(wrdoco as [DECIMAL](38, 4)) as wrdoco
	,cast(wrdcto as [NVARCHAR](2)) as wrdcto
	,wrkcoo as wrkcoo
	,ltrim(rtrim(wrkcoo)) as wrkcoo_conv
	,cast(wrlnid as [DECIMAL](38, 4)) as wrlnid
	,cast(wrlnid as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wrlnid_conv
	,cast(wrordj as [INT]) as wrordj
	,case when cast(wrordj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrordj as [INT]) %1000, dateadd(year, cast(wrordj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrordj_conv
	,cast(wrshpj as [INT]) as wrshpj
	,case when cast(wrshpj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrshpj as [INT]) %1000, dateadd(year, cast(wrshpj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrshpj_conv
	,cast(wraddj as [INT]) as wraddj
	,case when cast(wraddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wraddj as [INT]) %1000, dateadd(year, cast(wraddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wraddj_conv
	,cast(wrshan as [DECIMAL](38, 4)) as wrshan
	,cast(wrpa8 as [DECIMAL](38, 4)) as wrpa8
	,cast(wrslsm as [DECIMAL](38, 4)) as wrslsm
	,cast(wranob as [DECIMAL](38, 4)) as wranob
	,wrrorn as wrrorn
	,ltrim(rtrim(wrrorn)) as wrrorn_conv
	,cast(wrrcto as [NVARCHAR](2)) as wrrcto
	,cast(wrstrx as [INT]) as wrstrx
	,case when cast(wrstrx as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrstrx as [INT]) %1000, dateadd(year, cast(wrstrx as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrstrx_conv
	,cast(wrvend as [DECIMAL](38, 4)) as wrvend
	,cast(wrvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wrvend_conv
	,wroorn as wroorn
	,ltrim(rtrim(wroorn)) as wroorn_conv
	,cast(wrocto as [NVARCHAR](2)) as wrocto
	,wrokco as wrokco
	,ltrim(rtrim(wrokco)) as wrokco_conv
	,wrsfxo as wrsfxo
	,ltrim(rtrim(wrsfxo)) as wrsfxo_conv
	,cast(wrogno as [DECIMAL](38, 4)) as wrogno
	,cast(wrogno as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wrogno_conv
	,cast(wrprodf as [NVARCHAR](8)) as wrprodf
	,cast(wrprodm as [NVARCHAR](8)) as wrprodm
	,cast(wrprodc as [NVARCHAR](10)) as wrprodc
	,wrcmod as wrcmod
	,ltrim(rtrim(wrcmod)) as wrcmod_conv
	,wrsyem as wrsyem
	,ltrim(rtrim(wrsyem)) as wrsyem_conv
	,wrvinnu as wrvinnu
	,ltrim(rtrim(wrvinnu)) as wrvinnu_conv
	,wrrefn as wrrefn
	,ltrim(rtrim(wrrefn)) as wrrefn_conv
	,cast(wrze01 as [NVARCHAR](10)) as wrze01
	,cast(wrze02 as [NVARCHAR](10)) as wrze02
	,cast(wrze03 as [NVARCHAR](10)) as wrze03
	,cast(wrze04 as [NVARCHAR](10)) as wrze04
	,cast(wrze05 as [NVARCHAR](10)) as wrze05
	,cast(wrze06 as [NVARCHAR](10)) as wrze06
	,cast(wrze07 as [NVARCHAR](10)) as wrze07
	,cast(wrze08 as [NVARCHAR](10)) as wrze08
	,cast(wrze09 as [NVARCHAR](10)) as wrze09
	,cast(wrze10 as [NVARCHAR](10)) as wrze10
	,wrurcd as wrurcd
	,ltrim(rtrim(wrurcd)) as wrurcd_conv
	,cast(wrurdt as [INT]) as wrurdt
	,case when cast(wrurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrurdt as [INT]) %1000, dateadd(year, cast(wrurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrurdt_conv
	,cast(wrurat as [DECIMAL](38, 4)) as wrurat
	,cast(wrurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wrurat_conv
	,cast(wrurab as [DECIMAL](38, 4)) as wrurab
	,cast(wrurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wrurab_conv
	,wrurrf as wrurrf
	,ltrim(rtrim(wrurrf)) as wrurrf_conv
	,wrcrtu as wrcrtu
	,ltrim(rtrim(wrcrtu)) as wrcrtu_conv
	,wruser as wruser
	,ltrim(rtrim(wruser)) as wruser_conv
	,wrpid as wrpid
	,ltrim(rtrim(wrpid)) as wrpid_conv
	,wrjobn as wrjobn
	,ltrim(rtrim(wrjobn)) as wrjobn_conv
	,cast(wrupmj as [INT]) as wrupmj
	,case when cast(wrupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrupmj as [INT]) %1000, dateadd(year, cast(wrupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrupmj_conv
	,cast(wrupmt as [DECIMAL](38, 4)) as wrupmt
	,wrlotn as wrlotn
	,ltrim(rtrim(wrlotn)) as wrlotn_conv
	,wrlocn as wrlocn
	,ltrim(rtrim(wrlocn)) as wrlocn_conv
	,cast(wrwod as [DECIMAL](38, 4)) as wrwod
	,wrbrev as wrbrev
	,ltrim(rtrim(wrbrev)) as wrbrev_conv
	,cast(wrefff as [INT]) as wrefff
	,case when cast(wrefff as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrefff as [INT]) %1000, dateadd(year, cast(wrefff as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrefff_conv
	,cast(wran8dl as [DECIMAL](38, 4)) as wran8dl
	,cast(wran8as as [DECIMAL](38, 4)) as wran8as
	,cast(wran8as as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wran8as_conv
	,wrtermyn as wrtermyn
	,ltrim(rtrim(wrtermyn)) as wrtermyn_conv
	,cast(wrsatyp as [NVARCHAR](3)) as wrsatyp
	,cast(wrinsdte as [INT]) as wrinsdte
	,case when cast(wrinsdte as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wrinsdte as [INT]) %1000, dateadd(year, cast(wrinsdte as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wrinsdte_conv
	,wrmcu as wrmcu
	,ltrim(rtrim(wrmcu)) as wrmcu_conv
	,wrmrryn as wrmrryn
	,ltrim(rtrim(wrmrryn)) as wrmrryn_conv
	,cast(wrregsts as [NVARCHAR](3)) as wrregsts
	,cast(wrvmrs31 as [NVARCHAR](2)) as wrvmrs31
	,cast(wrvmrs32 as [NVARCHAR](8)) as wrvmrs32
	,wrvmrs34 as wrvmrs34
	,ltrim(rtrim(wrvmrs34)) as wrvmrs34_conv
	,cast(wran8dr as [DECIMAL](38, 4)) as wran8dr
	,cast(wran8dr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wran8dr_conv
	,cast(wreqpn as [DECIMAL](38, 4)) as wreqpn
	,cast(wreqpn as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wreqpn_conv
	,wrmtryn as wrmtryn
	,ltrim(rtrim(wrmtryn)) as wrmtryn_conv
FROM 
    stg_jde.tmp_f1217_init
OPTION ( LABEL = 'CREATE_rep_jde.f1217_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f1217_sys_integration_id ON rep_jde.f1217_new(sys_integration_id); 
