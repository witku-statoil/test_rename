-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f03b14_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f03b14_new


CREATE TABLE rep_jde_qat.f03b14_new
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
	,cast(rzpyid as [DECIMAL](38, 4)) as rzpyid
	,cast(rzrc5 as [DECIMAL](38, 4)) as rzrc5
	,rzcknu as rzcknu
	,ltrim(rtrim(rzcknu)) as rzcknu_conv
	,cast(rzdoc as [DECIMAL](38, 4)) as rzdoc
	,cast(rzdct as [NVARCHAR](2)) as rzdct
	,rzkco as rzkco
	,ltrim(rtrim(rzkco)) as rzkco_conv
	,rzsfx as rzsfx
	,ltrim(rtrim(rzsfx)) as rzsfx_conv
	,cast(rzan8 as [DECIMAL](38, 4)) as rzan8
	,cast(rzdctm as [NVARCHAR](2)) as rzdctm
	,cast(rzdmtj as [INT]) as rzdmtj
	,case when cast(rzdmtj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzdmtj as [INT]) %1000, dateadd(year, cast(rzdmtj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzdmtj_conv
	,cast(rzdgj as [INT]) as rzdgj
	,case when cast(rzdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzdgj as [INT]) %1000, dateadd(year, cast(rzdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzdgj_conv
	,rzpost as rzpost
	,ltrim(rtrim(rzpost)) as rzpost_conv
	,rzglc as rzglc
	,ltrim(rtrim(rzglc)) as rzglc_conv
	,rzaid as rzaid
	,ltrim(rtrim(rzaid)) as rzaid_conv
	,cast(rzctry as [DECIMAL](38, 4)) as rzctry
	,cast(rzfy as [DECIMAL](38, 4)) as rzfy
	,cast(rzfy as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rzfy_conv
	,cast(rzpn as [DECIMAL](38, 4)) as rzpn
	,rzco as rzco
	,ltrim(rtrim(rzco)) as rzco_conv
	,cast(rzicut as [NVARCHAR](2)) as rzicut
	,cast(rzicu as [DECIMAL](38, 4)) as rzicu
	,cast(rzdicj as [INT]) as rzdicj
	,case when cast(rzdicj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzdicj as [INT]) %1000, dateadd(year, cast(rzdicj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzdicj_conv
	,cast(rzpa8 as [DECIMAL](38, 4)) as rzpa8
	,cast(rzrpid as [DECIMAL](38, 4)) as rzrpid
	,cast(rzrlin as [DECIMAL](38, 4)) as rzrlin
	,cast(rzpaap as [DECIMAL](38, 4)) as rzpaap
	,cast(rzpaap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzpaap_conv
	,cast(rzadsc as [DECIMAL](38, 4)) as rzadsc
	,cast(rzadsc as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzadsc_conv
	,cast(rzadsa as [DECIMAL](38, 4)) as rzadsa
	,cast(rzadsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzadsa_conv
	,cast(rzaaaj as [DECIMAL](38, 4)) as rzaaaj
	,cast(rzaaaj as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzaaaj_conv
	,cast(rzecba as [DECIMAL](38, 4)) as rzecba
	,cast(rzecba as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzecba_conv
	,cast(rzdda as [DECIMAL](38, 4)) as rzdda
	,cast(rzdda as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzdda_conv
	,rzbcrc as rzbcrc
	,ltrim(rtrim(rzbcrc)) as rzbcrc_conv
	,cast(rzcrrm as [NVARCHAR](1)) as rzcrrm
	,rzcrcd as rzcrcd
	,ltrim(rtrim(rzcrcd)) as rzcrcd_conv
	,cast(rzcrr as [DECIMAL](38, 4)) as rzcrr
	,cast(rzpfap as [DECIMAL](38, 4)) as rzpfap
	,cast(rzpfap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzpfap_conv
	,cast(rzcds as [DECIMAL](38, 4)) as rzcds
	,cast(rzcds as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzcds_conv
	,cast(rzcdsa as [DECIMAL](38, 4)) as rzcdsa
	,cast(rzcdsa as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzcdsa_conv
	,cast(rzfchg as [DECIMAL](38, 4)) as rzfchg
	,cast(rzfchg as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzfchg_conv
	,cast(rzecbf as [DECIMAL](38, 4)) as rzecbf
	,cast(rzecbf as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzecbf_conv
	,cast(rzfda as [DECIMAL](38, 4)) as rzfda
	,cast(rzfda as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzfda_conv
	,cast(rzagl as [DECIMAL](38, 4)) as rzagl
	,cast(rzagl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzagl_conv
	,rzaidt as rzaidt
	,ltrim(rtrim(rzaidt)) as rzaidt_conv
	,rztcrc as rztcrc
	,ltrim(rtrim(rztcrc)) as rztcrc_conv
	,cast(rztaap as [DECIMAL](38, 4)) as rztaap
	,cast(rztaap as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rztaap_conv
	,cast(rztada as [DECIMAL](38, 4)) as rztada
	,cast(rztada as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rztada_conv
	,cast(rztaaj as [DECIMAL](38, 4)) as rztaaj
	,cast(rztaaj as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rztaaj_conv
	,cast(rztcba as [DECIMAL](38, 4)) as rztcba
	,cast(rztcba as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rztcba_conv
	,cast(rztda as [DECIMAL](38, 4)) as rztda
	,cast(rztda as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rztda_conv
	,cast(rzacrr as [DECIMAL](38, 4)) as rzacrr
	,cast(rzacr1 as [DECIMAL](38, 4)) as rzacr1
	,cast(rzacr2 as [DECIMAL](38, 4)) as rzacr2
	,rzacrm as rzacrm
	,ltrim(rtrim(rzacrm)) as rzacrm_conv
	,cast(rzagla as [DECIMAL](38, 4)) as rzagla
	,cast(rzagla as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzagla_conv
	,rzaida as rzaida
	,ltrim(rtrim(rzaida)) as rzaida_conv
	,rzaidd as rzaidd
	,ltrim(rtrim(rzaidd)) as rzaidd_conv
	,cast(rzrsco as [NVARCHAR](2)) as rzrsco
	,rzaidw as rzaidw
	,ltrim(rtrim(rzaidw)) as rzaidw_conv
	,cast(rzecbr as [NVARCHAR](2)) as rzecbr
	,rzglcc as rzglcc
	,ltrim(rtrim(rzglcc)) as rzglcc_conv
	,rzaidc as rzaidc
	,ltrim(rtrim(rzaidc)) as rzaidc_conv
	,cast(rzddex as [NVARCHAR](2)) as rzddex
	,rzdaid as rzdaid
	,ltrim(rtrim(rzdaid)) as rzdaid_conv
	,cast(rztyin as [NVARCHAR](1)) as rztyin
	,cast(rzutic as [NVARCHAR](2)) as rzutic
	,rzuctf as rzuctf
	,ltrim(rtrim(rzuctf)) as rzuctf_conv
	,rzaid2 as rzaid2
	,ltrim(rtrim(rzaid2)) as rzaid2_conv
	,cast(rzam2 as [NVARCHAR](1)) as rzam2
	,rzmcu as rzmcu
	,ltrim(rtrim(rzmcu)) as rzmcu_conv
	,rzsbl as rzsbl
	,ltrim(rtrim(rzsbl)) as rzsbl_conv
	,cast(rzsblt as [NVARCHAR](1)) as rzsblt
	,rzpkco as rzpkco
	,ltrim(rtrim(rzpkco)) as rzpkco_conv
	,rzpo as rzpo
	,ltrim(rtrim(rzpo)) as rzpo_conv
	,cast(rzpdct as [NVARCHAR](2)) as rzpdct
	,cast(rznumb as [DECIMAL](38, 4)) as rznumb
	,rzunit as rzunit
	,ltrim(rtrim(rzunit)) as rzunit_conv
	,rzmcu2 as rzmcu2
	,ltrim(rtrim(rzmcu2)) as rzmcu2_conv
	,rzrmk as rzrmk
	,ltrim(rtrim(rzrmk)) as rzrmk_conv
	,rzfnlp as rzfnlp
	,ltrim(rtrim(rzfnlp)) as rzfnlp_conv
	,rzalt6 as rzalt6
	,ltrim(rtrim(rzalt6)) as rzalt6_conv
	,cast(rzodoc as [DECIMAL](38, 4)) as rzodoc
	,cast(rzodct as [NVARCHAR](2)) as rzodct
	,rzokco as rzokco
	,ltrim(rtrim(rzokco)) as rzokco_conv
	,rzosfx as rzosfx
	,ltrim(rtrim(rzosfx)) as rzosfx_conv
	,cast(rzgdoc as [DECIMAL](38, 4)) as rzgdoc
	,cast(rzgdct as [NVARCHAR](2)) as rzgdct
	,rzgkco as rzgkco
	,ltrim(rtrim(rzgkco)) as rzgkco_conv
	,rzgsfx as rzgsfx
	,ltrim(rtrim(rzgsfx)) as rzgsfx_conv
	,cast(rzdctg as [NVARCHAR](2)) as rzdctg
	,cast(rzdocg as [DECIMAL](38, 4)) as rzdocg
	,rzkcog as rzkcog
	,ltrim(rtrim(rzkcog)) as rzkcog_conv
	,rzctl as rzctl
	,ltrim(rtrim(rzctl)) as rzctl_conv
	,cast(rzsmtj as [INT]) as rzsmtj
	,case when cast(rzsmtj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzsmtj as [INT]) %1000, dateadd(year, cast(rzsmtj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzsmtj_conv
	,cast(rzvdgj as [INT]) as rzvdgj
	,case when cast(rzvdgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzvdgj as [INT]) %1000, dateadd(year, cast(rzvdgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzvdgj_conv
	,cast(rzvre as [NVARCHAR](3)) as rzvre
	,rznfvd as rznfvd
	,ltrim(rtrim(rznfvd)) as rznfvd_conv
	,cast(rzhcrr as [DECIMAL](38, 4)) as rzhcrr
	,rzistc as rzistc
	,ltrim(rtrim(rzistc)) as rzistc_conv
	,cast(rzlfcj as [INT]) as rzlfcj
	,case when cast(rzlfcj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzlfcj as [INT]) %1000, dateadd(year, cast(rzlfcj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzlfcj_conv
	,cast(rzpdlt as [NVARCHAR](1)) as rzpdlt
	,cast(rzddj as [INT]) as rzddj
	,case when cast(rzddj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzddj as [INT]) %1000, dateadd(year, cast(rzddj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzddj_conv
	,cast(rzddnj as [INT]) as rzddnj
	,case when cast(rzddnj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzddnj as [INT]) %1000, dateadd(year, cast(rzddnj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzddnj_conv
	,cast(rzidgj as [INT]) as rzidgj
	,case when cast(rzidgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzidgj as [INT]) %1000, dateadd(year, cast(rzidgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzidgj_conv
	,cast(rzddst as [NVARCHAR](1)) as rzddst
	,rzvr01 as rzvr01
	,ltrim(rtrim(rzvr01)) as rzvr01_conv
	,cast(rzrfid as [DECIMAL](38, 4)) as rzrfid
	,rzridc as rzridc
	,ltrim(rtrim(rzridc)) as rzridc_conv
	,rzprgf as rzprgf
	,ltrim(rtrim(rzprgf)) as rzprgf_conv
	,rzgfl1 as rzgfl1
	,ltrim(rtrim(rzgfl1)) as rzgfl1_conv
	,cast(rzrnid as [DECIMAL](38, 4)) as rzrnid
	,rztorg as rztorg
	,ltrim(rtrim(rztorg)) as rztorg_conv
	,rzuser as rzuser
	,ltrim(rtrim(rzuser)) as rzuser_conv
	,rzpid as rzpid
	,ltrim(rtrim(rzpid)) as rzpid_conv
	,cast(rzupmj as [INT]) as rzupmj
	,case when cast(rzupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzupmj as [INT]) %1000, dateadd(year, cast(rzupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzupmj_conv
	,cast(rzupmt as [DECIMAL](38, 4)) as rzupmt
	,rzjobn as rzjobn
	,ltrim(rtrim(rzjobn)) as rzjobn_conv
	,cast(rzurab as [DECIMAL](38, 4)) as rzurab
	,cast(rzurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rzurab_conv
	,cast(rzurat as [DECIMAL](38, 4)) as rzurat
	,cast(rzurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzurat_conv
	,rzurc1 as rzurc1
	,ltrim(rtrim(rzurc1)) as rzurc1_conv
	,cast(rzurdt as [INT]) as rzurdt
	,case when cast(rzurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzurdt as [INT]) %1000, dateadd(year, cast(rzurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzurdt_conv
	,rzurrf as rzurrf
	,ltrim(rtrim(rzurrf)) as rzurrf_conv
	,cast(rzshpn as [DECIMAL](38, 4)) as rzshpn
	,cast(rzomod as [NVARCHAR](1)) as rzomod
	,cast(rzdoco as [DECIMAL](38, 4)) as rzdoco
	,rzrasi as rzrasi
	,ltrim(rtrim(rzrasi)) as rzrasi_conv
	,rzsfxo as rzsfxo
	,ltrim(rtrim(rzsfxo)) as rzsfxo_conv
	,rzkcoo as rzkcoo
	,ltrim(rtrim(rzkcoo)) as rzkcoo_conv
	,cast(rzdcto as [NVARCHAR](2)) as rzdcto
	,cast(rzramt as [DECIMAL](38, 4)) as rzramt
	,cast(rzramt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as rzramt_conv
	,rzlrfl as rzlrfl
	,ltrim(rtrim(rzlrfl)) as rzlrfl_conv
	,rzgfl2 as rzgfl2
	,ltrim(rtrim(rzgfl2)) as rzgfl2_conv
	,cast(rzdrco as [NVARCHAR](3)) as rzdrco
	,cast(rznettcid as [DECIMAL](38, 4)) as rznettcid
	,cast(rznettcid as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rznettcid_conv
	,cast(rznetdoc as [DECIMAL](38, 4)) as rznetdoc
	,cast(rznetdoc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rznetdoc_conv
	,cast(rznetrc5 as [DECIMAL](38, 4)) as rznetrc5
	,cast(rznetrc5 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as rznetrc5_conv
	,cast(rzadgj as [INT]) as rzadgj
	,case when cast(rzadgj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(rzadgj as [INT]) %1000, dateadd(year, cast(rzadgj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as rzadgj_conv
FROM 
    stg_jde_qat.tmp_f03b14_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f03b14_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f03b14_sys_integration_id ON rep_jde_qat.f03b14_new(sys_integration_id); 
