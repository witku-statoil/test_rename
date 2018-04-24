-- Create rep new table for init
IF OBJECT_ID('rep_jde.f3111_new') IS NOT NULL
    DROP TABLE rep_jde.f3111_new


CREATE TABLE rep_jde.f3111_new
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
	,cast(wmdoco as [DECIMAL](38, 4)) as wmdoco
	,cast(wmdcto as [NVARCHAR](2)) as wmdcto
	,wmsfxo as wmsfxo
	,ltrim(rtrim(wmsfxo)) as wmsfxo_conv
	,cast(wmtbm as [NVARCHAR](3)) as wmtbm
	,cast(wmforq as [NVARCHAR](1)) as wmforq
	,cast(wmitc as [NVARCHAR](1)) as wmitc
	,cast(wmcoby as [NVARCHAR](1)) as wmcoby
	,cast(wmcoty as [NVARCHAR](1)) as wmcoty
	,cast(wmcpnt as [DECIMAL](38, 4)) as wmcpnt
	,cast(wmfrmp as [DECIMAL](38, 4)) as wmfrmp
	,cast(wmfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wmfrmp_conv
	,cast(wmthrp as [DECIMAL](38, 4)) as wmthrp
	,cast(wmthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wmthrp_conv
	,cast(wmfrgd as [NVARCHAR](3)) as wmfrgd
	,cast(wmthgd as [NVARCHAR](3)) as wmthgd
	,wmrkco as wmrkco
	,ltrim(rtrim(wmrkco)) as wmrkco_conv
	,wmrorn as wmrorn
	,ltrim(rtrim(wmrorn)) as wmrorn_conv
	,cast(wmrcto as [NVARCHAR](2)) as wmrcto
	,cast(wmrlln as [DECIMAL](38, 4)) as wmrlln
	,cast(wmrlln as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as wmrlln_conv
	,cast(wmopsq as [DECIMAL](38, 4)) as wmopsq
	,cast(wmopsq as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmopsq_conv
	,cast(wmbseq as [DECIMAL](38, 4)) as wmbseq
	,cast(wmbseq as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wmbseq_conv
	,cast(wmrscp as [DECIMAL](38, 4)) as wmrscp
	,cast(wmrscp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmrscp_conv
	,cast(wmscrp as [DECIMAL](38, 4)) as wmscrp
	,cast(wmscrp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmscrp_conv
	,cast(wmrewp as [DECIMAL](38, 4)) as wmrewp
	,cast(wmrewp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmrewp_conv
	,cast(wmasip as [DECIMAL](38, 4)) as wmasip
	,cast(wmasip as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmasip_conv
	,cast(wmcpyp as [DECIMAL](38, 4)) as wmcpyp
	,cast(wmcpyp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmcpyp_conv
	,cast(wmstpp as [DECIMAL](38, 4)) as wmstpp
	,cast(wmstpp as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmstpp_conv
	,cast(wmlovd as [DECIMAL](38, 4)) as wmlovd
	,cast(wmlovd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wmlovd_conv
	,cast(wmcpit as [DECIMAL](38, 4)) as wmcpit
	,wmcpil as wmcpil
	,ltrim(rtrim(wmcpil)) as wmcpil_conv
	,wmcpia as wmcpia
	,ltrim(rtrim(wmcpia)) as wmcpia_conv
	,wmcmcu as wmcmcu
	,ltrim(rtrim(wmcmcu)) as wmcmcu_conv
	,wmdsc1 as wmdsc1
	,ltrim(rtrim(wmdsc1)) as wmdsc1_conv
	,wmdsc2 as wmdsc2
	,ltrim(rtrim(wmdsc2)) as wmdsc2_conv
	,wmlocn as wmlocn
	,ltrim(rtrim(wmlocn)) as wmlocn_conv
	,wmlotn as wmlotn
	,ltrim(rtrim(wmlotn)) as wmlotn_conv
	,cast(wman8 as [DECIMAL](38, 4)) as wman8
	,wmlnty as wmlnty
	,ltrim(rtrim(wmlnty)) as wmlnty_conv
	,wmsern as wmsern
	,ltrim(rtrim(wmsern)) as wmsern_conv
	,cast(wmtrdj as [INT]) as wmtrdj
	,case when cast(wmtrdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmtrdj as [INT]) %1000, dateadd(year, cast(wmtrdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmtrdj_conv
	,cast(wmdrqj as [INT]) as wmdrqj
	,case when cast(wmdrqj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmdrqj as [INT]) %1000, dateadd(year, cast(wmdrqj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmdrqj_conv
	,cast(wmuorg as [DECIMAL](38, 4)) as wmuorg
	,cast(wmuorg as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmuorg_conv
	,cast(wmtrqt as [DECIMAL](38, 4)) as wmtrqt
	,cast(wmtrqt as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmtrqt_conv
	,cast(wmsocn as [DECIMAL](38, 4)) as wmsocn
	,cast(wmsocn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmsocn_conv
	,cast(wmsobk as [DECIMAL](38, 4)) as wmsobk
	,cast(wmsobk as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmsobk_conv
	,cast(wmcts1 as [DECIMAL](38, 4)) as wmcts1
	,cast(wmcts1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmcts1_conv
	,cast(wmqnta as [DECIMAL](38, 4)) as wmqnta
	,cast(wmqnta as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmqnta_conv
	,cast(wmum as [NVARCHAR](2)) as wmum
	,cast(wmea as [DECIMAL](38, 4)) as wmea
	,cast(wmea as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmea_conv
	,wmrtg as wmrtg
	,ltrim(rtrim(wmrtg)) as wmrtg_conv
	,cast(wmmtst as [NVARCHAR](2)) as wmmtst
	,cast(wmdct as [NVARCHAR](2)) as wmdct
	,wmshno as wmshno
	,ltrim(rtrim(wmshno)) as wmshno_conv
	,wmmcu as wmmcu
	,ltrim(rtrim(wmmcu)) as wmmcu_conv
	,wmomcu as wmomcu
	,ltrim(rtrim(wmomcu)) as wmomcu_conv
	,wmobj as wmobj
	,ltrim(rtrim(wmobj)) as wmobj_conv
	,wmsub as wmsub
	,ltrim(rtrim(wmsub)) as wmsub_conv
	,wmcmrv as wmcmrv
	,ltrim(rtrim(wmcmrv)) as wmcmrv_conv
	,cast(wmstrx as [INT]) as wmstrx
	,case when cast(wmstrx as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmstrx as [INT]) %1000, dateadd(year, cast(wmstrx as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmstrx_conv
	,wmpars as wmpars
	,ltrim(rtrim(wmpars)) as wmpars_conv
	,cast(wmcomm as [NVARCHAR](1)) as wmcomm
	,wmurcd as wmurcd
	,ltrim(rtrim(wmurcd)) as wmurcd_conv
	,cast(wmurdt as [INT]) as wmurdt
	,case when cast(wmurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmurdt as [INT]) %1000, dateadd(year, cast(wmurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmurdt_conv
	,cast(wmurat as [DECIMAL](38, 4)) as wmurat
	,cast(wmurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmurat_conv
	,wmurrf as wmurrf
	,ltrim(rtrim(wmurrf)) as wmurrf_conv
	,cast(wmurab as [DECIMAL](38, 4)) as wmurab
	,cast(wmurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wmurab_conv
	,wmuser as wmuser
	,ltrim(rtrim(wmuser)) as wmuser_conv
	,wmpid as wmpid
	,ltrim(rtrim(wmpid)) as wmpid_conv
	,wmjobn as wmjobn
	,ltrim(rtrim(wmjobn)) as wmjobn_conv
	,cast(wmupmj as [INT]) as wmupmj
	,case when cast(wmupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmupmj as [INT]) %1000, dateadd(year, cast(wmupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmupmj_conv
	,cast(wmtday as [DECIMAL](38, 4)) as wmtday
	,cast(wmukid as [DECIMAL](38, 4)) as wmukid
	,cast(wmvend as [DECIMAL](38, 4)) as wmvend
	,cast(wmvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as wmvend_conv
	,cast(wmpoc as [NVARCHAR](1)) as wmpoc
	,cast(wmcts4 as [DECIMAL](38, 4)) as wmcts4
	,cast(wmcts4 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmcts4_conv
	,cast(wmcts7 as [DECIMAL](38, 4)) as wmcts7
	,cast(wmcts7 as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmcts7_conv
	,cast(wmcts8 as [DECIMAL](38, 4)) as wmcts8
	,cast(wmcts8 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmcts8_conv
	,cast(wmgld as [INT]) as wmgld
	,case when cast(wmgld as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmgld as [INT]) %1000, dateadd(year, cast(wmgld as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmgld_conv
	,wmsbfl as wmsbfl
	,ltrim(rtrim(wmsbfl)) as wmsbfl_conv
	,wmaing as wmaing
	,ltrim(rtrim(wmaing)) as wmaing_conv
	,cast(wmsstq as [DECIMAL](38, 4)) as wmsstq
	,cast(wmsstq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as wmsstq_conv
	,cast(wmuom2 as [NVARCHAR](2)) as wmuom2
	,cast(wmapsc as [NVARCHAR](1)) as wmapsc
	,cast(wmpsn as [DECIMAL](38, 4)) as wmpsn
	,cast(wmdlej as [INT]) as wmdlej
	,case when cast(wmdlej as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(wmdlej as [INT]) %1000, dateadd(year, cast(wmdlej as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as wmdlej_conv
	,cast(wmcost as [NVARCHAR](3)) as wmcost
	,wmchpp as wmchpp
	,ltrim(rtrim(wmchpp)) as wmchpp_conv
	,cast(wmcpnb as [DECIMAL](38, 4)) as wmcpnb
	,cast(wmcpnb as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as wmcpnb_conv
	,wmbseqan as wmbseqan
	,ltrim(rtrim(wmbseqan)) as wmbseqan_conv
FROM 
    stg_jde.tmp_f3111_init
OPTION ( LABEL = 'CREATE_rep_jde.f3111_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f3111_sys_integration_id ON rep_jde.f3111_new(sys_integration_id); 
