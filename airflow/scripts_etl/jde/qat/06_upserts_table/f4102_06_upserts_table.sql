-- Create upsert table for cdc
IF OBJECT_ID('stg_jde_qat.tmp_upsert_f4102_cdc') IS NOT NULL
    DROP TABLE stg_jde_qat.tmp_upsert_f4102_cdc


CREATE TABLE stg_jde_qat.tmp_upsert_f4102_cdc
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
	,cast(ibitm as [DECIMAL](38, 4)) as ibitm
	,iblitm as iblitm
	,ltrim(rtrim(iblitm)) as iblitm_conv
	,ibaitm as ibaitm
	,ltrim(rtrim(ibaitm)) as ibaitm_conv
	,ibmcu as ibmcu
	,ltrim(rtrim(ibmcu)) as ibmcu_conv
	,cast(ibsrp1 as [NVARCHAR](3)) as ibsrp1
	,cast(ibsrp2 as [NVARCHAR](3)) as ibsrp2
	,cast(ibsrp3 as [NVARCHAR](3)) as ibsrp3
	,cast(ibsrp4 as [NVARCHAR](3)) as ibsrp4
	,cast(ibsrp5 as [NVARCHAR](3)) as ibsrp5
	,cast(ibsrp6 as [NVARCHAR](6)) as ibsrp6
	,cast(ibsrp7 as [NVARCHAR](6)) as ibsrp7
	,cast(ibsrp8 as [NVARCHAR](6)) as ibsrp8
	,cast(ibsrp9 as [NVARCHAR](6)) as ibsrp9
	,cast(ibsrp0 as [NVARCHAR](6)) as ibsrp0
	,cast(ibprp1 as [NVARCHAR](3)) as ibprp1
	,cast(ibprp2 as [NVARCHAR](3)) as ibprp2
	,cast(ibprp3 as [NVARCHAR](3)) as ibprp3
	,cast(ibprp4 as [NVARCHAR](3)) as ibprp4
	,cast(ibprp5 as [NVARCHAR](3)) as ibprp5
	,cast(ibprp6 as [NVARCHAR](6)) as ibprp6
	,cast(ibprp7 as [NVARCHAR](6)) as ibprp7
	,cast(ibprp8 as [NVARCHAR](6)) as ibprp8
	,cast(ibprp9 as [NVARCHAR](6)) as ibprp9
	,cast(ibprp0 as [NVARCHAR](6)) as ibprp0
	,ibcdcd as ibcdcd
	,ltrim(rtrim(ibcdcd)) as ibcdcd_conv
	,cast(ibpdgr as [NVARCHAR](3)) as ibpdgr
	,cast(ibdsgp as [NVARCHAR](3)) as ibdsgp
	,cast(ibvend as [DECIMAL](38, 4)) as ibvend
	,cast(ibvend as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibvend_conv
	,cast(ibanpl as [DECIMAL](38, 4)) as ibanpl
	,cast(ibbuyr as [DECIMAL](38, 4)) as ibbuyr
	,cast(ibbuyr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibbuyr_conv
	,cast(ibglpt as [NVARCHAR](4)) as ibglpt
	,cast(iborig as [NVARCHAR](3)) as iborig
	,cast(ibropi as [DECIMAL](38, 4)) as ibropi
	,cast(ibropi as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibropi_conv
	,cast(ibroqi as [DECIMAL](38, 4)) as ibroqi
	,cast(ibroqi as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibroqi_conv
	,cast(ibrqmx as [DECIMAL](38, 4)) as ibrqmx
	,cast(ibrqmx as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibrqmx_conv
	,cast(ibrqmn as [DECIMAL](38, 4)) as ibrqmn
	,cast(ibrqmn as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibrqmn_conv
	,cast(ibwomo as [DECIMAL](38, 4)) as ibwomo
	,cast(ibwomo as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibwomo_conv
	,cast(ibserv as [DECIMAL](38, 4)) as ibserv
	,cast(ibsafe as [DECIMAL](38, 4)) as ibsafe
	,cast(ibsafe as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibsafe_conv
	,cast(ibsld as [DECIMAL](38, 4)) as ibsld
	,cast(ibsld as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibsld_conv
	,ibckav as ibckav
	,ltrim(rtrim(ibckav)) as ibckav_conv
	,cast(ibsrce as [NVARCHAR](1)) as ibsrce
	,cast(iblots as [NVARCHAR](1)) as iblots
	,ibot1y as ibot1y
	,ltrim(rtrim(ibot1y)) as ibot1y_conv
	,ibot2y as ibot2y
	,ltrim(rtrim(ibot2y)) as ibot2y_conv
	,cast(ibstdp as [DECIMAL](38, 4)) as ibstdp
	,cast(ibstdp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ibstdp_conv
	,cast(ibfrmp as [DECIMAL](38, 4)) as ibfrmp
	,cast(ibfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ibfrmp_conv
	,cast(ibthrp as [DECIMAL](38, 4)) as ibthrp
	,cast(ibthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ibthrp_conv
	,cast(ibstdg as [NVARCHAR](3)) as ibstdg
	,cast(ibfrgd as [NVARCHAR](3)) as ibfrgd
	,cast(ibthgd as [NVARCHAR](3)) as ibthgd
	,cast(ibcoty as [NVARCHAR](1)) as ibcoty
	,cast(ibmmpc as [DECIMAL](38, 4)) as ibmmpc
	,cast(ibmmpc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ibmmpc_conv
	,cast(ibprgr as [NVARCHAR](8)) as ibprgr
	,cast(ibrprc as [NVARCHAR](8)) as ibrprc
	,cast(iborpr as [NVARCHAR](8)) as iborpr
	,ibback as ibback
	,ltrim(rtrim(ibback)) as ibback_conv
	,cast(ibifla as [NVARCHAR](2)) as ibifla
	,cast(ibabcs as [NVARCHAR](1)) as ibabcs
	,cast(ibabcm as [NVARCHAR](1)) as ibabcm
	,cast(ibabci as [NVARCHAR](1)) as ibabci
	,ibovr as ibovr
	,ltrim(rtrim(ibovr)) as ibovr_conv
	,cast(ibshcm as [NVARCHAR](3)) as ibshcm
	,cast(ibcars as [DECIMAL](38, 4)) as ibcars
	,cast(ibcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibcars_conv
	,cast(ibcarp as [DECIMAL](38, 4)) as ibcarp
	,cast(ibcarp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibcarp_conv
	,cast(ibshcn as [NVARCHAR](3)) as ibshcn
	,cast(ibstkt as [NVARCHAR](1)) as ibstkt
	,iblnty as iblnty
	,ltrim(rtrim(iblnty)) as iblnty_conv
	,ibfifo as ibfifo
	,ltrim(rtrim(ibfifo)) as ibfifo_conv
	,cast(ibcycl as [NVARCHAR](3)) as ibcycl
	,cast(ibinmg as [NVARCHAR](10)) as ibinmg
	,ibwarr as ibwarr
	,ltrim(rtrim(ibwarr)) as ibwarr_conv
	,cast(ibsrnr as [NVARCHAR](1)) as ibsrnr
	,cast(ibpctm as [DECIMAL](38, 4)) as ibpctm
	,cast(ibpctm as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as ibpctm_conv
	,ibcmcg as ibcmcg
	,ltrim(rtrim(ibcmcg)) as ibcmcg_conv
	,cast(ibfuf1 as [NVARCHAR](1)) as ibfuf1
	,cast(ibtx as [NVARCHAR](1)) as ibtx
	,cast(ibtax1 as [NVARCHAR](1)) as ibtax1
	,cast(ibmpst as [NVARCHAR](1)) as ibmpst
	,cast(ibmrpd as [NVARCHAR](1)) as ibmrpd
	,cast(ibmrpc as [NVARCHAR](1)) as ibmrpc
	,cast(ibupc as [DECIMAL](38, 4)) as ibupc
	,cast(ibupc as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibupc_conv
	,cast(ibsns as [NVARCHAR](1)) as ibsns
	,ibmerl as ibmerl
	,ltrim(rtrim(ibmerl)) as ibmerl_conv
	,cast(ibltlv as [DECIMAL](38, 4)) as ibltlv
	,cast(ibltlv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibltlv_conv
	,cast(ibltmf as [DECIMAL](38, 4)) as ibltmf
	,cast(ibltmf as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibltmf_conv
	,cast(ibltcm as [DECIMAL](38, 4)) as ibltcm
	,cast(ibltcm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibltcm_conv
	,cast(ibopc as [NVARCHAR](1)) as ibopc
	,cast(ibopv as [DECIMAL](38, 4)) as ibopv
	,cast(ibopv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibopv_conv
	,cast(ibacq as [DECIMAL](38, 4)) as ibacq
	,cast(ibacq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibacq_conv
	,cast(ibmlq as [DECIMAL](38, 4)) as ibmlq
	,cast(ibmlq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibmlq_conv
	,cast(ibltpu as [DECIMAL](38, 4)) as ibltpu
	,cast(ibltpu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ibltpu_conv
	,cast(ibmpsp as [NVARCHAR](1)) as ibmpsp
	,cast(ibmrpp as [NVARCHAR](1)) as ibmrpp
	,cast(ibitc as [NVARCHAR](1)) as ibitc
	,ibeco as ibeco
	,ltrim(rtrim(ibeco)) as ibeco_conv
	,cast(ibecty as [NVARCHAR](2)) as ibecty
	,cast(ibecod as [INT]) as ibecod
	,case when cast(ibecod as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ibecod as [INT]) %1000, dateadd(year, cast(ibecod as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ibecod_conv
	,cast(ibmtf1 as [DECIMAL](38, 4)) as ibmtf1
	,cast(ibmtf1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibmtf1_conv
	,cast(ibmtf2 as [DECIMAL](38, 4)) as ibmtf2
	,cast(ibmtf2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibmtf2_conv
	,cast(ibmtf3 as [DECIMAL](38, 4)) as ibmtf3
	,cast(ibmtf3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibmtf3_conv
	,cast(ibmtf4 as [DECIMAL](38, 4)) as ibmtf4
	,cast(ibmtf4 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibmtf4_conv
	,cast(ibmtf5 as [DECIMAL](38, 4)) as ibmtf5
	,cast(ibmtf5 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibmtf5_conv
	,cast(ibmovd as [DECIMAL](38, 4)) as ibmovd
	,cast(ibmovd as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ibmovd_conv
	,cast(ibqued as [DECIMAL](38, 4)) as ibqued
	,cast(ibqued as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ibqued_conv
	,cast(ibsetl as [DECIMAL](38, 4)) as ibsetl
	,cast(ibsetl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ibsetl_conv
	,cast(ibsrnk as [DECIMAL](38, 4)) as ibsrnk
	,cast(ibsrnk as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ibsrnk_conv
	,cast(ibsrkf as [NVARCHAR](1)) as ibsrkf
	,cast(ibtimb as [NVARCHAR](1)) as ibtimb
	,cast(ibbqty as [DECIMAL](38, 4)) as ibbqty
	,cast(ibbqty as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibbqty_conv
	,ibordw as ibordw
	,ltrim(rtrim(ibordw)) as ibordw_conv
	,cast(ibexpd as [DECIMAL](38, 4)) as ibexpd
	,cast(ibexpd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibexpd_conv
	,cast(ibdefd as [DECIMAL](38, 4)) as ibdefd
	,cast(ibdefd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibdefd_conv
	,cast(ibmult as [DECIMAL](38, 4)) as ibmult
	,cast(ibmult as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibmult_conv
	,cast(ibsflt as [DECIMAL](38, 4)) as ibsflt
	,cast(ibsflt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibsflt_conv
	,cast(ibmake as [NVARCHAR](1)) as ibmake
	,cast(iblfdj as [INT]) as iblfdj
	,case when cast(iblfdj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(iblfdj as [INT]) %1000, dateadd(year, cast(iblfdj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as iblfdj_conv
	,cast(ibllx as [DECIMAL](38, 4)) as ibllx
	,cast(ibcmgl as [NVARCHAR](1)) as ibcmgl
	,iburcd as iburcd
	,ltrim(rtrim(iburcd)) as iburcd_conv
	,cast(iburdt as [INT]) as iburdt
	,case when cast(iburdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(iburdt as [INT]) %1000, dateadd(year, cast(iburdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as iburdt_conv
	,cast(iburat as [DECIMAL](38, 4)) as iburat
	,cast(iburat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as iburat_conv
	,cast(iburab as [DECIMAL](38, 4)) as iburab
	,cast(iburab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as iburab_conv
	,iburrf as iburrf
	,ltrim(rtrim(iburrf)) as iburrf_conv
	,ibuser as ibuser
	,ltrim(rtrim(ibuser)) as ibuser_conv
	,ibpid as ibpid
	,ltrim(rtrim(ibpid)) as ibpid_conv
	,ibjobn as ibjobn
	,ltrim(rtrim(ibjobn)) as ibjobn_conv
	,cast(ibupmj as [INT]) as ibupmj
	,case when cast(ibupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(ibupmj as [INT]) %1000, dateadd(year, cast(ibupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as ibupmj_conv
	,cast(ibtday as [DECIMAL](38, 4)) as ibtday
	,cast(ibtfla as [NVARCHAR](2)) as ibtfla
	,cast(ibcomh as [DECIMAL](38, 4)) as ibcomh
	,cast(ibavrt as [DECIMAL](38, 4)) as ibavrt
	,cast(ibavrt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as ibavrt_conv
	,cast(ibpoc as [NVARCHAR](1)) as ibpoc
	,ibaing as ibaing
	,ltrim(rtrim(ibaing)) as ibaing_conv
	,cast(ibbbdd as [DECIMAL](38, 4)) as ibbbdd
	,cast(ibbbdd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibbbdd_conv
	,cast(ibcmdm as [NVARCHAR](1)) as ibcmdm
	,cast(iblecm as [NVARCHAR](1)) as iblecm
	,cast(ibledd as [DECIMAL](38, 4)) as ibledd
	,cast(ibledd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibledd_conv
	,ibmlot as ibmlot
	,ltrim(rtrim(ibmlot)) as ibmlot_conv
	,cast(ibpefd as [DECIMAL](38, 4)) as ibpefd
	,cast(ibpefd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibpefd_conv
	,cast(ibsbdd as [DECIMAL](38, 4)) as ibsbdd
	,cast(ibsbdd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibsbdd_conv
	,cast(ibu1dd as [DECIMAL](38, 4)) as ibu1dd
	,cast(ibu1dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibu1dd_conv
	,cast(ibu2dd as [DECIMAL](38, 4)) as ibu2dd
	,cast(ibu2dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibu2dd_conv
	,cast(ibu3dd as [DECIMAL](38, 4)) as ibu3dd
	,cast(ibu3dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibu3dd_conv
	,cast(ibu4dd as [DECIMAL](38, 4)) as ibu4dd
	,cast(ibu4dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibu4dd_conv
	,cast(ibu5dd as [DECIMAL](38, 4)) as ibu5dd
	,cast(ibu5dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as ibu5dd_conv
	,ibxdck as ibxdck
	,ltrim(rtrim(ibxdck)) as ibxdck_conv
	,iblaf as iblaf
	,ltrim(rtrim(iblaf)) as iblaf_conv
	,ibltfm as ibltfm
	,ltrim(rtrim(ibltfm)) as ibltfm_conv
	,ibrwla as ibrwla
	,ltrim(rtrim(ibrwla)) as ibrwla_conv
	,iblnpa as iblnpa
	,ltrim(rtrim(iblnpa)) as iblnpa_conv
	,iblotc as iblotc
	,ltrim(rtrim(iblotc)) as iblotc_conv
	,cast(ibapsc as [NVARCHAR](1)) as ibapsc
	,cast(ibpri1 as [DECIMAL](38, 4)) as ibpri1
	,cast(ibpri1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibpri1_conv
	,cast(ibpri2 as [DECIMAL](38, 4)) as ibpri2
	,cast(ibpri2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibpri2_conv
	,cast(ibltcv as [DECIMAL](38, 4)) as ibltcv
	,cast(ibltcv as [DECIMAL](38, 7)) / cast(10000000 as [DECIMAL](38, 7)) as ibltcv_conv
	,ibashl as ibashl
	,ltrim(rtrim(ibashl)) as ibashl_conv
	,cast(ibopth as [DECIMAL](38, 4)) as ibopth
	,cast(ibopth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibopth_conv
	,cast(ibcuth as [DECIMAL](38, 4)) as ibcuth
	,cast(ibcuth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibcuth_conv
	,cast(ibumth as [NVARCHAR](3)) as ibumth
	,iblmfg as iblmfg
	,ltrim(rtrim(iblmfg)) as iblmfg_conv
	,ibline as ibline
	,ltrim(rtrim(ibline)) as ibline_conv
	,cast(ibdftpct as [DECIMAL](38, 4)) as ibdftpct
	,cast(ibdftpct as [DECIMAL](38, 5)) / cast(100000 as [DECIMAL](38, 5)) as ibdftpct_conv
	,ibkbit as ibkbit
	,ltrim(rtrim(ibkbit)) as ibkbit_conv
	,ibdfenditm as ibdfenditm
	,ltrim(rtrim(ibdfenditm)) as ibdfenditm_conv
	,ibkanexll as ibkanexll
	,ltrim(rtrim(ibkanexll)) as ibkanexll_conv
	,cast(ibscpsell as [NVARCHAR](1)) as ibscpsell
	,cast(ibmopth as [DECIMAL](38, 4)) as ibmopth
	,cast(ibmopth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibmopth_conv
	,cast(ibmcuth as [DECIMAL](38, 4)) as ibmcuth
	,cast(ibmcuth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as ibmcuth_conv
	,cast(ibcumth as [NVARCHAR](3)) as ibcumth
	,ibatprn as ibatprn
	,ltrim(rtrim(ibatprn)) as ibatprn_conv
	,cast(ibatpca as [NVARCHAR](1)) as ibatpca
	,cast(ibatpac as [NVARCHAR](5)) as ibatpac
	,ibcoore as ibcoore
	,ltrim(rtrim(ibcoore)) as ibcoore_conv
	,cast(ibvcpfc as [NVARCHAR](20)) as ibvcpfc
	,ibpnyn as ibpnyn
	,ltrim(rtrim(ibpnyn)) as ibpnyn_conv
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
	,ibitm
	,iblitm
	,ibaitm
	,ibmcu
	,ibsrp1
	,ibsrp2
	,ibsrp3
	,ibsrp4
	,ibsrp5
	,ibsrp6
	,ibsrp7
	,ibsrp8
	,ibsrp9
	,ibsrp0
	,ibprp1
	,ibprp2
	,ibprp3
	,ibprp4
	,ibprp5
	,ibprp6
	,ibprp7
	,ibprp8
	,ibprp9
	,ibprp0
	,ibcdcd
	,ibpdgr
	,ibdsgp
	,ibvend
	,ibanpl
	,ibbuyr
	,ibglpt
	,iborig
	,ibropi
	,ibroqi
	,ibrqmx
	,ibrqmn
	,ibwomo
	,ibserv
	,ibsafe
	,ibsld
	,ibckav
	,ibsrce
	,iblots
	,ibot1y
	,ibot2y
	,ibstdp
	,ibfrmp
	,ibthrp
	,ibstdg
	,ibfrgd
	,ibthgd
	,ibcoty
	,ibmmpc
	,ibprgr
	,ibrprc
	,iborpr
	,ibback
	,ibifla
	,ibabcs
	,ibabcm
	,ibabci
	,ibovr
	,ibshcm
	,ibcars
	,ibcarp
	,ibshcn
	,ibstkt
	,iblnty
	,ibfifo
	,ibcycl
	,ibinmg
	,ibwarr
	,ibsrnr
	,ibpctm
	,ibcmcg
	,ibfuf1
	,ibtx
	,ibtax1
	,ibmpst
	,ibmrpd
	,ibmrpc
	,ibupc
	,ibsns
	,ibmerl
	,ibltlv
	,ibltmf
	,ibltcm
	,ibopc
	,ibopv
	,ibacq
	,ibmlq
	,ibltpu
	,ibmpsp
	,ibmrpp
	,ibitc
	,ibeco
	,ibecty
	,ibecod
	,ibmtf1
	,ibmtf2
	,ibmtf3
	,ibmtf4
	,ibmtf5
	,ibmovd
	,ibqued
	,ibsetl
	,ibsrnk
	,ibsrkf
	,ibtimb
	,ibbqty
	,ibordw
	,ibexpd
	,ibdefd
	,ibmult
	,ibsflt
	,ibmake
	,iblfdj
	,ibllx
	,ibcmgl
	,iburcd
	,iburdt
	,iburat
	,iburab
	,iburrf
	,ibuser
	,ibpid
	,ibjobn
	,ibupmj
	,ibtday
	,ibtfla
	,ibcomh
	,ibavrt
	,ibpoc
	,ibaing
	,ibbbdd
	,ibcmdm
	,iblecm
	,ibledd
	,ibmlot
	,ibpefd
	,ibsbdd
	,ibu1dd
	,ibu2dd
	,ibu3dd
	,ibu4dd
	,ibu5dd
	,ibxdck
	,iblaf
	,ibltfm
	,ibrwla
	,iblnpa
	,iblotc
	,ibapsc
	,ibpri1
	,ibpri2
	,ibltcv
	,ibashl
	,ibopth
	,ibcuth
	,ibumth
	,iblmfg
	,ibline
	,ibdftpct
	,ibkbit
	,ibdfenditm
	,ibkanexll
	,ibscpsell
	,ibmopth
	,ibmcuth
	,ibcumth
	,ibatprn
	,ibatpca
	,ibatpac
	,ibcoore
	,ibvcpfc
	,ibpnyn,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde_qat.tmp_f4102_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde_qat.tmp_upsert_f4102_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4102_cdc_sys_integration_id ON stg_jde_qat.tmp_upsert_f4102_cdc(sys_integration_id); 
