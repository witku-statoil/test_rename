-- Create upsert table for cdc
IF OBJECT_ID('stg_jde.tmp_upsert_f4101_cdc') IS NOT NULL
    DROP TABLE stg_jde.tmp_upsert_f4101_cdc


CREATE TABLE stg_jde.tmp_upsert_f4101_cdc
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
	,cast(imitm as [DECIMAL](38, 4)) as imitm
	,imlitm as imlitm
	,ltrim(rtrim(imlitm)) as imlitm_conv
	,imaitm as imaitm
	,ltrim(rtrim(imaitm)) as imaitm_conv
	,imdsc1 as imdsc1
	,ltrim(rtrim(imdsc1)) as imdsc1_conv
	,imdsc2 as imdsc2
	,ltrim(rtrim(imdsc2)) as imdsc2_conv
	,imsrtx as imsrtx
	,ltrim(rtrim(imsrtx)) as imsrtx_conv
	,imaln as imaln
	,ltrim(rtrim(imaln)) as imaln_conv
	,imsrp1 as imsrp1
	,stg_jde.prc_decode_f0005_41('41','S1',ltrim(rtrim(imsrp1)))  as imsrp1_desc
	,imsrp2 as imsrp2
	,stg_jde.prc_decode_f0005_41('41','S2',ltrim(rtrim(imsrp2)))  as imsrp2_desc
	,imsrp3 as imsrp3
	,stg_jde.prc_decode_f0005_41('41','S3',ltrim(rtrim(imsrp3)))  as imsrp3_desc
	,imsrp4 as imsrp4
	,stg_jde.prc_decode_f0005_41('41','S4',ltrim(rtrim(imsrp4)))  as imsrp4_desc
	,imsrp5 as imsrp5
	,stg_jde.prc_decode_f0005_41('41','S5',ltrim(rtrim(imsrp5)))  as imsrp5_desc
	,imsrp6 as imsrp6
	,stg_jde.prc_decode_f0005_41('41','06',ltrim(rtrim(imsrp6)))  as imsrp6_desc
	,imsrp7 as imsrp7
	,stg_jde.prc_decode_f0005_41('41','07',ltrim(rtrim(imsrp7)))  as imsrp7_desc
	,cast(imsrp8 as [NVARCHAR](6)) as imsrp8
	,cast(imsrp9 as [NVARCHAR](6)) as imsrp9
	,cast(imsrp0 as [NVARCHAR](6)) as imsrp0
	,cast(imprp1 as [NVARCHAR](3)) as imprp1
	,cast(imprp2 as [NVARCHAR](3)) as imprp2
	,cast(imprp3 as [NVARCHAR](3)) as imprp3
	,cast(imprp4 as [NVARCHAR](3)) as imprp4
	,cast(imprp5 as [NVARCHAR](3)) as imprp5
	,cast(imprp6 as [NVARCHAR](6)) as imprp6
	,cast(imprp7 as [NVARCHAR](6)) as imprp7
	,cast(imprp8 as [NVARCHAR](6)) as imprp8
	,cast(imprp9 as [NVARCHAR](6)) as imprp9
	,cast(imprp0 as [NVARCHAR](6)) as imprp0
	,imcdcd as imcdcd
	,ltrim(rtrim(imcdcd)) as imcdcd_conv
	,cast(impdgr as [NVARCHAR](3)) as impdgr
	,cast(imdsgp as [NVARCHAR](3)) as imdsgp
	,cast(imprgr as [NVARCHAR](8)) as imprgr
	,cast(imrprc as [NVARCHAR](8)) as imrprc
	,cast(imorpr as [NVARCHAR](8)) as imorpr
	,cast(imbuyr as [DECIMAL](38, 4)) as imbuyr
	,cast(imbuyr as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imbuyr_conv
	,imdraw as imdraw
	,ltrim(rtrim(imdraw)) as imdraw_conv
	,imrvno as imrvno
	,ltrim(rtrim(imrvno)) as imrvno_conv
	,imdsze as imdsze
	,ltrim(rtrim(imdsze)) as imdsze_conv
	,cast(imvcud as [DECIMAL](38, 4)) as imvcud
	,cast(imvcud as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as imvcud_conv
	,cast(imcars as [DECIMAL](38, 4)) as imcars
	,cast(imcars as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imcars_conv
	,cast(imcarp as [DECIMAL](38, 4)) as imcarp
	,cast(imcarp as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imcarp_conv
	,cast(imshcn as [NVARCHAR](3)) as imshcn
	,cast(imshcm as [NVARCHAR](3)) as imshcm
	,cast(imuom1 as [NVARCHAR](2)) as imuom1
	,cast(imuom2 as [NVARCHAR](2)) as imuom2
	,cast(imuom3 as [NVARCHAR](2)) as imuom3
	,cast(imuom4 as [NVARCHAR](2)) as imuom4
	,cast(imuom6 as [NVARCHAR](2)) as imuom6
	,cast(imuom8 as [NVARCHAR](2)) as imuom8
	,cast(imuom9 as [NVARCHAR](2)) as imuom9
	,cast(imuwum as [NVARCHAR](2)) as imuwum
	,cast(imuvm1 as [NVARCHAR](2)) as imuvm1
	,cast(imsutm as [NVARCHAR](2)) as imsutm
	,cast(imumvw as [NVARCHAR](1)) as imumvw
	,cast(imcycl as [NVARCHAR](3)) as imcycl
	,imglpt as imglpt
	,stg_jde.prc_decode_f0005_41('41','9',ltrim(rtrim(imglpt)))  as imglpt_desc
	,cast(implev as [NVARCHAR](1)) as implev
	,cast(impplv as [NVARCHAR](1)) as impplv
	,cast(imclev as [NVARCHAR](1)) as imclev
	,cast(imprpo as [NVARCHAR](1)) as imprpo
	,imckav as imckav
	,ltrim(rtrim(imckav)) as imckav_conv
	,cast(imbpfg as [NVARCHAR](1)) as imbpfg
	,cast(imsrce as [NVARCHAR](1)) as imsrce
	,imot1y as imot1y
	,ltrim(rtrim(imot1y)) as imot1y_conv
	,imot2y as imot2y
	,ltrim(rtrim(imot2y)) as imot2y_conv
	,cast(imstdp as [DECIMAL](38, 4)) as imstdp
	,cast(imstdp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as imstdp_conv
	,cast(imfrmp as [DECIMAL](38, 4)) as imfrmp
	,cast(imfrmp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as imfrmp_conv
	,cast(imthrp as [DECIMAL](38, 4)) as imthrp
	,cast(imthrp as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as imthrp_conv
	,cast(imstdg as [NVARCHAR](3)) as imstdg
	,cast(imfrgd as [NVARCHAR](3)) as imfrgd
	,cast(imthgd as [NVARCHAR](3)) as imthgd
	,cast(imcoty as [NVARCHAR](1)) as imcoty
	,cast(imstkt as [NVARCHAR](1)) as imstkt
	,imlnty as imlnty
	,ltrim(rtrim(imlnty)) as imlnty_conv
	,imcont as imcont
	,ltrim(rtrim(imcont)) as imcont_conv
	,imback as imback
	,ltrim(rtrim(imback)) as imback_conv
	,cast(imifla as [NVARCHAR](2)) as imifla
	,cast(imtfla as [NVARCHAR](2)) as imtfla
	,cast(iminmg as [NVARCHAR](10)) as iminmg
	,cast(imabcs as [NVARCHAR](1)) as imabcs
	,cast(imabcm as [NVARCHAR](1)) as imabcm
	,cast(imabci as [NVARCHAR](1)) as imabci
	,imovr as imovr
	,ltrim(rtrim(imovr)) as imovr_conv
	,imwarr as imwarr
	,ltrim(rtrim(imwarr)) as imwarr_conv
	,imcmcg as imcmcg
	,ltrim(rtrim(imcmcg)) as imcmcg_conv
	,cast(imsrnr as [NVARCHAR](1)) as imsrnr
	,cast(impmth as [NVARCHAR](1)) as impmth
	,imfifo as imfifo
	,ltrim(rtrim(imfifo)) as imfifo_conv
	,cast(imlots as [NVARCHAR](1)) as imlots
	,cast(imsld as [DECIMAL](38, 4)) as imsld
	,cast(imsld as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imsld_conv
	,cast(imanpl as [DECIMAL](38, 4)) as imanpl
	,cast(immpst as [NVARCHAR](1)) as immpst
	,cast(impctm as [DECIMAL](38, 4)) as impctm
	,cast(impctm as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as impctm_conv
	,cast(immmpc as [DECIMAL](38, 4)) as immmpc
	,cast(immmpc as [DECIMAL](38, 3)) / cast(1000 as [DECIMAL](38, 3)) as immmpc_conv
	,cast(imptsc as [NVARCHAR](2)) as imptsc
	,cast(imsns as [NVARCHAR](1)) as imsns
	,cast(imltlv as [DECIMAL](38, 4)) as imltlv
	,cast(imltlv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imltlv_conv
	,cast(imltmf as [DECIMAL](38, 4)) as imltmf
	,cast(imltmf as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imltmf_conv
	,cast(imltcm as [DECIMAL](38, 4)) as imltcm
	,cast(imltcm as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imltcm_conv
	,cast(imopc as [NVARCHAR](1)) as imopc
	,cast(imopv as [DECIMAL](38, 4)) as imopv
	,cast(imopv as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imopv_conv
	,cast(imacq as [DECIMAL](38, 4)) as imacq
	,cast(imacq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as imacq_conv
	,cast(immlq as [DECIMAL](38, 4)) as immlq
	,cast(immlq as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as immlq_conv
	,cast(imltpu as [DECIMAL](38, 4)) as imltpu
	,cast(imltpu as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as imltpu_conv
	,cast(immpsp as [NVARCHAR](1)) as immpsp
	,cast(immrpp as [NVARCHAR](1)) as immrpp
	,cast(imitc as [NVARCHAR](1)) as imitc
	,imordw as imordw
	,ltrim(rtrim(imordw)) as imordw_conv
	,cast(immtf1 as [DECIMAL](38, 4)) as immtf1
	,cast(immtf1 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as immtf1_conv
	,cast(immtf2 as [DECIMAL](38, 4)) as immtf2
	,cast(immtf2 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as immtf2_conv
	,cast(immtf3 as [DECIMAL](38, 4)) as immtf3
	,cast(immtf3 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as immtf3_conv
	,cast(immtf4 as [DECIMAL](38, 4)) as immtf4
	,cast(immtf4 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as immtf4_conv
	,cast(immtf5 as [DECIMAL](38, 4)) as immtf5
	,cast(immtf5 as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as immtf5_conv
	,cast(imexpd as [DECIMAL](38, 4)) as imexpd
	,cast(imexpd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imexpd_conv
	,cast(imdefd as [DECIMAL](38, 4)) as imdefd
	,cast(imdefd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imdefd_conv
	,cast(imsflt as [DECIMAL](38, 4)) as imsflt
	,cast(imsflt as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imsflt_conv
	,cast(immake as [NVARCHAR](1)) as immake
	,cast(imcoby as [NVARCHAR](1)) as imcoby
	,cast(imllx as [DECIMAL](38, 4)) as imllx
	,cast(imcmgl as [NVARCHAR](1)) as imcmgl
	,cast(imcomh as [DECIMAL](38, 4)) as imcomh
	,imurcd as imurcd
	,ltrim(rtrim(imurcd)) as imurcd_conv
	,cast(imurdt as [INT]) as imurdt
	,case when cast(imurdt as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(imurdt as [INT]) %1000, dateadd(year, cast(imurdt as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as imurdt_conv
	,cast(imurat as [DECIMAL](38, 4)) as imurat
	,cast(imurat as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as imurat_conv
	,cast(imurab as [DECIMAL](38, 4)) as imurab
	,cast(imurab as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imurab_conv
	,imurrf as imurrf
	,ltrim(rtrim(imurrf)) as imurrf_conv
	,imuser as imuser
	,ltrim(rtrim(imuser)) as imuser_conv
	,impid as impid
	,ltrim(rtrim(impid)) as impid_conv
	,imjobn as imjobn
	,ltrim(rtrim(imjobn)) as imjobn_conv
	,cast(imupmj as [INT]) as imupmj
	,case when cast(imupmj as [INT]) <= 0 then convert(datetime, '1900-01-01', 120) else dateadd(day, cast(imupmj as [INT]) %1000, dateadd(year, cast(imupmj as [INT])/ 1000, convert(datetime, '1899-12-31', 120))) end as imupmj_conv
	,cast(imtday as [DECIMAL](38, 4)) as imtday
	,imupcn as imupcn
	,ltrim(rtrim(imupcn)) as imupcn_conv
	,imscc0 as imscc0
	,ltrim(rtrim(imscc0)) as imscc0_conv
	,cast(imumup as [NVARCHAR](2)) as imumup
	,cast(imumdf as [NVARCHAR](2)) as imumdf
	,cast(imums0 as [NVARCHAR](2)) as imums0
	,cast(imums1 as [NVARCHAR](2)) as imums1
	,cast(imums2 as [NVARCHAR](2)) as imums2
	,cast(imums3 as [NVARCHAR](2)) as imums3
	,cast(imums4 as [NVARCHAR](2)) as imums4
	,cast(imums5 as [NVARCHAR](2)) as imums5
	,cast(imums6 as [NVARCHAR](2)) as imums6
	,cast(imums7 as [NVARCHAR](2)) as imums7
	,cast(imums8 as [NVARCHAR](2)) as imums8
	,cast(impoc as [NVARCHAR](1)) as impoc
	,cast(imavrt as [DECIMAL](38, 4)) as imavrt
	,cast(imavrt as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as imavrt_conv
	,cast(imeqty as [NVARCHAR](5)) as imeqty
	,imwtrq as imwtrq
	,ltrim(rtrim(imwtrq)) as imwtrq_conv
	,imtmpl as imtmpl
	,ltrim(rtrim(imtmpl)) as imtmpl_conv
	,imseg1 as imseg1
	,ltrim(rtrim(imseg1)) as imseg1_conv
	,imseg2 as imseg2
	,ltrim(rtrim(imseg2)) as imseg2_conv
	,imseg3 as imseg3
	,ltrim(rtrim(imseg3)) as imseg3_conv
	,imseg4 as imseg4
	,ltrim(rtrim(imseg4)) as imseg4_conv
	,imseg5 as imseg5
	,ltrim(rtrim(imseg5)) as imseg5_conv
	,imseg6 as imseg6
	,ltrim(rtrim(imseg6)) as imseg6_conv
	,imseg7 as imseg7
	,ltrim(rtrim(imseg7)) as imseg7_conv
	,imseg8 as imseg8
	,ltrim(rtrim(imseg8)) as imseg8_conv
	,imseg9 as imseg9
	,ltrim(rtrim(imseg9)) as imseg9_conv
	,imseg0 as imseg0
	,ltrim(rtrim(imseg0)) as imseg0_conv
	,cast(immic as [NVARCHAR](1)) as immic
	,imaing as imaing
	,ltrim(rtrim(imaing)) as imaing_conv
	,cast(imbbdd as [DECIMAL](38, 4)) as imbbdd
	,cast(imbbdd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imbbdd_conv
	,cast(imcmdm as [NVARCHAR](1)) as imcmdm
	,cast(imlecm as [NVARCHAR](1)) as imlecm
	,cast(imledd as [DECIMAL](38, 4)) as imledd
	,cast(imledd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imledd_conv
	,cast(impefd as [DECIMAL](38, 4)) as impefd
	,cast(impefd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as impefd_conv
	,cast(imsbdd as [DECIMAL](38, 4)) as imsbdd
	,cast(imsbdd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imsbdd_conv
	,cast(imu1dd as [DECIMAL](38, 4)) as imu1dd
	,cast(imu1dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imu1dd_conv
	,cast(imu2dd as [DECIMAL](38, 4)) as imu2dd
	,cast(imu2dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imu2dd_conv
	,cast(imu3dd as [DECIMAL](38, 4)) as imu3dd
	,cast(imu3dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imu3dd_conv
	,cast(imu4dd as [DECIMAL](38, 4)) as imu4dd
	,cast(imu4dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imu4dd_conv
	,cast(imu5dd as [DECIMAL](38, 4)) as imu5dd
	,cast(imu5dd as [DECIMAL](38, 0)) / cast(1 as [DECIMAL](38, 0)) as imu5dd_conv
	,cast(imdltl as [DECIMAL](38, 4)) as imdltl
	,cast(imdltl as [DECIMAL](38, 2)) / cast(100 as [DECIMAL](38, 2)) as imdltl_conv
	,imdppo as imdppo
	,ltrim(rtrim(imdppo)) as imdppo_conv
	,imdual as imdual
	,ltrim(rtrim(imdual)) as imdual_conv
	,imxdck as imxdck
	,ltrim(rtrim(imxdck)) as imxdck_conv
	,imlaf as imlaf
	,ltrim(rtrim(imlaf)) as imlaf_conv
	,imltfm as imltfm
	,ltrim(rtrim(imltfm)) as imltfm_conv
	,imrwla as imrwla
	,ltrim(rtrim(imrwla)) as imrwla_conv
	,imlnpa as imlnpa
	,ltrim(rtrim(imlnpa)) as imlnpa_conv
	,imlotc as imlotc
	,ltrim(rtrim(imlotc)) as imlotc_conv
	,cast(imapsc as [NVARCHAR](1)) as imapsc
	,imauom as imauom
	,ltrim(rtrim(imauom)) as imauom_conv
	,imconb as imconb
	,ltrim(rtrim(imconb)) as imconb_conv
	,imgcmp as imgcmp
	,ltrim(rtrim(imgcmp)) as imgcmp_conv
	,cast(impri1 as [DECIMAL](38, 4)) as impri1
	,cast(impri1 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as impri1_conv
	,cast(impri2 as [DECIMAL](38, 4)) as impri2
	,cast(impri2 as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as impri2_conv
	,imashl as imashl
	,ltrim(rtrim(imashl)) as imashl_conv
	,imvminv as imvminv
	,ltrim(rtrim(imvminv)) as imvminv_conv
	,cast(imcmeth as [NVARCHAR](1)) as imcmeth
	,imexpi as imexpi
	,ltrim(rtrim(imexpi)) as imexpi_conv
	,cast(imopth as [DECIMAL](38, 4)) as imopth
	,cast(imopth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as imopth_conv
	,cast(imcuth as [DECIMAL](38, 4)) as imcuth
	,cast(imcuth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as imcuth_conv
	,cast(imumth as [NVARCHAR](3)) as imumth
	,imlmfg as imlmfg
	,ltrim(rtrim(imlmfg)) as imlmfg_conv
	,imline as imline
	,ltrim(rtrim(imline)) as imline_conv
	,cast(imdftpct as [DECIMAL](38, 4)) as imdftpct
	,cast(imdftpct as [DECIMAL](38, 5)) / cast(100000 as [DECIMAL](38, 5)) as imdftpct_conv
	,imkbit as imkbit
	,ltrim(rtrim(imkbit)) as imkbit_conv
	,imdfenditm as imdfenditm
	,ltrim(rtrim(imdfenditm)) as imdfenditm_conv
	,imkanexll as imkanexll
	,ltrim(rtrim(imkanexll)) as imkanexll_conv
	,cast(imscpsell as [NVARCHAR](1)) as imscpsell
	,cast(immopth as [DECIMAL](38, 4)) as immopth
	,cast(immopth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as immopth_conv
	,cast(immcuth as [DECIMAL](38, 4)) as immcuth
	,cast(immcuth as [DECIMAL](38, 4)) / cast(10000 as [DECIMAL](38, 4)) as immcuth_conv
	,cast(imcumth as [NVARCHAR](3)) as imcumth
	,imatprn as imatprn
	,ltrim(rtrim(imatprn)) as imatprn_conv
	,cast(imatpca as [NVARCHAR](1)) as imatpca
	,cast(imatpac as [NVARCHAR](5)) as imatpac
	,imcoore as imcoore
	,ltrim(rtrim(imcoore)) as imcoore_conv
	,cast(imvcpfc as [NVARCHAR](20)) as imvcpfc
	,impnyn as impnyn
	,ltrim(rtrim(impnyn)) as impnyn_conv
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
	,imitm
	,imlitm
	,imaitm
	,imdsc1
	,imdsc2
	,imsrtx
	,imaln
	,imsrp1
	,imsrp2
	,imsrp3
	,imsrp4
	,imsrp5
	,imsrp6
	,imsrp7
	,imsrp8
	,imsrp9
	,imsrp0
	,imprp1
	,imprp2
	,imprp3
	,imprp4
	,imprp5
	,imprp6
	,imprp7
	,imprp8
	,imprp9
	,imprp0
	,imcdcd
	,impdgr
	,imdsgp
	,imprgr
	,imrprc
	,imorpr
	,imbuyr
	,imdraw
	,imrvno
	,imdsze
	,imvcud
	,imcars
	,imcarp
	,imshcn
	,imshcm
	,imuom1
	,imuom2
	,imuom3
	,imuom4
	,imuom6
	,imuom8
	,imuom9
	,imuwum
	,imuvm1
	,imsutm
	,imumvw
	,imcycl
	,imglpt
	,implev
	,impplv
	,imclev
	,imprpo
	,imckav
	,imbpfg
	,imsrce
	,imot1y
	,imot2y
	,imstdp
	,imfrmp
	,imthrp
	,imstdg
	,imfrgd
	,imthgd
	,imcoty
	,imstkt
	,imlnty
	,imcont
	,imback
	,imifla
	,imtfla
	,iminmg
	,imabcs
	,imabcm
	,imabci
	,imovr
	,imwarr
	,imcmcg
	,imsrnr
	,impmth
	,imfifo
	,imlots
	,imsld
	,imanpl
	,immpst
	,impctm
	,immmpc
	,imptsc
	,imsns
	,imltlv
	,imltmf
	,imltcm
	,imopc
	,imopv
	,imacq
	,immlq
	,imltpu
	,immpsp
	,immrpp
	,imitc
	,imordw
	,immtf1
	,immtf2
	,immtf3
	,immtf4
	,immtf5
	,imexpd
	,imdefd
	,imsflt
	,immake
	,imcoby
	,imllx
	,imcmgl
	,imcomh
	,imurcd
	,imurdt
	,imurat
	,imurab
	,imurrf
	,imuser
	,impid
	,imjobn
	,imupmj
	,imtday
	,imupcn
	,imscc0
	,imumup
	,imumdf
	,imums0
	,imums1
	,imums2
	,imums3
	,imums4
	,imums5
	,imums6
	,imums7
	,imums8
	,impoc
	,imavrt
	,imeqty
	,imwtrq
	,imtmpl
	,imseg1
	,imseg2
	,imseg3
	,imseg4
	,imseg5
	,imseg6
	,imseg7
	,imseg8
	,imseg9
	,imseg0
	,immic
	,imaing
	,imbbdd
	,imcmdm
	,imlecm
	,imledd
	,impefd
	,imsbdd
	,imu1dd
	,imu2dd
	,imu3dd
	,imu4dd
	,imu5dd
	,imdltl
	,imdppo
	,imdual
	,imxdck
	,imlaf
	,imltfm
	,imrwla
	,imlnpa
	,imlotc
	,imapsc
	,imauom
	,imconb
	,imgcmp
	,impri1
	,impri2
	,imashl
	,imvminv
	,imcmeth
	,imexpi
	,imopth
	,imcuth
	,imumth
	,imlmfg
	,imline
	,imdftpct
	,imkbit
	,imdfenditm
	,imkanexll
	,imscpsell
	,immopth
	,immcuth
	,imcumth
	,imatprn
	,imatpca
	,imatpac
	,imcoore
	,imvcpfc
	,impnyn,
        ROW_NUMBER() OVER (PARTITION BY f.sys_integration_id ORDER BY f.sys_cdc_scn DESC) RNUM 
    FROM 
        stg_jde.tmp_f4101_cdc f
    WHERE 1=1
        AND LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('INSERT','SQL CO','SQL COMPUPDATE','PK UPDATE') 
        -- below can be changed to exclude all "before" rows? 
        AND NOT (LTRIM(RTRIM(f.sys_cdc_operation_type)) IN ('SQL CO','SQL COMPUPDATE') 
                AND f.sys_cdc_before_after='BEFORE' 
                ) 
    ) c
WHERE 
    c.RNUM=1
OPTION ( LABEL = 'CREATE_stg_jde.tmp_upsert_f4101_cdc AF:{{ task_instance_key_str }}' ) 


CREATE STATISTICS stat_tmp_upsert_f4101_cdc_sys_integration_id ON stg_jde.tmp_upsert_f4101_cdc(sys_integration_id); 
