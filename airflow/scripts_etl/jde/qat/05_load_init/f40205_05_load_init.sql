-- Create rep new table for init
IF OBJECT_ID('rep_jde_qat.f40205_new') IS NOT NULL
    DROP TABLE rep_jde_qat.f40205_new


CREATE TABLE rep_jde_qat.f40205_new
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
	,lflnty as lflnty
	,ltrim(rtrim(lflnty)) as lflnty_conv
	,lflnds as lflnds
	,ltrim(rtrim(lflnds)) as lflnds_conv
	,lflnd2 as lflnd2
	,ltrim(rtrim(lflnd2)) as lflnd2_conv
	,lfgli as lfgli
	,ltrim(rtrim(lfgli)) as lfgli_conv
	,cast(lfivi as [NVARCHAR](1)) as lfivi
	,lfari as lfari
	,ltrim(rtrim(lfari)) as lfari_conv
	,lfapi as lfapi
	,ltrim(rtrim(lfapi)) as lfapi_conv
	,lfrsgn as lfrsgn
	,ltrim(rtrim(lfrsgn)) as lfrsgn_conv
	,lftxyn as lftxyn
	,ltrim(rtrim(lftxyn)) as lftxyn_conv
	,lfprft as lfprft
	,ltrim(rtrim(lfprft)) as lfprft_conv
	,lfcdsc as lfcdsc
	,ltrim(rtrim(lfcdsc)) as lfcdsc_conv
	,cast(lftx01 as [NVARCHAR](1)) as lftx01
	,cast(lftx02 as [NVARCHAR](1)) as lftx02
	,lfglc as lfglc
	,ltrim(rtrim(lfglc)) as lfglc_conv
	,lfpdc1 as lfpdc1
	,ltrim(rtrim(lfpdc1)) as lfpdc1_conv
	,lfpdc2 as lfpdc2
	,ltrim(rtrim(lfpdc2)) as lfpdc2_conv
	,lfpdc3 as lfpdc3
	,ltrim(rtrim(lfpdc3)) as lfpdc3_conv
	,lfpdc4 as lfpdc4
	,ltrim(rtrim(lfpdc4)) as lfpdc4_conv
	,lfidc1 as lfidc1
	,ltrim(rtrim(lfidc1)) as lfidc1_conv
	,cast(lfidc2 as [NVARCHAR](1)) as lfidc2
	,lfidc3 as lfidc3
	,ltrim(rtrim(lfidc3)) as lfidc3_conv
	,lfidc4 as lfidc4
	,ltrim(rtrim(lfidc4)) as lfidc4_conv
	,cast(lfcsj as [NVARCHAR](1)) as lfcsj
	,cast(lfdcto as [NVARCHAR](2)) as lfdcto
	,lfart as lfart
	,ltrim(rtrim(lfart)) as lfart_conv
	,lfaft as lfaft
	,ltrim(rtrim(lfaft)) as lfaft_conv
	,lfgwo as lfgwo
	,ltrim(rtrim(lfgwo)) as lfgwo_conv
	,lfexvr as lfexvr
	,ltrim(rtrim(lfexvr)) as lfexvr_conv
	,lfcmi as lfcmi
	,ltrim(rtrim(lfcmi)) as lfcmi_conv
	,lfprrq as lfprrq
	,ltrim(rtrim(lfprrq)) as lfprrq_conv
	,lfev01 as lfev01
	,ltrim(rtrim(lfev01)) as lfev01_conv
	,lfev02 as lfev02
	,ltrim(rtrim(lfev02)) as lfev02_conv
	,lfev03 as lfev03
	,ltrim(rtrim(lfev03)) as lfev03_conv
	,lfev04 as lfev04
	,ltrim(rtrim(lfev04)) as lfev04_conv
	,lfev05 as lfev05
	,ltrim(rtrim(lfev05)) as lfev05_conv
	,cast(lfnewr as [NVARCHAR](1)) as lfnewr
	,lfsruf as lfsruf
	,ltrim(rtrim(lfsruf)) as lfsruf_conv
	,cast(lfnbru as [NVARCHAR](1)) as lfnbru
FROM 
    stg_jde_qat.tmp_f40205_init
OPTION ( LABEL = 'CREATE_rep_jde_qat.f40205_new AF:{{ task_instance_key_str }}' ) 

CREATE STATISTICS stat_f40205_sys_integration_id ON rep_jde_qat.f40205_new(sys_integration_id); 
