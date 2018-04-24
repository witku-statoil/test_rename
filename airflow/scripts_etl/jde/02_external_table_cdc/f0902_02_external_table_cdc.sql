--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f0902_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f0902_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f0902_cdc
(
	[sys_file_name] [NVARCHAR](100) 
	,[sys_file_ln] [VARCHAR](20) 
	,[sys_integration_id] [NVARCHAR](200) 
	,[sys_extract_dt] [NVARCHAR](40) 
	,[sys_cdc_dt] [NVARCHAR](40) 
	,[sys_cdc_scn] [NVARCHAR](14) 
	,[sys_cdc_operation_type] [NVARCHAR](15) 
	,[sys_cdc_before_after] [NVARCHAR](10) 
	,[sys_line_modified_ind] [VARCHAR](20) 
	,[gbaid] [NVARCHAR](8) 
	,[gbctry] [DECIMAL](38, 4) 
	,[gbfy] [DECIMAL](38, 4) 
	,[gbfq] [NVARCHAR](4) 
	,[gblt] [NVARCHAR](2) 
	,[gbsbl] [NVARCHAR](8) 
	,[gbco] [NVARCHAR](5) 
	,[gbapyc] [DECIMAL](38, 4) 
	,[gban01] [DECIMAL](38, 4) 
	,[gban02] [DECIMAL](38, 4) 
	,[gban03] [DECIMAL](38, 4) 
	,[gban04] [DECIMAL](38, 4) 
	,[gban05] [DECIMAL](38, 4) 
	,[gban06] [DECIMAL](38, 4) 
	,[gban07] [DECIMAL](38, 4) 
	,[gban08] [DECIMAL](38, 4) 
	,[gban09] [DECIMAL](38, 4) 
	,[gban10] [DECIMAL](38, 4) 
	,[gban11] [DECIMAL](38, 4) 
	,[gban12] [DECIMAL](38, 4) 
	,[gban13] [DECIMAL](38, 4) 
	,[gban14] [DECIMAL](38, 4) 
	,[gbapyn] [DECIMAL](38, 4) 
	,[gbawtd] [DECIMAL](38, 4) 
	,[gbborg] [DECIMAL](38, 4) 
	,[gbpou] [DECIMAL](38, 4) 
	,[gbpc] [DECIMAL](38, 4) 
	,[gbtker] [DECIMAL](38, 4) 
	,[gbbreq] [DECIMAL](38, 4) 
	,[gbbapr] [DECIMAL](38, 4) 
	,[gbmcu] [NVARCHAR](12) 
	,[gbobj] [NVARCHAR](6) 
	,[gbsub] [NVARCHAR](8) 
	,[gbuser] [NVARCHAR](10) 
	,[gbpid] [NVARCHAR](10) 
	,[gbupmj] [VARCHAR](20) 
	,[gbjobn] [NVARCHAR](10) 
	,[gbsblt] [NVARCHAR](1) 
	,[gbupmt] [DECIMAL](38, 4) 
	,[gbcrcd] [NVARCHAR](3) 
	,[gbcrcx] [NVARCHAR](3) 
	,[gbprgf] [NVARCHAR](1) 
	,[gband01] [DECIMAL](38, 4) 
	,[gband02] [DECIMAL](38, 4) 
	,[gband03] [DECIMAL](38, 4) 
	,[gband04] [DECIMAL](38, 4) 
	,[gband05] [DECIMAL](38, 4) 
	,[gband06] [DECIMAL](38, 4) 
	,[gband07] [DECIMAL](38, 4) 
	,[gband08] [DECIMAL](38, 4) 
	,[gband09] [DECIMAL](38, 4) 
	,[gband10] [DECIMAL](38, 4) 
	,[gband11] [DECIMAL](38, 4) 
	,[gband12] [DECIMAL](38, 4) 
	,[gband13] [DECIMAL](38, 4) 
	,[gband14] [DECIMAL](38, 4) 
)
WITH
(
    LOCATION='processing-clean/jde/f0902/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
