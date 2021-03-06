--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f550006_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f550006_init

CREATE EXTERNAL TABLE stg_jde.ext_f550006_init
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
	,[pmy55prmcd] [NVARCHAR](16) 
	,[pmprnumb] [DECIMAL](38, 4) 
	,[pmy55prmty] [NVARCHAR](8) 
	,[pmy55cmpcd] [NVARCHAR](16) 
	,[pmcmpid] [DECIMAL](38, 4) 
	,[pmdl01] [NVARCHAR](30) 
	,[pmlngp1] [NVARCHAR](2) 
	,[pmstrt] [VARCHAR](20) 
	,[pmwwast] [DECIMAL](38, 4) 
	,[pmwwaet] [DECIMAL](38, 4) 
	,[pmenddj] [VARCHAR](20) 
	,[pmctr] [NVARCHAR](3) 
	,[pmcrcd] [NVARCHAR](3) 
	,[pmtxky] [NVARCHAR](254) 
	,[pmy55dur] [NVARCHAR](10) 
	,[pmy55hrs] [DECIMAL](38, 4) 
	,[pmy55prity] [NVARCHAR](6) 
	,[pmy55plcmt] [NVARCHAR](6) 
	,[pmy55sldts] [VARCHAR](20) 
	,[pmy55sldte] [VARCHAR](20) 
	,[pmy55prdts] [VARCHAR](20) 
	,[pmy55prdte] [VARCHAR](20) 
	,[pmy55cpflg] [NVARCHAR](1) 
	,[pmy55cptyp] [NVARCHAR](6) 
	,[pmy55srst] [NVARCHAR](2) 
	,[pmy55ntst] [NVARCHAR](2) 
	,[pmy55psusd] [VARCHAR](20) 
	,[pmy55pcand] [VARCHAR](20) 
	,[pmurab] [DECIMAL](38, 4) 
	,[pmurrf] [NVARCHAR](15) 
	,[pmurdt] [VARCHAR](20) 
	,[pmurat] [DECIMAL](38, 4) 
	,[pmurcd] [NVARCHAR](2) 
	,[pmcrtu] [NVARCHAR](10) 
	,[pmcrdj] [VARCHAR](20) 
	,[pmc9crp] [NVARCHAR](10) 
	,[pmupmj] [VARCHAR](20) 
	,[pmupmt] [DECIMAL](38, 4) 
	,[pmjobn] [NVARCHAR](10) 
	,[pmpid] [NVARCHAR](10) 
	,[pmuser] [NVARCHAR](10) 
	,[pmy55alapr] [NVARCHAR](1) 
	,[pmy55strg1] [NVARCHAR](30) 
	,[pmy55char1] [NVARCHAR](1) 
	,[pmy55char2] [NVARCHAR](1) 
	,[pmy55strg2] [NVARCHAR](30) 
	,[pmy55amnt1] [DECIMAL](38, 4) 
	,[pmy55amnt2] [DECIMAL](38, 4) 
	,[pmy55time1] [DECIMAL](38, 4) 
	,[pmy55time2] [DECIMAL](38, 4) 
	,[pmy55date1] [VARCHAR](20) 
	,[pmy55date2] [VARCHAR](20) 
)
WITH
(
    LOCATION='processing-clean/jde/f550006/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
