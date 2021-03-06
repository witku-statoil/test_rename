--Create ext table for cdc
IF OBJECT_ID('stg_jde.ext_f1217_cdc') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f1217_cdc


CREATE EXTERNAL TABLE stg_jde.ext_f1217_cdc
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
	,[wrnumb] [DECIMAL](38, 4) 
	,[wrctr] [NVARCHAR](3) 
	,[wrcoown] [NVARCHAR](1) 
	,[wrmmcu] [NVARCHAR](12) 
	,[wrdoco] [DECIMAL](38, 4) 
	,[wrdcto] [NVARCHAR](2) 
	,[wrkcoo] [NVARCHAR](5) 
	,[wrlnid] [DECIMAL](38, 4) 
	,[wrordj] [VARCHAR](20) 
	,[wrshpj] [VARCHAR](20) 
	,[wraddj] [VARCHAR](20) 
	,[wrshan] [DECIMAL](38, 4) 
	,[wrpa8] [DECIMAL](38, 4) 
	,[wrslsm] [DECIMAL](38, 4) 
	,[wranob] [DECIMAL](38, 4) 
	,[wrrorn] [NVARCHAR](8) 
	,[wrrcto] [NVARCHAR](2) 
	,[wrstrx] [VARCHAR](20) 
	,[wrvend] [DECIMAL](38, 4) 
	,[wroorn] [NVARCHAR](8) 
	,[wrocto] [NVARCHAR](2) 
	,[wrokco] [NVARCHAR](5) 
	,[wrsfxo] [NVARCHAR](3) 
	,[wrogno] [DECIMAL](38, 4) 
	,[wrprodf] [NVARCHAR](8) 
	,[wrprodm] [NVARCHAR](8) 
	,[wrprodc] [NVARCHAR](10) 
	,[wrcmod] [NVARCHAR](25) 
	,[wrsyem] [NVARCHAR](36) 
	,[wrvinnu] [NVARCHAR](30) 
	,[wrrefn] [NVARCHAR](30) 
	,[wrze01] [NVARCHAR](10) 
	,[wrze02] [NVARCHAR](10) 
	,[wrze03] [NVARCHAR](10) 
	,[wrze04] [NVARCHAR](10) 
	,[wrze05] [NVARCHAR](10) 
	,[wrze06] [NVARCHAR](10) 
	,[wrze07] [NVARCHAR](10) 
	,[wrze08] [NVARCHAR](10) 
	,[wrze09] [NVARCHAR](10) 
	,[wrze10] [NVARCHAR](10) 
	,[wrurcd] [NVARCHAR](2) 
	,[wrurdt] [VARCHAR](20) 
	,[wrurat] [DECIMAL](38, 4) 
	,[wrurab] [DECIMAL](38, 4) 
	,[wrurrf] [NVARCHAR](15) 
	,[wrcrtu] [NVARCHAR](10) 
	,[wruser] [NVARCHAR](10) 
	,[wrpid] [NVARCHAR](10) 
	,[wrjobn] [NVARCHAR](10) 
	,[wrupmj] [VARCHAR](20) 
	,[wrupmt] [DECIMAL](38, 4) 
	,[wrlotn] [NVARCHAR](30) 
	,[wrlocn] [NVARCHAR](20) 
	,[wrwod] [DECIMAL](38, 4) 
	,[wrbrev] [NVARCHAR](3) 
	,[wrefff] [VARCHAR](20) 
	,[wran8dl] [DECIMAL](38, 4) 
	,[wran8as] [DECIMAL](38, 4) 
	,[wrtermyn] [NVARCHAR](1) 
	,[wrsatyp] [NVARCHAR](3) 
	,[wrinsdte] [VARCHAR](20) 
	,[wrmcu] [NVARCHAR](12) 
	,[wrmrryn] [NVARCHAR](1) 
	,[wrregsts] [NVARCHAR](3) 
	,[wrvmrs31] [NVARCHAR](2) 
	,[wrvmrs32] [NVARCHAR](8) 
	,[wrvmrs34] [NVARCHAR](10) 
	,[wran8dr] [DECIMAL](38, 4) 
	,[wreqpn] [DECIMAL](38, 4) 
	,[wrmtryn] [NVARCHAR](1) 
)
WITH
(
    LOCATION='processing-clean/jde/f1217/cdc/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
