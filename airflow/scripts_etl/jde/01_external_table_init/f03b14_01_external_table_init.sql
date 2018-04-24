--Create ext table for init
IF OBJECT_ID('stg_jde.ext_f03b14_init') IS NOT NULL
    DROP EXTERNAL TABLE stg_jde.ext_f03b14_init

CREATE EXTERNAL TABLE stg_jde.ext_f03b14_init
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
	,[rzpyid] [DECIMAL](38, 4) 
	,[rzrc5] [DECIMAL](38, 4) 
	,[rzcknu] [NVARCHAR](25) 
	,[rzdoc] [DECIMAL](38, 4) 
	,[rzdct] [NVARCHAR](2) 
	,[rzkco] [NVARCHAR](5) 
	,[rzsfx] [NVARCHAR](3) 
	,[rzan8] [DECIMAL](38, 4) 
	,[rzdctm] [NVARCHAR](2) 
	,[rzdmtj] [VARCHAR](20) 
	,[rzdgj] [VARCHAR](20) 
	,[rzpost] [NVARCHAR](1) 
	,[rzglc] [NVARCHAR](4) 
	,[rzaid] [NVARCHAR](8) 
	,[rzctry] [DECIMAL](38, 4) 
	,[rzfy] [DECIMAL](38, 4) 
	,[rzpn] [DECIMAL](38, 4) 
	,[rzco] [NVARCHAR](5) 
	,[rzicut] [NVARCHAR](2) 
	,[rzicu] [DECIMAL](38, 4) 
	,[rzdicj] [VARCHAR](20) 
	,[rzpa8] [DECIMAL](38, 4) 
	,[rzrpid] [DECIMAL](38, 4) 
	,[rzrlin] [DECIMAL](38, 4) 
	,[rzpaap] [DECIMAL](38, 4) 
	,[rzadsc] [DECIMAL](38, 4) 
	,[rzadsa] [DECIMAL](38, 4) 
	,[rzaaaj] [DECIMAL](38, 4) 
	,[rzecba] [DECIMAL](38, 4) 
	,[rzdda] [DECIMAL](38, 4) 
	,[rzbcrc] [NVARCHAR](3) 
	,[rzcrrm] [NVARCHAR](1) 
	,[rzcrcd] [NVARCHAR](3) 
	,[rzcrr] [DECIMAL](38, 4) 
	,[rzpfap] [DECIMAL](38, 4) 
	,[rzcds] [DECIMAL](38, 4) 
	,[rzcdsa] [DECIMAL](38, 4) 
	,[rzfchg] [DECIMAL](38, 4) 
	,[rzecbf] [DECIMAL](38, 4) 
	,[rzfda] [DECIMAL](38, 4) 
	,[rzagl] [DECIMAL](38, 4) 
	,[rzaidt] [NVARCHAR](8) 
	,[rztcrc] [NVARCHAR](3) 
	,[rztaap] [DECIMAL](38, 4) 
	,[rztada] [DECIMAL](38, 4) 
	,[rztaaj] [DECIMAL](38, 4) 
	,[rztcba] [DECIMAL](38, 4) 
	,[rztda] [DECIMAL](38, 4) 
	,[rzacrr] [DECIMAL](38, 4) 
	,[rzacr1] [DECIMAL](38, 4) 
	,[rzacr2] [DECIMAL](38, 4) 
	,[rzacrm] [NVARCHAR](1) 
	,[rzagla] [DECIMAL](38, 4) 
	,[rzaida] [NVARCHAR](8) 
	,[rzaidd] [NVARCHAR](8) 
	,[rzrsco] [NVARCHAR](2) 
	,[rzaidw] [NVARCHAR](8) 
	,[rzecbr] [NVARCHAR](2) 
	,[rzglcc] [NVARCHAR](4) 
	,[rzaidc] [NVARCHAR](8) 
	,[rzddex] [NVARCHAR](2) 
	,[rzdaid] [NVARCHAR](8) 
	,[rztyin] [NVARCHAR](1) 
	,[rzutic] [NVARCHAR](2) 
	,[rzuctf] [NVARCHAR](1) 
	,[rzaid2] [NVARCHAR](8) 
	,[rzam2] [NVARCHAR](1) 
	,[rzmcu] [NVARCHAR](12) 
	,[rzsbl] [NVARCHAR](8) 
	,[rzsblt] [NVARCHAR](1) 
	,[rzpkco] [NVARCHAR](5) 
	,[rzpo] [NVARCHAR](8) 
	,[rzpdct] [NVARCHAR](2) 
	,[rznumb] [DECIMAL](38, 4) 
	,[rzunit] [NVARCHAR](8) 
	,[rzmcu2] [NVARCHAR](12) 
	,[rzrmk] [NVARCHAR](30) 
	,[rzfnlp] [NVARCHAR](1) 
	,[rzalt6] [NVARCHAR](1) 
	,[rzodoc] [DECIMAL](38, 4) 
	,[rzodct] [NVARCHAR](2) 
	,[rzokco] [NVARCHAR](5) 
	,[rzosfx] [NVARCHAR](3) 
	,[rzgdoc] [DECIMAL](38, 4) 
	,[rzgdct] [NVARCHAR](2) 
	,[rzgkco] [NVARCHAR](5) 
	,[rzgsfx] [NVARCHAR](3) 
	,[rzdctg] [NVARCHAR](2) 
	,[rzdocg] [DECIMAL](38, 4) 
	,[rzkcog] [NVARCHAR](5) 
	,[rzctl] [NVARCHAR](13) 
	,[rzsmtj] [VARCHAR](20) 
	,[rzvdgj] [VARCHAR](20) 
	,[rzvre] [NVARCHAR](3) 
	,[rznfvd] [NVARCHAR](1) 
	,[rzhcrr] [DECIMAL](38, 4) 
	,[rzistc] [NVARCHAR](1) 
	,[rzlfcj] [VARCHAR](20) 
	,[rzpdlt] [NVARCHAR](1) 
	,[rzddj] [VARCHAR](20) 
	,[rzddnj] [VARCHAR](20) 
	,[rzidgj] [VARCHAR](20) 
	,[rzddst] [NVARCHAR](1) 
	,[rzvr01] [NVARCHAR](25) 
	,[rzrfid] [DECIMAL](38, 4) 
	,[rzridc] [NVARCHAR](1) 
	,[rzprgf] [NVARCHAR](1) 
	,[rzgfl1] [NVARCHAR](1) 
	,[rzrnid] [DECIMAL](38, 4) 
	,[rztorg] [NVARCHAR](10) 
	,[rzuser] [NVARCHAR](10) 
	,[rzpid] [NVARCHAR](10) 
	,[rzupmj] [VARCHAR](20) 
	,[rzupmt] [DECIMAL](38, 4) 
	,[rzjobn] [NVARCHAR](10) 
	,[rzurab] [DECIMAL](38, 4) 
	,[rzurat] [DECIMAL](38, 4) 
	,[rzurc1] [NVARCHAR](3) 
	,[rzurdt] [VARCHAR](20) 
	,[rzurrf] [NVARCHAR](15) 
	,[rzshpn] [DECIMAL](38, 4) 
	,[rzomod] [NVARCHAR](1) 
	,[rzdoco] [DECIMAL](38, 4) 
	,[rzrasi] [NVARCHAR](8) 
	,[rzsfxo] [NVARCHAR](3) 
	,[rzkcoo] [NVARCHAR](5) 
	,[rzdcto] [NVARCHAR](2) 
	,[rzramt] [DECIMAL](38, 4) 
	,[rzlrfl] [NVARCHAR](1) 
	,[rzgfl2] [NVARCHAR](1) 
	,[rzdrco] [NVARCHAR](3) 
	,[rznettcid] [DECIMAL](38, 4) 
	,[rznetdoc] [DECIMAL](38, 4) 
	,[rznetrc5] [DECIMAL](38, 4) 
	,[rzadgj] [VARCHAR](20) 
)
WITH
(
    LOCATION='processing-clean/jde/f03b14/init/'
    ,DATA_SOURCE = [dw-landing-fs]
    ,FILE_FORMAT = [jde_fileformat_124_127_no_header]
)
