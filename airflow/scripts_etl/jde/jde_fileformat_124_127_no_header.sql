
CREATE EXTERNAL FILE FORMAT [jde_fileformat_124_127_no_header] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N'|', DATE_FORMAT = N'yyyy-MM-dd HH:mm:ss.fff', USE_TYPE_DEFAULT = False), DATA_COMPRESSION = N'org.apache.hadoop.io.compress.GzipCodec')
GO
