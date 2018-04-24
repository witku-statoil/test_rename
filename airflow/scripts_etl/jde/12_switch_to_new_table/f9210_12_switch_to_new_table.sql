-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f9210') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f9210_previous') IS NOT NULL
        DROP TABLE rep_jde.f9210_previous
    
    RENAME OBJECT rep_jde.f9210 TO f9210_previous
END
GO

RENAME OBJECT rep_jde.f9210_new TO f9210

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f9210_sys_integration_id' and object_id = object_id('rep_jde.f9210')
if @v=1
	update statistics rep_jde.f9210 stat_f9210_sys_integration_id
else
	create statistics stat_f9210_sys_integration_id on rep_jde.f9210(sys_integration_id)
