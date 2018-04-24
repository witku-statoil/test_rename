-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f03b11') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f03b11_previous') IS NOT NULL
        DROP TABLE rep_jde.f03b11_previous
    
    RENAME OBJECT rep_jde.f03b11 TO f03b11_previous
END
GO

RENAME OBJECT rep_jde.f03b11_new TO f03b11

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f03b11_sys_integration_id' and object_id = object_id('rep_jde.f03b11')
if @v=1
	update statistics rep_jde.f03b11 stat_f03b11_sys_integration_id
else
	create statistics stat_f03b11_sys_integration_id on rep_jde.f03b11(sys_integration_id)
