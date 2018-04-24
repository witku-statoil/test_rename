-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f550006') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f550006_previous') IS NOT NULL
        DROP TABLE rep_jde.f550006_previous
    
    RENAME OBJECT rep_jde.f550006 TO f550006_previous
END
GO

RENAME OBJECT rep_jde.f550006_new TO f550006

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f550006_sys_integration_id' and object_id = object_id('rep_jde.f550006')
if @v=1
	update statistics rep_jde.f550006 stat_f550006_sys_integration_id
else
	create statistics stat_f550006_sys_integration_id on rep_jde.f550006(sys_integration_id)
