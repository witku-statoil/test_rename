-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f3460') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f3460_previous') IS NOT NULL
        DROP TABLE rep_jde.f3460_previous
    
    RENAME OBJECT rep_jde.f3460 TO f3460_previous
END
GO

RENAME OBJECT rep_jde.f3460_new TO f3460

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f3460_sys_integration_id' and object_id = object_id('rep_jde.f3460')
if @v=1
	update statistics rep_jde.f3460 stat_f3460_sys_integration_id
else
	create statistics stat_f3460_sys_integration_id on rep_jde.f3460(sys_integration_id)
