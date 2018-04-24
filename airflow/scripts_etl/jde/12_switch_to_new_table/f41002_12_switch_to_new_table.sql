-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f41002') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f41002_previous') IS NOT NULL
        DROP TABLE rep_jde.f41002_previous
    
    RENAME OBJECT rep_jde.f41002 TO f41002_previous
END
GO

RENAME OBJECT rep_jde.f41002_new TO f41002

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f41002_sys_integration_id' and object_id = object_id('rep_jde.f41002')
if @v=1
	update statistics rep_jde.f41002 stat_f41002_sys_integration_id
else
	create statistics stat_f41002_sys_integration_id on rep_jde.f41002(sys_integration_id)
