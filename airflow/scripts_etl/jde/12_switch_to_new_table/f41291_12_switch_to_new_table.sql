-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f41291') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f41291_previous') IS NOT NULL
        DROP TABLE rep_jde.f41291_previous
    
    RENAME OBJECT rep_jde.f41291 TO f41291_previous
END
GO

RENAME OBJECT rep_jde.f41291_new TO f41291

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f41291_sys_integration_id' and object_id = object_id('rep_jde.f41291')
if @v=1
	update statistics rep_jde.f41291 stat_f41291_sys_integration_id
else
	create statistics stat_f41291_sys_integration_id on rep_jde.f41291(sys_integration_id)
