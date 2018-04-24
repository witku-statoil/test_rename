-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f41112') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f41112_previous') IS NOT NULL
        DROP TABLE rep_jde.f41112_previous
    
    RENAME OBJECT rep_jde.f41112 TO f41112_previous
END
GO

RENAME OBJECT rep_jde.f41112_new TO f41112

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f41112_sys_integration_id' and object_id = object_id('rep_jde.f41112')
if @v=1
	update statistics rep_jde.f41112 stat_f41112_sys_integration_id
else
	create statistics stat_f41112_sys_integration_id on rep_jde.f41112(sys_integration_id)
