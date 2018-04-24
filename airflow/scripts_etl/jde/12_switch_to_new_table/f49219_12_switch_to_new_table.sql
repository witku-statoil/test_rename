-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f49219') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f49219_previous') IS NOT NULL
        DROP TABLE rep_jde.f49219_previous
    
    RENAME OBJECT rep_jde.f49219 TO f49219_previous
END
GO

RENAME OBJECT rep_jde.f49219_new TO f49219

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f49219_sys_integration_id' and object_id = object_id('rep_jde.f49219')
if @v=1
	update statistics rep_jde.f49219 stat_f49219_sys_integration_id
else
	create statistics stat_f49219_sys_integration_id on rep_jde.f49219(sys_integration_id)
