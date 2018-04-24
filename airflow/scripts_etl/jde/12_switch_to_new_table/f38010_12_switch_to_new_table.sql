-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f38010') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f38010_previous') IS NOT NULL
        DROP TABLE rep_jde.f38010_previous
    
    RENAME OBJECT rep_jde.f38010 TO f38010_previous
END
GO

RENAME OBJECT rep_jde.f38010_new TO f38010

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f38010_sys_integration_id' and object_id = object_id('rep_jde.f38010')
if @v=1
	update statistics rep_jde.f38010 stat_f38010_sys_integration_id
else
	create statistics stat_f38010_sys_integration_id on rep_jde.f38010(sys_integration_id)
