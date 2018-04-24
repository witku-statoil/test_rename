-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f0014') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f0014_previous') IS NOT NULL
        DROP TABLE rep_jde.f0014_previous
    
    RENAME OBJECT rep_jde.f0014 TO f0014_previous
END
GO

RENAME OBJECT rep_jde.f0014_new TO f0014

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f0014_sys_integration_id' and object_id = object_id('rep_jde.f0014')
if @v=1
	update statistics rep_jde.f0014 stat_f0014_sys_integration_id
else
	create statistics stat_f0014_sys_integration_id on rep_jde.f0014(sys_integration_id)
