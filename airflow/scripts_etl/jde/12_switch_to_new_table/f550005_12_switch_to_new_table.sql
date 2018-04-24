-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f550005') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f550005_previous') IS NOT NULL
        DROP TABLE rep_jde.f550005_previous
    
    RENAME OBJECT rep_jde.f550005 TO f550005_previous
END
GO

RENAME OBJECT rep_jde.f550005_new TO f550005

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f550005_sys_integration_id' and object_id = object_id('rep_jde.f550005')
if @v=1
	update statistics rep_jde.f550005 stat_f550005_sys_integration_id
else
	create statistics stat_f550005_sys_integration_id on rep_jde.f550005(sys_integration_id)
