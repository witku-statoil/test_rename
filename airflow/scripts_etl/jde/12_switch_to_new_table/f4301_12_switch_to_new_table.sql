-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4301') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4301_previous') IS NOT NULL
        DROP TABLE rep_jde.f4301_previous
    
    RENAME OBJECT rep_jde.f4301 TO f4301_previous
END
GO

RENAME OBJECT rep_jde.f4301_new TO f4301

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4301_sys_integration_id' and object_id = object_id('rep_jde.f4301')
if @v=1
	update statistics rep_jde.f4301 stat_f4301_sys_integration_id
else
	create statistics stat_f4301_sys_integration_id on rep_jde.f4301(sys_integration_id)
