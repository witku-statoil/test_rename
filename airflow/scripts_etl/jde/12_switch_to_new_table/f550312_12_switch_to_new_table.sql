-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f550312') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f550312_previous') IS NOT NULL
        DROP TABLE rep_jde.f550312_previous
    
    RENAME OBJECT rep_jde.f550312 TO f550312_previous
END
GO

RENAME OBJECT rep_jde.f550312_new TO f550312

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f550312_sys_integration_id' and object_id = object_id('rep_jde.f550312')
if @v=1
	update statistics rep_jde.f550312 stat_f550312_sys_integration_id
else
	create statistics stat_f550312_sys_integration_id on rep_jde.f550312(sys_integration_id)
