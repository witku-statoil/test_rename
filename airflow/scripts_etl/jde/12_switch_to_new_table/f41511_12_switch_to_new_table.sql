-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f41511') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f41511_previous') IS NOT NULL
        DROP TABLE rep_jde.f41511_previous
    
    RENAME OBJECT rep_jde.f41511 TO f41511_previous
END
GO

RENAME OBJECT rep_jde.f41511_new TO f41511

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f41511_sys_integration_id' and object_id = object_id('rep_jde.f41511')
if @v=1
	update statistics rep_jde.f41511 stat_f41511_sys_integration_id
else
	create statistics stat_f41511_sys_integration_id on rep_jde.f41511(sys_integration_id)
