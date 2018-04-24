-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4074') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4074_previous') IS NOT NULL
        DROP TABLE rep_jde.f4074_previous
    
    RENAME OBJECT rep_jde.f4074 TO f4074_previous
END
GO

RENAME OBJECT rep_jde.f4074_new TO f4074

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4074_sys_integration_id' and object_id = object_id('rep_jde.f4074')
if @v=1
	update statistics rep_jde.f4074 stat_f4074_sys_integration_id
else
	create statistics stat_f4074_sys_integration_id on rep_jde.f4074(sys_integration_id)
