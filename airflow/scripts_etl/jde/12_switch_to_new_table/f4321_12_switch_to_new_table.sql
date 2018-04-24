-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4321') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4321_previous') IS NOT NULL
        DROP TABLE rep_jde.f4321_previous
    
    RENAME OBJECT rep_jde.f4321 TO f4321_previous
END
GO

RENAME OBJECT rep_jde.f4321_new TO f4321

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4321_sys_integration_id' and object_id = object_id('rep_jde.f4321')
if @v=1
	update statistics rep_jde.f4321 stat_f4321_sys_integration_id
else
	create statistics stat_f4321_sys_integration_id on rep_jde.f4321(sys_integration_id)
