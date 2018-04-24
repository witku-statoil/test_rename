-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f0902') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f0902_previous') IS NOT NULL
        DROP TABLE rep_jde.f0902_previous
    
    RENAME OBJECT rep_jde.f0902 TO f0902_previous
END
GO

RENAME OBJECT rep_jde.f0902_new TO f0902

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f0902_sys_integration_id' and object_id = object_id('rep_jde.f0902')
if @v=1
	update statistics rep_jde.f0902 stat_f0902_sys_integration_id
else
	create statistics stat_f0902_sys_integration_id on rep_jde.f0902(sys_integration_id)
