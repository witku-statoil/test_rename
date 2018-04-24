-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f38112') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f38112_previous') IS NOT NULL
        DROP TABLE rep_jde.f38112_previous
    
    RENAME OBJECT rep_jde.f38112 TO f38112_previous
END
GO

RENAME OBJECT rep_jde.f38112_new TO f38112

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f38112_sys_integration_id' and object_id = object_id('rep_jde.f38112')
if @v=1
	update statistics rep_jde.f38112 stat_f38112_sys_integration_id
else
	create statistics stat_f38112_sys_integration_id on rep_jde.f38112(sys_integration_id)
