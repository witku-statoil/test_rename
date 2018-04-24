-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4072') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4072_previous') IS NOT NULL
        DROP TABLE rep_jde.f4072_previous
    
    RENAME OBJECT rep_jde.f4072 TO f4072_previous
END
GO

RENAME OBJECT rep_jde.f4072_new TO f4072

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4072_sys_integration_id' and object_id = object_id('rep_jde.f4072')
if @v=1
	update statistics rep_jde.f4072 stat_f4072_sys_integration_id
else
	create statistics stat_f4072_sys_integration_id on rep_jde.f4072(sys_integration_id)
