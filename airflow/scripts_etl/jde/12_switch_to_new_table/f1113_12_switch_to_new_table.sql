-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f1113') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f1113_previous') IS NOT NULL
        DROP TABLE rep_jde.f1113_previous
    
    RENAME OBJECT rep_jde.f1113 TO f1113_previous
END
GO

RENAME OBJECT rep_jde.f1113_new TO f1113

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f1113_sys_integration_id' and object_id = object_id('rep_jde.f1113')
if @v=1
	update statistics rep_jde.f1113 stat_f1113_sys_integration_id
else
	create statistics stat_f1113_sys_integration_id on rep_jde.f1113(sys_integration_id)
