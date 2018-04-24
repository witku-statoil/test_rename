-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f40205') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f40205_previous') IS NOT NULL
        DROP TABLE rep_jde.f40205_previous
    
    RENAME OBJECT rep_jde.f40205 TO f40205_previous
END
GO

RENAME OBJECT rep_jde.f40205_new TO f40205

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f40205_sys_integration_id' and object_id = object_id('rep_jde.f40205')
if @v=1
	update statistics rep_jde.f40205 stat_f40205_sys_integration_id
else
	create statistics stat_f40205_sys_integration_id on rep_jde.f40205(sys_integration_id)
