-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4942') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4942_previous') IS NOT NULL
        DROP TABLE rep_jde.f4942_previous
    
    RENAME OBJECT rep_jde.f4942 TO f4942_previous
END
GO

RENAME OBJECT rep_jde.f4942_new TO f4942

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4942_sys_integration_id' and object_id = object_id('rep_jde.f4942')
if @v=1
	update statistics rep_jde.f4942 stat_f4942_sys_integration_id
else
	create statistics stat_f4942_sys_integration_id on rep_jde.f4942(sys_integration_id)
