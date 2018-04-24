-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f556103') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f556103_previous') IS NOT NULL
        DROP TABLE rep_jde.f556103_previous
    
    RENAME OBJECT rep_jde.f556103 TO f556103_previous
END
GO

RENAME OBJECT rep_jde.f556103_new TO f556103

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f556103_sys_integration_id' and object_id = object_id('rep_jde.f556103')
if @v=1
	update statistics rep_jde.f556103 stat_f556103_sys_integration_id
else
	create statistics stat_f556103_sys_integration_id on rep_jde.f556103(sys_integration_id)
