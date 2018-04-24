-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f01151') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f01151_previous') IS NOT NULL
        DROP TABLE rep_jde.f01151_previous
    
    RENAME OBJECT rep_jde.f01151 TO f01151_previous
END
GO

RENAME OBJECT rep_jde.f01151_new TO f01151

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f01151_sys_integration_id' and object_id = object_id('rep_jde.f01151')
if @v=1
	update statistics rep_jde.f01151 stat_f01151_sys_integration_id
else
	create statistics stat_f01151_sys_integration_id on rep_jde.f01151(sys_integration_id)
