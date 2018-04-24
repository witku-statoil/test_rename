-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4981') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4981_previous') IS NOT NULL
        DROP TABLE rep_jde.f4981_previous
    
    RENAME OBJECT rep_jde.f4981 TO f4981_previous
END
GO

RENAME OBJECT rep_jde.f4981_new TO f4981

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4981_sys_integration_id' and object_id = object_id('rep_jde.f4981')
if @v=1
	update statistics rep_jde.f4981 stat_f4981_sys_integration_id
else
	create statistics stat_f4981_sys_integration_id on rep_jde.f4981(sys_integration_id)
