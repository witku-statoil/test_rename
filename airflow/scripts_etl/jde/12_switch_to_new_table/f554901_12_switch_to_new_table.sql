-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f554901') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f554901_previous') IS NOT NULL
        DROP TABLE rep_jde.f554901_previous
    
    RENAME OBJECT rep_jde.f554901 TO f554901_previous
END
GO

RENAME OBJECT rep_jde.f554901_new TO f554901

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554901_sys_integration_id' and object_id = object_id('rep_jde.f554901')
if @v=1
	update statistics rep_jde.f554901 stat_f554901_sys_integration_id
else
	create statistics stat_f554901_sys_integration_id on rep_jde.f554901(sys_integration_id)
