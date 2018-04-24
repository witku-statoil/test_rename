-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f554231') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f554231_previous') IS NOT NULL
        DROP TABLE rep_jde.f554231_previous
    
    RENAME OBJECT rep_jde.f554231 TO f554231_previous
END
GO

RENAME OBJECT rep_jde.f554231_new TO f554231

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554231_sys_integration_id' and object_id = object_id('rep_jde.f554231')
if @v=1
	update statistics rep_jde.f554231 stat_f554231_sys_integration_id
else
	create statistics stat_f554231_sys_integration_id on rep_jde.f554231(sys_integration_id)