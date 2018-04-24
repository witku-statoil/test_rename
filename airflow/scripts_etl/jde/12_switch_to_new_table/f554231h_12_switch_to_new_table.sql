-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f554231h') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f554231h_previous') IS NOT NULL
        DROP TABLE rep_jde.f554231h_previous
    
    RENAME OBJECT rep_jde.f554231h TO f554231h_previous
END
GO

RENAME OBJECT rep_jde.f554231h_new TO f554231h

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554231h_sys_integration_id' and object_id = object_id('rep_jde.f554231h')
if @v=1
	update statistics rep_jde.f554231h stat_f554231h_sys_integration_id
else
	create statistics stat_f554231h_sys_integration_id on rep_jde.f554231h(sys_integration_id)
