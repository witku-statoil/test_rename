-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f554506') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f554506_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f554506_previous
    
    RENAME OBJECT rep_jde_qat.f554506 TO f554506_previous
END
GO

RENAME OBJECT rep_jde_qat.f554506_new TO f554506

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554506_sys_integration_id' and object_id = object_id('rep_jde_qat.f554506')
if @v=1
	update statistics rep_jde_qat.f554506 stat_f554506_sys_integration_id
else
	create statistics stat_f554506_sys_integration_id on rep_jde_qat.f554506(sys_integration_id)
