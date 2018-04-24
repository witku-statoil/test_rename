-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f554904') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f554904_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f554904_previous
    
    RENAME OBJECT rep_jde_qat.f554904 TO f554904_previous
END
GO

RENAME OBJECT rep_jde_qat.f554904_new TO f554904

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554904_sys_integration_id' and object_id = object_id('rep_jde_qat.f554904')
if @v=1
	update statistics rep_jde_qat.f554904 stat_f554904_sys_integration_id
else
	create statistics stat_f554904_sys_integration_id on rep_jde_qat.f554904(sys_integration_id)
