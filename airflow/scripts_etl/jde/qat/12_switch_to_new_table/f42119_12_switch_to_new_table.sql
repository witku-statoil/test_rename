-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f42119') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f42119_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f42119_previous
    
    RENAME OBJECT rep_jde_qat.f42119 TO f42119_previous
END
GO

RENAME OBJECT rep_jde_qat.f42119_new TO f42119

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f42119_sys_integration_id' and object_id = object_id('rep_jde_qat.f42119')
if @v=1
	update statistics rep_jde_qat.f42119 stat_f42119_sys_integration_id
else
	create statistics stat_f42119_sys_integration_id on rep_jde_qat.f42119(sys_integration_id)
