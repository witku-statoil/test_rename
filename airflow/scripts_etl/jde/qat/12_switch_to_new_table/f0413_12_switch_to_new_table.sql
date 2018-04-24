-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f0413') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f0413_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f0413_previous
    
    RENAME OBJECT rep_jde_qat.f0413 TO f0413_previous
END
GO

RENAME OBJECT rep_jde_qat.f0413_new TO f0413

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f0413_sys_integration_id' and object_id = object_id('rep_jde_qat.f0413')
if @v=1
	update statistics rep_jde_qat.f0413 stat_f0413_sys_integration_id
else
	create statistics stat_f0413_sys_integration_id on rep_jde_qat.f0413(sys_integration_id)
