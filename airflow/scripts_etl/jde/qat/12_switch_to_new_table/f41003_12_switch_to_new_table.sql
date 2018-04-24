-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f41003') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f41003_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f41003_previous
    
    RENAME OBJECT rep_jde_qat.f41003 TO f41003_previous
END
GO

RENAME OBJECT rep_jde_qat.f41003_new TO f41003

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f41003_sys_integration_id' and object_id = object_id('rep_jde_qat.f41003')
if @v=1
	update statistics rep_jde_qat.f41003 stat_f41003_sys_integration_id
else
	create statistics stat_f41003_sys_integration_id on rep_jde_qat.f41003(sys_integration_id)
