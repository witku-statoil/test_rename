-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f5509176') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f5509176_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f5509176_previous
    
    RENAME OBJECT rep_jde_qat.f5509176 TO f5509176_previous
END
GO

RENAME OBJECT rep_jde_qat.f5509176_new TO f5509176

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f5509176_sys_integration_id' and object_id = object_id('rep_jde_qat.f5509176')
if @v=1
	update statistics rep_jde_qat.f5509176 stat_f5509176_sys_integration_id
else
	create statistics stat_f5509176_sys_integration_id on rep_jde_qat.f5509176(sys_integration_id)
