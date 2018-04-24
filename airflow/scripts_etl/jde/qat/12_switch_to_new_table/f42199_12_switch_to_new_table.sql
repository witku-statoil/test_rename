-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f42199') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f42199_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f42199_previous
    
    RENAME OBJECT rep_jde_qat.f42199 TO f42199_previous
END
GO

RENAME OBJECT rep_jde_qat.f42199_new TO f42199

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f42199_sys_integration_id' and object_id = object_id('rep_jde_qat.f42199')
if @v=1
	update statistics rep_jde_qat.f42199 stat_f42199_sys_integration_id
else
	create statistics stat_f42199_sys_integration_id on rep_jde_qat.f42199(sys_integration_id)
