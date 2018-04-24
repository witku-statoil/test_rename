-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4111') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4111_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4111_previous
    
    RENAME OBJECT rep_jde_qat.f4111 TO f4111_previous
END
GO

RENAME OBJECT rep_jde_qat.f4111_new TO f4111

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4111_sys_integration_id' and object_id = object_id('rep_jde_qat.f4111')
if @v=1
	update statistics rep_jde_qat.f4111 stat_f4111_sys_integration_id
else
	create statistics stat_f4111_sys_integration_id on rep_jde_qat.f4111(sys_integration_id)
