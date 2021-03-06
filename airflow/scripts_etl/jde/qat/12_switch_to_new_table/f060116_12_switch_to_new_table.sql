-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f060116') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f060116_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f060116_previous
    
    RENAME OBJECT rep_jde_qat.f060116 TO f060116_previous
END
GO

RENAME OBJECT rep_jde_qat.f060116_new TO f060116

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f060116_sys_integration_id' and object_id = object_id('rep_jde_qat.f060116')
if @v=1
	update statistics rep_jde_qat.f060116 stat_f060116_sys_integration_id
else
	create statistics stat_f060116_sys_integration_id on rep_jde_qat.f060116(sys_integration_id)
