-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f3412') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f3412_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f3412_previous
    
    RENAME OBJECT rep_jde_qat.f3412 TO f3412_previous
END
GO

RENAME OBJECT rep_jde_qat.f3412_new TO f3412

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f3412_sys_integration_id' and object_id = object_id('rep_jde_qat.f3412')
if @v=1
	update statistics rep_jde_qat.f3412 stat_f3412_sys_integration_id
else
	create statistics stat_f3412_sys_integration_id on rep_jde_qat.f3412(sys_integration_id)
