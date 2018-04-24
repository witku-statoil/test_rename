-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4209') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4209_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4209_previous
    
    RENAME OBJECT rep_jde_qat.f4209 TO f4209_previous
END
GO

RENAME OBJECT rep_jde_qat.f4209_new TO f4209

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4209_sys_integration_id' and object_id = object_id('rep_jde_qat.f4209')
if @v=1
	update statistics rep_jde_qat.f4209 stat_f4209_sys_integration_id
else
	create statistics stat_f4209_sys_integration_id on rep_jde_qat.f4209(sys_integration_id)
