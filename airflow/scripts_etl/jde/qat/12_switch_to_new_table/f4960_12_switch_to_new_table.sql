-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4960') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4960_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4960_previous
    
    RENAME OBJECT rep_jde_qat.f4960 TO f4960_previous
END
GO

RENAME OBJECT rep_jde_qat.f4960_new TO f4960

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4960_sys_integration_id' and object_id = object_id('rep_jde_qat.f4960')
if @v=1
	update statistics rep_jde_qat.f4960 stat_f4960_sys_integration_id
else
	create statistics stat_f4960_sys_integration_id on rep_jde_qat.f4960(sys_integration_id)
