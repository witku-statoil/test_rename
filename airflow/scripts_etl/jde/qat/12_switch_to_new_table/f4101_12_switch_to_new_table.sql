-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4101') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4101_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4101_previous
    
    RENAME OBJECT rep_jde_qat.f4101 TO f4101_previous
END
GO

RENAME OBJECT rep_jde_qat.f4101_new TO f4101

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4101_sys_integration_id' and object_id = object_id('rep_jde_qat.f4101')
if @v=1
	update statistics rep_jde_qat.f4101 stat_f4101_sys_integration_id
else
	create statistics stat_f4101_sys_integration_id on rep_jde_qat.f4101(sys_integration_id)
