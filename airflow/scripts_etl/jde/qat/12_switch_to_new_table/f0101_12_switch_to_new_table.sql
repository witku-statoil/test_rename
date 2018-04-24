-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f0101') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f0101_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f0101_previous
    
    RENAME OBJECT rep_jde_qat.f0101 TO f0101_previous
END
GO

RENAME OBJECT rep_jde_qat.f0101_new TO f0101

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f0101_sys_integration_id' and object_id = object_id('rep_jde_qat.f0101')
if @v=1
	update statistics rep_jde_qat.f0101 stat_f0101_sys_integration_id
else
	create statistics stat_f0101_sys_integration_id on rep_jde_qat.f0101(sys_integration_id)
