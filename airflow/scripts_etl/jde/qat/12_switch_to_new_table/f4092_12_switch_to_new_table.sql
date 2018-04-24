-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4092') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4092_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4092_previous
    
    RENAME OBJECT rep_jde_qat.f4092 TO f4092_previous
END
GO

RENAME OBJECT rep_jde_qat.f4092_new TO f4092

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4092_sys_integration_id' and object_id = object_id('rep_jde_qat.f4092')
if @v=1
	update statistics rep_jde_qat.f4092 stat_f4092_sys_integration_id
else
	create statistics stat_f4092_sys_integration_id on rep_jde_qat.f4092(sys_integration_id)
