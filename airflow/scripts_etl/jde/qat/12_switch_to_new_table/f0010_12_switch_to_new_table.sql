-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f0010') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f0010_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f0010_previous
    
    RENAME OBJECT rep_jde_qat.f0010 TO f0010_previous
END
GO

RENAME OBJECT rep_jde_qat.f0010_new TO f0010

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f0010_sys_integration_id' and object_id = object_id('rep_jde_qat.f0010')
if @v=1
	update statistics rep_jde_qat.f0010 stat_f0010_sys_integration_id
else
	create statistics stat_f0010_sys_integration_id on rep_jde_qat.f0010(sys_integration_id)
