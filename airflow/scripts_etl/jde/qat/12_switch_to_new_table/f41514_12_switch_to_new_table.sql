-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f41514') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f41514_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f41514_previous
    
    RENAME OBJECT rep_jde_qat.f41514 TO f41514_previous
END
GO

RENAME OBJECT rep_jde_qat.f41514_new TO f41514

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f41514_sys_integration_id' and object_id = object_id('rep_jde_qat.f41514')
if @v=1
	update statistics rep_jde_qat.f41514 stat_f41514_sys_integration_id
else
	create statistics stat_f41514_sys_integration_id on rep_jde_qat.f41514(sys_integration_id)
