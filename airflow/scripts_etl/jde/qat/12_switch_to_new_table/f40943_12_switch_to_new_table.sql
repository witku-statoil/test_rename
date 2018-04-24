-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f40943') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f40943_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f40943_previous
    
    RENAME OBJECT rep_jde_qat.f40943 TO f40943_previous
END
GO

RENAME OBJECT rep_jde_qat.f40943_new TO f40943

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f40943_sys_integration_id' and object_id = object_id('rep_jde_qat.f40943')
if @v=1
	update statistics rep_jde_qat.f40943 stat_f40943_sys_integration_id
else
	create statistics stat_f40943_sys_integration_id on rep_jde_qat.f40943(sys_integration_id)
