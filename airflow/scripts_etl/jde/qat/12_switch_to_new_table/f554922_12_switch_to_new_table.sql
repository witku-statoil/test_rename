-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f554922') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f554922_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f554922_previous
    
    RENAME OBJECT rep_jde_qat.f554922 TO f554922_previous
END
GO

RENAME OBJECT rep_jde_qat.f554922_new TO f554922

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554922_sys_integration_id' and object_id = object_id('rep_jde_qat.f554922')
if @v=1
	update statistics rep_jde_qat.f554922 stat_f554922_sys_integration_id
else
	create statistics stat_f554922_sys_integration_id on rep_jde_qat.f554922(sys_integration_id)
