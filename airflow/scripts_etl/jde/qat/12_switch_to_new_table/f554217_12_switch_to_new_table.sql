-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f554217') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f554217_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f554217_previous
    
    RENAME OBJECT rep_jde_qat.f554217 TO f554217_previous
END
GO

RENAME OBJECT rep_jde_qat.f554217_new TO f554217

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554217_sys_integration_id' and object_id = object_id('rep_jde_qat.f554217')
if @v=1
	update statistics rep_jde_qat.f554217 stat_f554217_sys_integration_id
else
	create statistics stat_f554217_sys_integration_id on rep_jde_qat.f554217(sys_integration_id)
