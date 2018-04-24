-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f554503') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f554503_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f554503_previous
    
    RENAME OBJECT rep_jde_qat.f554503 TO f554503_previous
END
GO

RENAME OBJECT rep_jde_qat.f554503_new TO f554503

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f554503_sys_integration_id' and object_id = object_id('rep_jde_qat.f554503')
if @v=1
	update statistics rep_jde_qat.f554503 stat_f554503_sys_integration_id
else
	create statistics stat_f554503_sys_integration_id on rep_jde_qat.f554503(sys_integration_id)
