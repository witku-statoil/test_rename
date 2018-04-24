-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f1620') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f1620_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f1620_previous
    
    RENAME OBJECT rep_jde_qat.f1620 TO f1620_previous
END
GO

RENAME OBJECT rep_jde_qat.f1620_new TO f1620

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f1620_sys_integration_id' and object_id = object_id('rep_jde_qat.f1620')
if @v=1
	update statistics rep_jde_qat.f1620 stat_f1620_sys_integration_id
else
	create statistics stat_f1620_sys_integration_id on rep_jde_qat.f1620(sys_integration_id)
