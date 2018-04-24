-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f42019') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f42019_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f42019_previous
    
    RENAME OBJECT rep_jde_qat.f42019 TO f42019_previous
END
GO

RENAME OBJECT rep_jde_qat.f42019_new TO f42019

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f42019_sys_integration_id' and object_id = object_id('rep_jde_qat.f42019')
if @v=1
	update statistics rep_jde_qat.f42019 stat_f42019_sys_integration_id
else
	create statistics stat_f42019_sys_integration_id on rep_jde_qat.f42019(sys_integration_id)
