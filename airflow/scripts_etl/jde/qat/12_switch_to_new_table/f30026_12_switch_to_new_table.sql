-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f30026') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f30026_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f30026_previous
    
    RENAME OBJECT rep_jde_qat.f30026 TO f30026_previous
END
GO

RENAME OBJECT rep_jde_qat.f30026_new TO f30026

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f30026_sys_integration_id' and object_id = object_id('rep_jde_qat.f30026')
if @v=1
	update statistics rep_jde_qat.f30026 stat_f30026_sys_integration_id
else
	create statistics stat_f30026_sys_integration_id on rep_jde_qat.f30026(sys_integration_id)
