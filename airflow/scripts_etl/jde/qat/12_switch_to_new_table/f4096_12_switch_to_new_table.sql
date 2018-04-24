-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4096') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4096_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4096_previous
    
    RENAME OBJECT rep_jde_qat.f4096 TO f4096_previous
END
GO

RENAME OBJECT rep_jde_qat.f4096_new TO f4096

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4096_sys_integration_id' and object_id = object_id('rep_jde_qat.f4096')
if @v=1
	update statistics rep_jde_qat.f4096 stat_f4096_sys_integration_id
else
	create statistics stat_f4096_sys_integration_id on rep_jde_qat.f4096(sys_integration_id)
