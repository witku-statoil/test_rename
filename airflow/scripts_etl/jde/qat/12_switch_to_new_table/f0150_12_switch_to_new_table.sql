-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f0150') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f0150_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f0150_previous
    
    RENAME OBJECT rep_jde_qat.f0150 TO f0150_previous
END
GO

RENAME OBJECT rep_jde_qat.f0150_new TO f0150

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f0150_sys_integration_id' and object_id = object_id('rep_jde_qat.f0150')
if @v=1
	update statistics rep_jde_qat.f0150 stat_f0150_sys_integration_id
else
	create statistics stat_f0150_sys_integration_id on rep_jde_qat.f0150(sys_integration_id)
