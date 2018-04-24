-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f556202') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f556202_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f556202_previous
    
    RENAME OBJECT rep_jde_qat.f556202 TO f556202_previous
END
GO

RENAME OBJECT rep_jde_qat.f556202_new TO f556202

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f556202_sys_integration_id' and object_id = object_id('rep_jde_qat.f556202')
if @v=1
	update statistics rep_jde_qat.f556202 stat_f556202_sys_integration_id
else
	create statistics stat_f556202_sys_integration_id on rep_jde_qat.f556202(sys_integration_id)
