-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f40540') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f40540_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f40540_previous
    
    RENAME OBJECT rep_jde_qat.f40540 TO f40540_previous
END
GO

RENAME OBJECT rep_jde_qat.f40540_new TO f40540

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f40540_sys_integration_id' and object_id = object_id('rep_jde_qat.f40540')
if @v=1
	update statistics rep_jde_qat.f40540 stat_f40540_sys_integration_id
else
	create statistics stat_f40540_sys_integration_id on rep_jde_qat.f40540(sys_integration_id)
