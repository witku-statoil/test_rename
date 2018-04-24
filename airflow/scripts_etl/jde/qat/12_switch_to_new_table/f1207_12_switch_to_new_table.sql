-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f1207') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f1207_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f1207_previous
    
    RENAME OBJECT rep_jde_qat.f1207 TO f1207_previous
END
GO

RENAME OBJECT rep_jde_qat.f1207_new TO f1207

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f1207_sys_integration_id' and object_id = object_id('rep_jde_qat.f1207')
if @v=1
	update statistics rep_jde_qat.f1207 stat_f1207_sys_integration_id
else
	create statistics stat_f1207_sys_integration_id on rep_jde_qat.f1207(sys_integration_id)