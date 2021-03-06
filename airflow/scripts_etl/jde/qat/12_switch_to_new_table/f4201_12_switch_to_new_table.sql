-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4201') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4201_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4201_previous
    
    RENAME OBJECT rep_jde_qat.f4201 TO f4201_previous
END
GO

RENAME OBJECT rep_jde_qat.f4201_new TO f4201

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4201_sys_integration_id' and object_id = object_id('rep_jde_qat.f4201')
if @v=1
	update statistics rep_jde_qat.f4201 stat_f4201_sys_integration_id
else
	create statistics stat_f4201_sys_integration_id on rep_jde_qat.f4201(sys_integration_id)
