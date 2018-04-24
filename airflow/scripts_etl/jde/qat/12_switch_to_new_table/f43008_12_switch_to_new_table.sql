-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f43008') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f43008_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f43008_previous
    
    RENAME OBJECT rep_jde_qat.f43008 TO f43008_previous
END
GO

RENAME OBJECT rep_jde_qat.f43008_new TO f43008

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f43008_sys_integration_id' and object_id = object_id('rep_jde_qat.f43008')
if @v=1
	update statistics rep_jde_qat.f43008 stat_f43008_sys_integration_id
else
	create statistics stat_f43008_sys_integration_id on rep_jde_qat.f43008(sys_integration_id)
