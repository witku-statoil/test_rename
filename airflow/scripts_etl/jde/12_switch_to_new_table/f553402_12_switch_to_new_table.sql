-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f553402') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f553402_previous') IS NOT NULL
        DROP TABLE rep_jde.f553402_previous
    
    RENAME OBJECT rep_jde.f553402 TO f553402_previous
END
GO

RENAME OBJECT rep_jde.f553402_new TO f553402

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f553402_sys_integration_id' and object_id = object_id('rep_jde.f553402')
if @v=1
	update statistics rep_jde.f553402 stat_f553402_sys_integration_id
else
	create statistics stat_f553402_sys_integration_id on rep_jde.f553402(sys_integration_id)
