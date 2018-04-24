-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde.f4930') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde.f4930_previous') IS NOT NULL
        DROP TABLE rep_jde.f4930_previous
    
    RENAME OBJECT rep_jde.f4930 TO f4930_previous
END
GO

RENAME OBJECT rep_jde.f4930_new TO f4930

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4930_sys_integration_id' and object_id = object_id('rep_jde.f4930')
if @v=1
	update statistics rep_jde.f4930 stat_f4930_sys_integration_id
else
	create statistics stat_f4930_sys_integration_id on rep_jde.f4930(sys_integration_id)
