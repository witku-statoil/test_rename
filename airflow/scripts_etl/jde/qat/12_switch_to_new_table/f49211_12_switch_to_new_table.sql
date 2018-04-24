-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f49211') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f49211_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f49211_previous
    
    RENAME OBJECT rep_jde_qat.f49211 TO f49211_previous
END
GO

RENAME OBJECT rep_jde_qat.f49211_new TO f49211

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f49211_sys_integration_id' and object_id = object_id('rep_jde_qat.f49211')
if @v=1
	update statistics rep_jde_qat.f49211 stat_f49211_sys_integration_id
else
	create statistics stat_f49211_sys_integration_id on rep_jde_qat.f49211(sys_integration_id)
