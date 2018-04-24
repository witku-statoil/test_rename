-- Switch "rep table" to "previous" then switch "new rep" to "rep table"
IF OBJECT_ID('rep_jde_qat.f4801t') IS NOT NULL
BEGIN
    IF OBJECT_ID('rep_jde_qat.f4801t_previous') IS NOT NULL
        DROP TABLE rep_jde_qat.f4801t_previous
    
    RENAME OBJECT rep_jde_qat.f4801t TO f4801t_previous
END
GO

RENAME OBJECT rep_jde_qat.f4801t_new TO f4801t

-- Create or update statistics
declare @v int
select @v=1 from sys.stats where name = 'stat_f4801t_sys_integration_id' and object_id = object_id('rep_jde_qat.f4801t')
if @v=1
	update statistics rep_jde_qat.f4801t stat_f4801t_sys_integration_id
else
	create statistics stat_f4801t_sys_integration_id on rep_jde_qat.f4801t(sys_integration_id)
