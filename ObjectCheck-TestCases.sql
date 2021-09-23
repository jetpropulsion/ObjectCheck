use [DataWorks]				--change the database name to your preference
go

set nocount on;

--Execute this procedure via job, periodically
exec utl.usp_obj_refresh

--Display overview of the changes found in all databases
select v.* from utl.vw_obj_hist v order by v.modified_ts desc

--Display overview of the changes found in all databases, show hashes and fetch compressed objects
select v.* from utl.vw_obj_hist_detail v order by v.modified_ts desc


/*
--You can also run it like this, in separate query editor window
while 1=1
begin
	exec utl.usp_obj_refresh
	waitfor delay '00:00:10.000';	--sleep 10 seconds
end

--Several rename / create cases; note that alter would make CREATE word in uppercase, changing the object hash
drop view get_table_info
go
create view get_table_info as select 1 as [1]
go
exec sp_rename N'get_table_info', N'get_table_info2'
go
drop view get_table_info
go
create view get_table_info as select 2 as [1]
go
drop view get_table_info
go
alter view get_table_info as select 1 as [1]
exec sp_refreshview N'get_table_info'
go
alter view get_table_info as select 2 as [1]
exec sp_refreshview N'get_table_info'
go
alter view get_table_info as select 3 as [1]
exec sp_refreshview N'get_table_info'
go

exec sp_rename N'get_table_info2', N'get_table_info'

select hashbytes('SHA2_256', N'create view get_table_info as select 1 as [1]')			--0x459AA2F2D8E320E7BB415AC1D2B7672E8BD648B30E10AA4BB43C5E8E15F4C697
select hashbytes('SHA2_256', N'create view get_table_info as select 2 as [1]')			--0x2A24CAD44F004A7D8A33F98F699122B9C3D692C9406B8874BC7EE417277879CB
select hashbytes('SHA2_256', N'create view get_table_info as select 3 as [1]')			--0xE802CBC56001BDE8D6702E4FADE8F07EC83988EFFEFBC59F1582AB468E8E157C
select hashbytes('SHA2_256', N'CREATE view get_table_info as select 1 as [1]')			--0x9F09DDB4CAE0D652C0565025C505A513F8AF3E581736E921C606D7B91E3D0E7E
select hashbytes('SHA2_256', N'CREATE view get_table_info as select 2 as [1]')			--0x2F89AD244CC70C322EFA7F789977400022C39A14F20EB6EDF9B6C9216BCAA4D7
select hashbytes('SHA2_256', N'CREATE view get_table_info as select 3 as [1]')			--0x17C713BB46CFAD36185030265A7CC5C2C4B6B23351CEA5D7C4739092A3D621C0
*/
