use [DataWorks]				--change the database name to your preference
go

set nocount on
go


drop table if exists #objs;
select distinct o.[schema_id] as sch_id
, o.parent_object_id as par_id
, o.[object_id] as obj_id
, i.[object_id] as idx_id
, i.[name] as idx_name
, schema_name(o.[schema_id]) as sch_name
, object_name(o.parent_object_id) as par_name
, o.[name] as obj_name
, o.[type] as obj_type
into #objs
from sys.objects o
left join sys.indexes i on o.[object_id] = i.[object_id] and i.is_primary_key = 0
where o.is_ms_shipped = 0
and schema_name(o.[schema_id]) = N'utl'
--select * from #objs s order by s.obj_name


if exists (select top 1 1 from #objs where obj_name = N'vw_obj_hist' and obj_type = 'V')
begin
	drop view utl.vw_obj_hist
	print 'dropped view utl.vw_obj_hist'
end
if exists (select top 1 1 from #objs where obj_name = N'vw_obj_hist_detail' and obj_type = 'V')
begin
	drop view utl.vw_obj_hist_detail
	print 'dropped view utl.vw_obj_hist_detail'
end



if exists (select top 1 1 from #objs where obj_name = N'usp_obj_refresh' and obj_type = 'P')
begin
	drop procedure utl.usp_obj_refresh
	print 'dropped procedure utl.usp_obj_refresh'
end



if exists (select top 1 1 from #objs o where o.obj_name = N'obj_hist' and o.idx_name = N'IX_obj_hist_reason_id')
begin
	drop index IX_obj_hist_reason_id on utl.obj_hist
	print 'dropped index IX_obj_hist_reason_id'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_hist' and o.obj_name = N'FK_obj_hist__ref_reason_reason_id' and obj_type = 'F')
begin
	alter table utl.obj_hist drop constraint FK_obj_hist__ref_reason_reason_id
	print 'dropped foreign key FK_obj_hist__ref_reason_reason_id'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_hist' and o.obj_name = N'FK_obj_hist__obj_name_obj_name_id' and obj_type = 'F')
begin
	alter table utl.obj_hist drop constraint FK_obj_hist__obj_name_obj_name_id
	print 'dropped foreign key FK_obj_hist__obj_name_obj_name_id'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_hist' and o.obj_name = N'FK_obj_hist__obj_defs_obj_def_id' and obj_type = 'F')
begin
	alter table utl.obj_hist drop constraint FK_obj_hist__obj_defs_obj_def_id
	print 'dropped foreign key FK_obj_hist__obj_defs_obj_def_id'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_hist' and o.obj_name = N'FK_obj_hist__obj_hist_obj_hist_id' and obj_type = 'F')
begin
	alter table utl.obj_hist drop constraint FK_obj_hist__obj_hist_obj_hist_id
	print 'dropped foreign key FK_obj_hist__obj_hist_obj_hist_id'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_hist' and o.obj_name = N'PK_obj_hist_obj_hist_id' and obj_type = 'PK')
begin
	alter table utl.obj_hist drop constraint PK_obj_hist_obj_hist_id;
	print 'dropped primary key PK_obj_hist_obj_hist_id'
end
if exists (select top 1 1 from #objs where obj_name = N'obj_hist' and obj_type = 'U')
begin
	drop table utl.obj_hist;
	print 'dropped table utl.obj_hist'
end



if exists (select top 1 1 from #objs o where o.obj_name = N'obj_defs' and o.idx_name = N'IX_obj_defs_obj_len_obj_hash')
begin
	drop index IX_obj_defs_obj_len_obj_hash on utl.obj_defs
	print 'dropped index IX_obj_defs_obj_len_obj_hash'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_defs' and o.obj_name = N'PK_obj_defs_obj_def_id' and obj_type = 'PK')
begin
	alter table utl.obj_defs drop constraint PK_obj_defs_obj_def_id;
	print 'dropped primary key PK_obj_defs_obj_def_id'
end
if exists (select top 1 1 from #objs where obj_name = N'obj_defs' and obj_type = 'U')
begin
	drop table utl.obj_defs;
	print 'dropped table utl.obj_defs'
end



if exists (select top 1 1 from #objs o where o.par_name = N'ref_change_map' and o.obj_name = N'PK_ref_change_map_change_map' and obj_type = 'PK')
begin
	alter table utl.ref_change_map drop constraint PK_ref_change_map_change_map;
	print 'dropped primary key PK_ref_change_map_change_map'
end
if exists (select top 1 1 from #objs where obj_name = N'ref_change_map' and obj_type = 'U')
begin
	drop table utl.ref_change_map;
	print 'dropped table utl.ref_change_map'
end



if exists (select top 1 1 from #objs where obj_name = N'ref_reason' and obj_type = 'U')
begin
	drop table utl.ref_reason;
	print 'dropped table utl.ref_reason'
end



if exists (select top 1 1 from #objs o where o.obj_name = N'obj_name' and o.idx_name = N'IX_obj_name_data_name_sch_name_obj_name_obj_type')
begin
	drop index IX_obj_name_data_name_sch_name_obj_name_obj_type on utl.obj_name
	print 'dropped index IX_obj_name_data_name_sch_name_obj_name_obj_type'
end
if exists (select top 1 1 from #objs o where o.par_name = N'obj_name' and o.obj_name = N'PK_obj_name_obj_name_id' and obj_type = 'PK')
begin
	alter table utl.obj_name drop constraint PK_obj_name_obj_name_id;
	print 'dropped primary key PK_obj_name_obj_name_id'
end
if exists (select top 1 1 from #objs where obj_name = N'obj_name' and obj_type = 'U')
begin
	drop table if exists utl.obj_name;
	print 'dropped table utl.obj_name'
end

/*
drop schema utl;
*/
