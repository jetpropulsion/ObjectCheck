use [DataWorks]				--change the database name to your preference
go

/*
create schema utl;			--if you decide to use name 'utl' in your new database, execute this; otherwise, rename 'utl' schema through the scripts
go
*/

set nocount on
set transaction isolation level read uncommitted
set deadlock_priority low
set textsize -1				--reset to unlimited
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
left join sys.indexes i on o.[parent_object_id] = i.[object_id]
where o.is_ms_shipped = 0
and schema_name(o.[schema_id]) = N'utl'

-------------------------------------------------------------------------------------------------------------------
-- Tables
-------------------------------------------------------------------------------------------------------------------

if not exists ( select top 1 1 from #objs where obj_name = N'obj_name' and obj_type = 'U' )
begin
	--This table contains unique object names including a type
	--for example, view and procedure will have different assigned obj_name_id for same database, schema and name
	create table utl.obj_name
	(
		obj_name_id bigint identity(1,1) not null constraint PK_obj_name_obj_name_id primary key
		, data_name sysname not null			--Database name
		, sch_name sysname not null				--Schema name
		, obj_name sysname not null				--Object name
		, obj_type varchar(2) not null			--Object type
	);
	print 'created table utl.obj_name'
end
go
if not exists ( select top 1 1 from #objs o where o.par_name = N'obj_name' and o.idx_name = N'IX_obj_name_data_name_sch_name_obj_name_obj_type' )
begin
	create unique index IX_obj_name_data_name_sch_name_obj_name_obj_type on utl.obj_name (data_name, sch_name, obj_name, obj_type);
	print 'created index IX_obj_name_data_name_sch_name_obj_name_obj_type'
end
go

if not exists ( select top 1 1 from #objs where obj_name = N'obj_defs' and obj_type = 'U' )
begin
	create table utl.obj_defs
	(
		--This table will contain unique object definition
		--Critical assumption is that combination of length and SHA2 hash is always unique
		obj_def_id bigint identity(1,1) not null constraint PK_obj_defs_obj_def_id primary key
		, obj_len int not null
		, obj_hash varbinary(32) not null	--sized for SHA2_256
		, obj_def varbinary(max) not null
		, processed_ts datetime not null default(getutcdate())
	);
	print 'created table utl.obj_defs'
end
go

if not exists ( select top 1 1 from #objs o where o.par_name = N'obj_defs' and o.idx_name = N'IX_obj_defs_obj_len_obj_hash' )
begin
	create unique index IX_obj_defs_obj_len_obj_hash on utl.obj_defs (obj_len, obj_hash);
	print 'created index IX_obj_defs_obj_len_obj_hash'
end
go

if not exists ( select top 1 1 from #objs where obj_name = N'ref_reason' and obj_type = 'U' )
begin
	--This table is dictionary for the type of the change
	create table utl.ref_reason
	(
		reason_id tinyint not null constraint PK_ref_reason_reason_id primary key
		, reason_name nvarchar(32) not null
	);
	insert utl.ref_reason(reason_id, reason_name)
	values
	  (cast(0x01 as tinyint), N'New')
	, (cast(0x02 as tinyint), N'Deleted')
	, (cast(0x04 as tinyint), N'Modified')
	, (cast(0x08 as tinyint), N'Renamed')
	;
end
go

if not exists ( select top 1 1 from #objs where obj_name = N'obj_hist' and obj_type = 'U' )
begin
	--This is the main history table
	create table utl.obj_hist
	(
		obj_hist_id bigint not null identity(1,1) constraint PK_obj_hist_obj_hist_id primary key
		, reason_id tinyint not null constraint FK_obj_hist__ref_reason_reason_id foreign key references utl.ref_reason(reason_id)
		, obj_name_id bigint not null constraint FK_obj_hist__obj_name_obj_name_id foreign key references utl.obj_name(obj_name_id)
		, data_id int null
		, sch_id int null
		, obj_id int not null
		, created_ts datetime null
		, modified_ts datetime null
		, previous_id bigint null constraint FK_obj_hist__obj_hist_obj_hist_id foreign key references utl.obj_hist(obj_hist_id)
		, processed_ts datetime not null default(getutcdate())
		, obj_def_id bigint null constraint FK_obj_hist__obj_defs_obj_def_id references utl.obj_defs(obj_def_id)
		, change_map bigint not null default(0)
	);
	print 'created table utl.obj_hist'
end
go

if not exists ( select top 1 1 from #objs o where o.par_name = N'obj_hist' and o.idx_name = N'IX_obj_hist_reason_id' )
begin
	create index IX_obj_hist_reason_id on utl.obj_hist (reason_id) where reason_id in (0x01, 0x02, 0x04, 0x08);
	print 'created filtered index IX_obj_hist_reason_id'
end
go

if not exists ( select top 1 1 from #objs where obj_name = N'ref_change_map' and obj_type = 'U' )
begin
	--This table is a dictionary holding the precalculated combination of possbile changes
	drop table if exists #change_map;
	create table #change_map(change_map bigint not null default(0), change_desc varchar(max) not null);
	insert #change_map(change_map, change_desc)
	select cast(map.change_map as tinyint) as change_map, map.change_desc
	from
	(
		values
			(0x01, 'data_id'	)
		,	(0x02, 'sch_id'		)
		,	(0x04, 'obj_id'		)
		,	(0x08, 'created_ts'	)
		,	(0x10, 'modified_ts')
		,	(0x20, 'obj_len'	)
		,	(0x40, 'obj_hash'	)
		,	(0x80, 'obj_def'	)
	)
	as map(change_map, change_desc);

	drop table if exists #mapping;
	;with cte as
	(
		select 1 as x, 1 as y, cast('' as varchar(max)) as dsc
		union all
		select iif(y = 0, cte.x + 1, cte.x) as x, iif(y = 0, cte.x + 1, y & y - 1) as y, dsc as dsc
		from cte cte
		where cte.x < 256
	)
	select cte.x as xx, cte.y as xy, cte.x, cte.y, cm.change_map, cm.change_desc
	into #mapping
	from cte cte
	join #change_map cm on cm.change_map = cte.y
	where cte.x & cte.y = cte.y and cte.y != 0
	order by cte.x
	option(maxrecursion 0);

	create table utl.ref_change_map(change_map bigint not null constraint PK_ref_change_map_change_map primary key, change_desc nvarchar(max));
	insert utl.ref_change_map(change_map, change_desc)
	select cast(m.xx as bigint) as change_map, string_agg(m.change_desc, ', ') as change_desc
	from #mapping m
	group by m.xx
	order by m.xx

	insert utl.ref_change_map(change_map, change_desc) values (0, N'')	--entry for no changes at value 0

	print 'created table utl.ref_change_map'
end
go

-------------------------------------------------------------------------------------------------------------------
-- Procedures
-------------------------------------------------------------------------------------------------------------------
drop procedure if exists utl.usp_obj_refresh;
go

--This is the main detection procedure, updates obj_hist, obj_defs, obj_names
create procedure utl.usp_obj_refresh
as
begin
	set nocount on
	set transaction isolation level read uncommitted
	set deadlock_priority low
	set textsize -1				--reset to unlimited

	declare @now datetime = getutcdate()
	declare @msg nvarchar(max) = null
	declare @rows int = 0

	declare @reason_new tinyint =		0x01	--(select r.reason_id from utl.ref_reason r where r.reason_name = N'New')
	declare @reason_deleted tinyint =	0x02	--(select r.reason_id from utl.ref_reason r where r.reason_name = N'Deleted')
	declare @reason_modified tinyint =	0x04	--(select r.reason_id from utl.ref_reason r where r.reason_name = N'Modified')
	declare @reason_renamed tinyint =	0x08	--(select r.reason_id from utl.ref_reason r where r.reason_name = N'Renamed')

	--Log table
	drop table if exists #msgs;
	create table #msgs(id int identity(1,1) not null, txt nvarchar(1000) not null, ts datetime not null default(getdate()), dur int null);
	insert #msgs(txt) values ( N'STARTED' );

	--This table will hold recalculated objects
	drop table if exists #temp_objects;
	create table #temp_objects
	(
		temp_object_id bigint not null identity(1,1) primary key
		, obj_name_id bigint null
		, data_name sysname not null
		, sch_name sysname not null
		, obj_name sysname not null
		, data_id int not null
		, sch_id int not null
		, obj_id int not null
		, obj_type varchar(2) not null
		, obj_hash varbinary(32) not null			--sized for SHA2_256
		, obj_len int not null
		, obj_def varbinary(max) not null
		, obj_def_id bigint null
		, created_ts datetime not null
		, modified_ts datetime not null
	)
	insert #msgs(txt) values ( N'#temp_objects created' );

	declare data_cursor cursor local forward_only read_only fast_forward for
		select d.[name]
		from sys.sysdatabases d(nolock)
		--where d.[name] not in (N'master', N'tempdb', N'model', N'msdb')
		order by d.[name];

	declare @data_name sysname
	open data_cursor
	fetch next from data_cursor into @data_name

	declare @cr nchar(1) = nchar(13)
	declare @lf nchar(1) = nchar(10)
	declare @crlf nchar(2) = @cr + @lf

	set @rows = 0;
	declare @count int = 0;
	while(@@fetch_status = 0)
	begin
		declare @sql nvarchar(max) = concat
		(
			N'set @result = 0;', @crlf
			, N'declare @tz_offset int = datediff(minute, getdate(), getutcdate());', @crlf
			, N'with def as', @crlf
			, N'(', @crlf
			, N'  select o.[object_id], o.[type], o.[schema_id] as sch_id, schema_name(o.[schema_id]) as sch_name, o.[name] as obj_name, dateadd(minute, @tz_offset, o.create_date) as create_date, dateadd(minute, @tz_offset, o.modify_date) as modify_date, body.val as body, body_len.val as body_len', @crlf
			, N'  from ( select o.* from sys.objects o where o.[type] in (''P'', ''FN'', ''TF'', ''TR'', ''IF'', ''V'', ''S'', ''SO'') ) as o', @crlf
			, N'  cross apply ( select object_definition(o.[object_id]) as val ) as body', @crlf
			, N'  cross apply ( select iif(body.val is null, 0, len(body.val)) as val ) as body_len', @crlf
			, N'  cross apply ( select iif(body_len.val = 0, null, compress(body.val)) as val ) as zip', @crlf
			, N')', @crlf
			, N'insert into #temp_objects(data_name, sch_name, obj_name, data_id, sch_id, obj_id, obj_type, obj_len, obj_def, obj_hash, created_ts, modified_ts)', @crlf
			, N'select @data_name, def.sch_name, def.obj_name, db_id(@data_name), def.sch_id, def.[object_id], def.[type], len(def.body), compress(def.body), convert(varbinary(32), hashbytes(N''SHA2_256'', def.body)), def.create_date, def.modify_date', @crlf
			, N'from def def', @crlf
			, N'cross apply ( select len(def.body) as body_len, compress(def.body) as body_comp ) as obj', @crlf
			, N'where def.body is not null;', @crlf
			, N'set @result += @@rowcount;', @crlf
		);

		declare @params nvarchar(max) = N'@result bigint output, @data_name sysname';
		declare @exec nvarchar(1024) = concat(quotename(@data_name), N'.sys.sp_executesql');
		declare @result bigint = 0;
		declare @rc int;

		if @count = 0
		print concat('SQL:', @crlf, @sql, @crlf);

		execute @rc = @exec @sql, @params, @data_name = @data_name, @result = @result output;
		set @rows += @result
		set @count += 1

		fetch next from data_cursor into @data_name
	end
	close data_cursor
	deallocate data_cursor

	insert #msgs(txt) values ( concat(N'cursor finished, ', @rows, N' rows') );

	create index IX_temp_objects_obj_name_id on #temp_objects(obj_name_id);
	insert #msgs(txt) values ( concat(N'indexed #temp_objects, ', @rows, N' rows') );

	--Resolve obj_name_id reference from existing entries, where object snapshot read already exists
	update t set
		t.obj_name_id = i.obj_name_id
	from #temp_objects as t
	join utl.obj_name as i on i.data_name = t.data_name and i.sch_name = t.sch_name and i.obj_name = t.obj_name and i.obj_type = t.obj_type
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'updated name entries, ', @rows, N' rows') );

	--Create new named entries for an object not previously encountered
	insert utl.obj_name(data_name, sch_name, obj_name, obj_type)
	select t.data_name, t.sch_name, t.obj_name, t.obj_type
	from #temp_objects as t
	where t.obj_name_id is null
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'creating new name entries, ', @rows, N' rows') );

	--Resolve obj_name_id reference from existing entries - where object snapshot read is new
	update t set
		t.obj_name_id = i.obj_name_id
	from #temp_objects as t
	join utl.obj_name as i on i.data_name = t.data_name and i.sch_name = t.sch_name and i.obj_name = t.obj_name and i.obj_type = t.obj_type
	where t.obj_name_id is null
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'updated with new name entries, ', @rows, N' rows') );

	--Sanity check, #temp_objects must have all names resolved
	if exists (select top 1 1 from #temp_objects as t where t.obj_name_id is null)
	begin
		raiserror(N'Name not resolved, fatal exit', 15, 1000);
	end

	--Resolve obj_def_id reference from existing entries
	update t set
		t.obj_def_id = d.obj_def_id
	from #temp_objects as t
	join utl.obj_defs d on d.obj_len = t.obj_len and d.obj_hash = t.obj_hash
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'updated object definitions, ', @rows, N' rows') );

	--Create new entries for an object definition not previously encountered
	insert utl.obj_defs(obj_len, obj_hash, obj_def)
	select t.obj_len, t.obj_hash, t.obj_def
	from #temp_objects as t
	where t.obj_def_id is null
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'creating new object definitions, ', @rows, N' rows') );

	update t set
		t.obj_def_id = d.obj_def_id
	from #temp_objects as t
	join utl.obj_defs d on d.obj_len = t.obj_len and d.obj_hash = t.obj_hash
	where t.obj_def_id is null
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'updated with new object definitions, ', @rows, N' rows') );

	--Sanity check, #temp_objects must have all definitions resolved
	if exists (select top 1 1 from #temp_objects as t where t.obj_def_id is null)
	begin
		raiserror(N'Object definition not resolved, fatal exit', 15, 1000);
	end

	--Get the latest version for each database + schema + object name fully qualified name
	drop table if exists #temp_latest
	create table #temp_latest(obj_name_id int not null, obj_hist_id bigint not null, reason_id tinyint);
	insert #temp_latest(obj_name_id, obj_hist_id, reason_id)
	select c.obj_name_id, c.obj_hist_id, c.reason_id
	from
	(
		select row_number() over (partition by obj_name_id order by c.obj_hist_id desc) as rn, c.obj_name_id, c.obj_hist_id, c.reason_id
		from utl.obj_hist as c
	)
	as c
	where c.rn = 1
	--where c.reason_id != @reason_deleted
	--group by c.obj_name_id
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'created #temp_latest, ', @rows, N' rows') );

	--select * from #temp_latest as l

	--Mark deleted entries
	drop table if exists #temp_deleted;
	create table #temp_deleted(obj_hist_id bigint not null);
	insert #temp_deleted(obj_hist_id)
	select c.obj_hist_id
	from #temp_latest as c
	where not exists (select top 1 1 from #temp_objects as t2 where t2.obj_name_id = c.obj_name_id)
	and c.reason_id != @reason_deleted

	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'marked deleted, ', @rows, N' rows') );

	--Update existing record from new readings
	drop table if exists #temp_modified;
	create table #temp_modified(temp_object_id bigint not null, obj_hist_id bigint null, change_map bigint null);
	insert #temp_modified(temp_object_id, obj_hist_id, change_map)
	select
		t.temp_object_id
		, c.obj_hist_id
		, iif(data_id_test.val is not null, 1, 0)
		+ iif(sch_id_test.val is not null, 2, 0)
		+ iif(obj_id_test.val is not null, 4, 0)
		+ iif(created_ts_test.val is not null, 8, 0)
		+ iif(modified_ts_test.val is not null, 16, 0)
		+ iif(obj_len_test.val is not null, 32, 0)
		+ iif(obj_hash_test.val is not null, 64, 0)
		+ iif(obj_def_test.val is not null, 128, 0)
	from #temp_objects as t
	join #temp_latest as i on i.obj_name_id = t.obj_name_id and i.reason_id != @reason_deleted
	outer apply
	(
		select top 1 h2.*
		from utl.obj_hist as h2
		join #temp_latest as t2 on t2.obj_hist_id = h2.obj_hist_id
		where h2.obj_name_id = t.obj_name_id
	)
	as c
	left join utl.obj_defs as dc on dc.obj_def_id = c.obj_def_id
	left join utl.obj_defs as dt on dt.obj_def_id = t.obj_def_id
	outer apply ( select iif(coalesce(dc.obj_len, dt.obj_len) is null or (dt.obj_len = dc.obj_len), null, isnull(dc.obj_len, dt.obj_len)) as val ) as obj_len_test
	outer apply ( select iif(coalesce(dc.obj_hash, dt.obj_hash) is null or (dt.obj_hash = dc.obj_hash), null, isnull(dc.obj_hash, dt.obj_hash)) as val ) as obj_hash_test
	outer apply ( select iif(coalesce(dc.obj_def, dt.obj_def) is null or (dt.obj_def = dc.obj_def), null, isnull(dc.obj_def, dt.obj_def)) as val ) as obj_def_test
	cross apply ( select iif(coalesce(c.data_id, t.data_id) is null or (t.data_id = c.data_id), null, isnull(c.data_id, t.data_id)) as val ) as data_id_test
	cross apply ( select iif(coalesce(c.sch_id, t.sch_id) is null or (t.sch_id = c.sch_id), null, isnull(c.sch_id, t.sch_id)) as val ) as sch_id_test
	cross apply ( select iif(coalesce(c.obj_id, t.obj_id) is null or (t.obj_id = c.obj_id), null, isnull(c.obj_id, t.obj_id)) as val ) as obj_id_test
	cross apply ( select iif(coalesce(c.created_ts, t.created_ts) is null or (t.created_ts = c.created_ts), null, isnull(c.created_ts, t.created_ts)) as val ) as created_ts_test
	cross apply ( select iif(coalesce(c.modified_ts, t.modified_ts) is null or (t.modified_ts = c.modified_ts), null, isnull(c.modified_ts, t.modified_ts)) as val ) as modified_ts_test
	outer apply ( select iif(coalesce(data_id_test.val, sch_id_test.val, obj_id_test.val, created_ts_test.val, modified_ts_test.val, obj_len_test.val, obj_hash_test.val, obj_def_test.val) is not null, 1, 0) as is_modified ) as test
	where test.is_modified = 1
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'updated #temp_modified, ', @rows, N' rows') );

	--Update deleted records
	update c set
		c.processed_ts = @now
		, c.reason_id = @reason_deleted
	from utl.obj_hist as c
	join #temp_deleted as d on d.obj_hist_id = c.obj_hist_id
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'deleted, ', @rows, N' rows') );

	--Update modified records
	insert utl.obj_hist(reason_id, obj_name_id, data_id, sch_id, obj_id, created_ts, modified_ts, previous_id, processed_ts, obj_def_id, change_map)
	select @reason_modified, t.obj_name_id, t.data_id, t.sch_id, t.obj_id, t.created_ts, t.modified_ts, m.obj_hist_id, @now, t.obj_def_id, m.change_map
	from #temp_modified as m
	join #temp_objects as t on t.temp_object_id = m.temp_object_id
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'updated, ', @rows, N' rows') );

	--Insert new records
	insert utl.obj_hist(reason_id, obj_name_id, data_id, sch_id, obj_id, created_ts, modified_ts, previous_id, processed_ts, obj_def_id, change_map)
	select @reason_new, t.obj_name_id, t.data_id, t.sch_id, t.obj_id, t.created_ts, t.modified_ts, null, @now, t.obj_def_id, 0
	from #temp_objects as t
	left join #temp_latest as l on l.obj_name_id = t.obj_name_id
	where l.obj_name_id is null or l.reason_id = @reason_deleted
	set @rows = @@rowcount
	insert #msgs(txt) values ( concat(N'inserted, ', @rows, N' rows') );

	---------------------------------------------------------------------------------------------------------------------------------------
	-- Calculate duration for performance log
	---------------------------------------------------------------------------------------------------------------------------------------
report:
	insert #msgs(txt) values ( N'FINISHED' );

	update m2 set
		m2.dur = datediff(ms, m1.ts, m2.ts)
	from #msgs m1
	left join #msgs as m2 on m2.id = m1.id + 1

	declare @log_entries int
	declare @log_entry_width int
	declare @total_duration int
	select @log_entries = max(id), @log_entry_width = max(len(txt)), @total_duration = sum(dur) from #msgs

	declare @i int = 1
	declare @pad int = 8
	while @i <= @log_entries
	begin
		select @msg = concat(convert(nvarchar(100), m.ts, 121), N' | ', right(replicate(' ', @pad) + convert(nvarchar(16), isnull(m.dur, 0)), @pad), N' | ', m.txt) from #msgs m where m.id = @i
		--raiserror(@msg, 0, 1) with nowait
		print @msg
		set @i += 1
	end

	select @msg = concat(N'Total duration: ',isnull(@total_duration, 0), N' ms')
	raiserror(@msg, 0, 1) with nowait
end
go

-------------------------------------------------------------------------------------------------------------------
-- Views
-------------------------------------------------------------------------------------------------------------------

--This view displays the overview of the changes found in all databases, show hashes and fetch compressed objects
drop view if exists utl.vw_obj_hist_detail;
go
create view utl.vw_obj_hist_detail
as
	--Shows all history entries
	select r.reason_name, cm.change_desc, c.obj_hist_id, c.previous_id, c.obj_name_id, c.data_id, c.sch_id, c.obj_id
	, i.data_name, i.sch_name, i.obj_name, i.obj_type
	, dc.obj_len, dc.obj_hash
	, c.created_ts, c.modified_ts, c.processed_ts
	, datalength(dc.obj_def) as zip_len_curr, dc.obj_def as zip_obj_curr
	--, convert(nvarchar(max), decompress(dc.obj_def)) as obj_def_curr
	from utl.obj_hist as c
	join utl.obj_name as i on i.obj_name_id = c.obj_name_id
	left join utl.obj_defs as dc on dc.obj_def_id = c.obj_def_id
	left join utl.ref_reason as r on r.reason_id = c.reason_id
	left join utl.ref_change_map as cm on cm.change_map = c.change_map
go

--This view displays the overview of the changes found in all databases, you can retrieve objects by querying obj_defs via obj_def_id
drop view if exists utl.vw_obj_hist;
go
create view utl.vw_obj_hist
as
	--Shows all history entries
	select r.reason_name, cm.change_desc, c.obj_hist_id, c.previous_id, c.obj_name_id, c.data_id, c.sch_id, c.obj_id
	, i.data_name, i.sch_name, i.obj_name, i.obj_type
	, c.obj_def_id
	, c.created_ts, c.modified_ts, c.processed_ts
	from utl.obj_hist as c
	join utl.obj_name as i on i.obj_name_id = c.obj_name_id
	left join utl.obj_defs as dc on dc.obj_def_id = c.obj_def_id
	left join utl.ref_reason as r on r.reason_id = c.reason_id
	left join utl.ref_change_map as cm on cm.change_map = c.change_map
go
