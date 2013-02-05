/*
	Notes:
	
	1. The main use of this package is to delete some records in several tables. 
		Performing actual delete will be very time-consuming so instead, all delete statements are converted into 'select count(*) from ...' instead of 'delete from ...'

	2. Results are saved into a temporary table and queried at the end of test routine.
	3. Same modification is #1 is done in Oracle script and then test is ran.
	4. Rows affected on both databases are compared.
	5. When the test is done, there's a need to recompile the actual procedures found in a separate file.
	
*/

if exists (select 1 from tempdb..sysobjects where id = object_id('#pkg_etmm_data_delete') and type = 'U')
   drop table #pkg_etmm_data_delete 
go
create table #pkg_etmm_data_delete(
   id int identity primary key,
   rowcnt int null, 
   ddl varchar(1000) null
)
go
if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_all') and type = 'P') 
   drop procedure etmm_data_delete_all
go
create procedure etmm_data_delete_all @tbl varchar(64)
as
begin
  declare @stmt varchar(1000)
  print @tbl
  set @stmt = 'delete from ' + @tbl
  insert into #pkg_etmm_data_delete(ddl) values (@stmt)
  set @stmt = 'update #pkg_etmm_data_delete set rowcnt = (select count(*) from ' + @tbl +') where id = ' + convert(varchar(10),@@identity)
  exec (@stmt) 
--  exec ('delete from ' + @tbl)
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_some') and type = 'P') 
   drop procedure etmm_data_delete_some
go
create procedure etmm_data_delete_some @cob_date datetime, @tbl varchar(64), @ops varchar(10)
as
begin
  declare @stmt varchar(256)
  print @tbl
  set @stmt = "delete from " + @tbl + " where cob_date " + @ops + " '" + convert(varchar(64),@cob_date) + "'"
  insert into #pkg_etmm_data_delete(ddl) values (@stmt)
  set @stmt = "select count(*) from " + @tbl + " where cob_date " + @ops + " '" + convert(varchar(64),@cob_date) + "'"
  set @stmt = 'update #pkg_etmm_data_delete set rowcnt = (' + @stmt + ') where id = ' + convert(varchar(10),@@identity)
  exec (@stmt) 
--  exec (@stmt)
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_cascade') and type = 'P') 
   drop procedure etmm_data_delete_cascade 
go
create procedure etmm_data_delete_cascade @tbl varchar(64), @parent_table varchar(64), @fk_name varchar(64)
as
begin
  declare @stmt varchar(256)
  print @tbl
  set @stmt = "delete from " + @tbl + " where not exists (select 1 from " + @parent_table + " p where " + @tbl + "." + @fk_name + "=p."+ @fk_name + ")"
  insert into #pkg_etmm_data_delete(ddl) values (@stmt)
  set @stmt = "select count(*) from " + @tbl + " where not exists (select 1 from " + @parent_table + " p where " + @tbl + "." + @fk_name + "=p."+ @fk_name + ")"
  set @stmt = 'update #pkg_etmm_data_delete set rowcnt = (' + @stmt + ') where id = ' + convert(varchar(10),@@identity)
  exec (@stmt) 
--  exec (@stmt)
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_audit') and type = 'P') 
   drop procedure etmm_data_delete_audit
go
create procedure etmm_data_delete_audit @tbl varchar(64), @fk_name varchar(64), @convert_to_int bit = 0
as
begin
  declare @stmt varchar(256)
  print 'entity_audit'
  set @stmt = "delete from entity_audit where entity_name = '" + UPPER(@tbl) +"' and not exists (select 1 from " + @tbl + " p where " + case @convert_to_int when 0 then "entity_audit.entity_id" else "convert(numeric(12),entity_audit.entity_id)" end +"=p." + @fk_name +")"
  insert into #pkg_etmm_data_delete(ddl) values (@stmt)
  set @stmt = "select count(*) from entity_audit where entity_name = '" + UPPER(@tbl) +"' and not exists (select 1 from " + @tbl + " p where " + case @convert_to_int when 0 then "entity_audit.entity_id" else "convert(numeric(12),entity_audit.entity_id)" end +"=p." + @fk_name +")"
  set @stmt = 'update #pkg_etmm_data_delete set rowcnt = (' + @stmt + ') where id = ' + convert(varchar(10),@@identity)
  exec (@stmt) 
--  exec (@stmt)
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_other') and type = 'P') 
   drop procedure etmm_data_delete_other
go

create procedure etmm_data_delete_other @cob_date date, @tbl varchar(64)
as exec etmm_data_delete_some @cob_date, @tbl, '<>'
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_this') and type = 'P') 
   drop procedure etmm_data_delete_this 
go
create procedure etmm_data_delete_this @cob_date date, @tbl varchar(64)
as exec etmm_data_delete_some @cob_date, @tbl, '='
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_internal_delete_business') and type = 'P') 
   drop procedure etmm_data_internal_delete_business
go
create procedure etmm_data_internal_delete_business @cob_date date 
as
begin
  print 'Removing business data..'
  print ''
  
  exec etmm_data_delete_this @cob_date, 'breach'  
  exec etmm_data_delete_audit 'breach', 'breach_id', 1  
  exec etmm_data_delete_this @cob_date, 'feed_statistic'    
  
  exec etmm_data_delete_cascade 'breach_review_action_hist', 'breach', 'breach_id'    
  exec etmm_data_delete_cascade 'breach_issuing_reason', 'breach', 'breach_id'    
  exec etmm_data_delete_cascade 'breach_breach_type', 'breach', 'breach_id'    
  exec etmm_data_delete_cascade 'breach_pfe', 'breach', 'breach_id'    
  exec etmm_data_delete_cascade 'breach_trade', 'breach', 'breach_id'    
  exec etmm_data_delete_cascade 'margin_trade', 'breach_trade', 'etmm_trade_id'    
  exec etmm_data_delete_cascade 'money_market_trade', 'breach_trade', 'etmm_trade_id'    
  exec etmm_data_delete_cascade 'cb_trade', 'breach_trade', 'etmm_trade_id'    
  exec etmm_data_delete_cascade 'breach_owner', 'breach', 'owner_id'    
  exec etmm_data_delete_cascade 'breach_comp_attr_cache', 'breach', 'breach_id'    
  exec etmm_data_delete_cascade 'owner_comp_attr_cache', 'breach_owner', 'owner_id'    
  
  print ''
  print 'Removing audit data..'
  print ''
  exec etmm_data_delete_cascade 'entity_audit_detail', 'entity_audit', 'entity_audit_id'
  print ''  
  
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_get_latest_cob') and type = 'SF') 
   drop function etmm_data_get_latest_cob 
go
create function etmm_data_get_latest_cob
returns date
as
begin
  declare @latest_cob_date date
  declare c1 cursor for select top 1 cob_date from cob order by cob_date desc
  open c1
  fetch c1 into @latest_cob_date
  close c1
  deallocate cursor c1
  return @latest_cob_date
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_is_initialized') and type = 'SF') 
   drop function etmm_data_delete_is_initialized 
go
create function etmm_data_delete_is_initialized
returns bit
as
begin
  declare @setting_value varchar(32)
  declare @res bit
  set @res = 0 
  set @setting_value = (select setting_value from etmm_system_setting where setting_name = 'RTB_MANUAL_DATA_DELETE')
  if @setting_value is not null set @res = 1
  return @res
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_is_delete_enabled') and type = 'SF') 
   drop function etmm_data_is_delete_enabled 
go
create function etmm_data_is_delete_enabled
returns bit
as
begin
  declare @is_delete_enabled varchar(32)
  declare @res bit
  set @is_delete_enabled = (select setting_value from etmm_system_setting where setting_name = 'RTB_MANUAL_DATA_DELETE')
  if @is_delete_enabled = '1' set @res = 1
  return @res
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_restrict') and type = 'P') 
   drop procedure etmm_data_delete_restrict 
go
create procedure etmm_data_delete_restrict @optn varchar(64)
as
begin
  if dbo.etmm_data_delete_is_initialized() = 0
     insert into etmm_system_setting(setting_name, setting_value)
	 values ('RTB_MANUAL_DATA_DELETE', '0')
  if upper(@optn) = 'ENABLE' 
     update etmm_system_setting set setting_value = '1' where setting_name = 'RTB_MANUAL_DATA_DELETE'
  else	 
     update etmm_system_setting set setting_value = '0' where setting_name = 'RTB_MANUAL_DATA_DELETE'
  commit
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_foreign_key') and type = 'U') 
   drop table etmm_foreign_key 
go
create table etmm_foreign_key (
  constraint_name     varchar(64),
  table_name         varchar(64),
  fk_columns         varchar(512),
  reftable_name      varchar(64),
  refkey_columns     varchar(512)
)
go

if exists (select 1 from sysobjects where id = object_id('etmm_foreign_key_refresh') and type = 'P') 
   drop procedure etmm_foreign_key_refresh 
go
create procedure etmm_foreign_key_refresh @truncate_first bit
as
begin
  if isnull(@truncate_first,0) = 1
     truncate table etmm_foreign_key
	 
  insert into etmm_foreign_key
  select object_name(constrid) constraint_name,
         object_name(tableid) table_name,
         col_name(tableid, fokey1) + 
         case when keycnt > 1 then ',' + col_name(tableid, fokey2) end  + 
         case when keycnt > 2 then ',' + col_name(tableid, fokey3) end  + 
         case when keycnt > 3 then ',' + col_name(tableid, fokey4) end  + 
         case when keycnt > 4 then ',' + col_name(tableid, fokey5) end  + 
         case when keycnt > 5 then ',' + col_name(tableid, fokey6) end  + 
         case when keycnt > 6 then ',' + col_name(tableid, fokey7) end  + 
         case when keycnt > 7 then ',' + col_name(tableid, fokey8) end  + 
         case when keycnt > 8 then ',' + col_name(tableid, fokey9) end  + 
         case when keycnt > 9 then ',' + col_name(tableid, fokey10) end  + 
         case when keycnt > 10 then ',' + col_name(tableid, fokey11) end  + 
         case when keycnt > 11 then ',' + col_name(tableid, fokey12) end  + 
         case when keycnt > 12 then ',' + col_name(tableid, fokey13) end  + 
         case when keycnt > 13 then ',' + col_name(tableid, fokey14) end  + 
         case when keycnt > 14 then ',' + col_name(tableid, fokey15) end  + 
         case when keycnt > 15 then ',' + col_name(tableid, fokey16) end fk_columns,
		 object_name(SR.reftabid) reftable_name,
         col_name(reftabid, refkey1) + 
         case when keycnt > 1 then ',' + col_name(reftabid, refkey2) end  + 
         case when keycnt > 2 then ',' + col_name(reftabid, refkey3) end  + 
         case when keycnt > 3 then ',' + col_name(reftabid, refkey4) end  + 
         case when keycnt > 4 then ',' + col_name(reftabid, refkey5) end  + 
         case when keycnt > 5 then ',' + col_name(reftabid, refkey6) end  + 
         case when keycnt > 6 then ',' + col_name(reftabid, refkey7) end  + 
         case when keycnt > 7 then ',' + col_name(reftabid, refkey8) end  + 
         case when keycnt > 8 then ',' + col_name(reftabid, refkey9) end  + 
         case when keycnt > 9 then ',' + col_name(reftabid, refkey10) end  + 
         case when keycnt > 10 then ',' + col_name(reftabid, refkey11) end  + 
         case when keycnt > 11 then ',' + col_name(reftabid, refkey12) end  + 
         case when keycnt > 12 then ',' + col_name(reftabid, refkey13) end  + 
         case when keycnt > 13 then ',' + col_name(reftabid, refkey14) end  + 
         case when keycnt > 14 then ',' + col_name(reftabid, refkey15) end  + 
         case when keycnt > 15 then ',' + col_name(reftabid, refkey16) end refkey_columns
  from sysreferences SR
  where not exists (select 1 from etmm_foreign_key where etmm_foreign_key.constraint_name = object_name(SR.constrid))  
  if @@error > 0 
     print 'Error occured when refreshing etmm_foreign_key_refresh'
  
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_manage_constraints') and type = 'P') 
   drop procedure etmm_data_manage_constraints 
go
create procedure etmm_data_manage_constraints @optn varchar(32), @constrnt varchar(128)
as
begin
  declare @constraint_name  varchar(64)
  declare @table_name       varchar(64)
  declare @fk_columns       varchar(512)
  declare @reftable_name    varchar(64)
  declare @refkey_columns   varchar(512)
  declare @stmt             varchar(1024)

  declare c cursor for
     select constraint_name, table_name, fk_columns, reftable_name, refkey_columns
	   from etmm_foreign_key 
	  where lower(@constrnt) = (case when upper(@constrnt) = 'ALL' then lower(@constrnt) else lower(constraint_name) end) 

  set @optn = upper(@optn)
  if @optn = 'DISABLE' execute etmm_foreign_key_refresh 0 
  open c
  fetch c into @constraint_name, @table_name, @fk_columns, @reftable_name, @refkey_columns 
  
  while (@@sqlstatus = 0)
  begin
     if @optn = 'ENABLE'
        set @stmt = 'alter table ' + @table_name + ' add constraint ' + @constraint_name +
		            ' foreign key (' + @fk_columns + ') references ' + @reftable_name +
					' (' + @refkey_columns + ')'
	 else
	    set @stmt = 'alter table ' + @table_name + ' drop constraint ' + @constraint_name
	 exec(@stmt)
     if @@error > 0
     begin
        set @stmt = 'Error occured while ' + case when @optn='ENABLE' then 'enabling' else 'disabling' end + ' the constraint named ' + @constraint_name + ' on table ' + @table_name 
        print @stmt	 
	 end
     fetch c into @constraint_name, @table_name, @fk_columns, @reftable_name, @refkey_columns 
  end
  close c
  deallocate cursor c
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_manage_triggers') and type = 'P') 
   drop procedure etmm_data_manage_triggers 
go
create procedure etmm_data_manage_triggers @optn varchar(32), @trigger_name varchar(128)
as
begin
  declare @trg_name         varchar(128)
  declare @table_name       varchar(64)
  declare @stmt             varchar(1024)

  declare c cursor for
		select 'table_name' = name
			  ,'trigger_name' = object_name(instrig)
		  from sysobjects
		 where type='U'
		   and instrig > 0
		   and lower(@trigger_name) IN ('all', lower(object_name(instrig))) 
		union
		select 'table_name' = name
			  ,'trigger_name' = object_name(updtrig)
		  from sysobjects
		 where type='U'
		   and updtrig > 0
		   and lower(@trigger_name) IN ('all', lower(object_name(updtrig))) 
		union
		select 'table_name' = name
			  ,'trigger_name' = object_name(deltrig)
		  from sysobjects
		 where type='U'
		 and deltrig > 0
		   and lower(@trigger_name) IN ('all', lower(object_name(deltrig))) 
	    

  set @optn = upper(@optn)
  open c
  fetch c into @table_name, @trg_name 
  
  while (@@sqlstatus = 0)
  begin
     set @stmt = 'alter table ' + @table_name + ' ' + @optn + ' trigger ' + @trg_name
	 exec(@stmt)
     if @@error > 0
     begin
        set @stmt = 'Error occured while ' + case when @optn='ENABLE' then 'enabling' else 'disabling' end + ' the trigger named ' + @trg_name + ' on table ' + @table_name 
        print @stmt	 
	 end
     fetch c into @table_name, @trg_name
  end
  close c
  deallocate cursor c
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_for_cob_date') and type = 'P') 
   drop procedure etmm_data_delete_for_cob_date 
go
create procedure etmm_data_delete_for_cob_date @cob_date date
as
begin
  declare @stmt varchar(1024)
  if dbo.etmm_data_is_delete_enabled() = 1
    begin
          print 'ETMM Data Delete'
          set @stmt = 'This routine will remove business and history data for date:' + convert(varchar(12), @cob_date) + ' existing in the environment: ' + user_name()  
          print @stmt 
          print ''
          exec etmm_data_manage_constraints 'disable', 'all'
          exec etmm_data_manage_triggers 'disable', 'all'
          print 'Processing'
          print ''
          print 'Removing history data..'
          print ''
          exec etmm_data_delete_this @cob_date, 'limit_detail_hist'
          exec etmm_data_delete_this @cob_date, 'exposure_detail_hist'
          exec etmm_data_delete_this @cob_date, 'money_market_trade_hist'
          exec etmm_data_delete_this @cob_date, 'margin_trade_hist'
          exec etmm_data_delete_this @cob_date, 'cb_trade_hist'
          exec etmm_data_delete_this @cob_date, 'portfolio_detail_hist'
          exec etmm_data_delete_this @cob_date, 'owner_hist'
          print ''
          exec etmm_data_internal_delete_business @cob_date
          print 'Removing COB'
          exec etmm_data_delete_this @cob_date, 'cob'
          print ''
          exec etmm_data_manage_triggers 'enable', 'all'
          exec etmm_data_manage_constraints 'enable', 'all'
          commit
    end
  exec etmm_data_delete_restrict 'disable'
end
go

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_business_for_cob_date') and type = 'P') 
   drop procedure etmm_data_delete_business_for_cob_date 
go
create procedure etmm_data_delete_business_for_cob_date @cob_date date
as
begin
  declare @msg varchar(512)
  if dbo.etmm_data_is_delete_enabled() = 1
    begin
          print 'ETMM Data Delete'
          set @msg = 'This routine will remove business data for date:' || convert(varchar(12), @cob_date) || ' existing in the environment: ' || user_name()  
          print @msg  
          print ''
          exec etmm_data_manage_constraints 'disable', 'all'
          exec etmm_data_manage_triggers 'disable', 'all'
          set @msg = 'Processing ' + convert(varchar(32), @cob_date) 
          print @msg
          print ''
          exec etmm_data_internal_delete_business @cob_date

          exec etmm_data_manage_triggers 'enable', 'all'
          exec etmm_data_manage_constraints 'enable', 'all'
          commit
    end
    exec etmm_data_delete_restrict 'disable'
end
go

execute etmm_foreign_key_refresh 1
go

begin
	if exists (select 1 from tempdb..sysobjects where id = object_id('#pkg_etmm_data_delete') and type = 'U')
	   truncate table #pkg_etmm_data_delete
	else
	   exec ('create table #pkg_etmm_data_delete(id int identity primary key, rowcnt int null, ddl varchar(1000) null)')      

	execute etmm_foreign_key_refresh 1
	execute etmm_data_delete_restrict 'enable'
	execute etmm_data_delete_business_for_cob_date '16-NOV-2011'
	select * from #pkg_etmm_data_delete
	
/* When the script is run for the first time and successful it should return the following:

	id	rowcnt		ddl
	--	------		-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	1	241			delete from breach where cob_date = convert(date,'Nov 16 2011 12:00AM',5)
	2	347121		delete from entity_audit where entity_name = UPPER('breach') and not exists (select 1 from breach p where entity_audit.entity_id=convert(varchar(200), breach.breach_id))
	3	0			delete from feed_statistic where cob_date = convert(date,'Nov 16 2011 12:00AM',5)
	4	527185		delete from breach_review_action_hist where not exists (select 1 from breach p where breach_review_action_hist.breach_id=p.breach_id)
	5	27278		delete from breach_issuing_reason where not exists (select 1 from breach p where breach_issuing_reason.breach_id=p.breach_id)
	6	70844		delete from breach_breach_type where not exists (select 1 from breach p where breach_breach_type.breach_id=p.breach_id)
	7	0			delete from breach_pfe where not exists (select 1 from breach p where breach_pfe.breach_id=p.breach_id)
	8	393438		delete from breach_trade where not exists (select 1 from breach p where breach_trade.breach_id=p.breach_id)
	9	0			delete from margin_trade where not exists (select 1 from breach_trade p where margin_trade.etmm_trade_id=p.etmm_trade_id)
	10	64			delete from money_market_trade where not exists (select 1 from breach_trade p where money_market_trade.etmm_trade_id=p.etmm_trade_id)
	11	0			delete from cb_trade where not exists (select 1 from breach_trade p where cb_trade.etmm_trade_id=p.etmm_trade_id)
	12	0			delete from breach_owner where not exists (select 1 from breach p where breach_owner.owner_id=p.owner_id)
	13	30166		delete from breach_comp_attr_cache where not exists (select 1 from breach p where breach_comp_attr_cache.breach_id=p.breach_id)
	14	1882		delete from owner_comp_attr_cache where not exists (select 1 from breach_owner p where owner_comp_attr_cache.owner_id=p.owner_id)
	15	222884		delete from entity_audit_detail where not exists (select 1 from entity_audit p where entity_audit_detail.entity_audit_id=p.entity_audit_id)

		The row counts above are of same result as tested in Oracle.
*/	
end
