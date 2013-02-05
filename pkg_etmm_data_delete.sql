/*
	Notes:
	
	1. There are only 3 procedures that are mainly important in this package
		a. etmm_data_delete_for_cob_date @cob_date date 			-- this is PKG_ETMM_DATA_DELETE.p_delete_for_cob_date(cob_date)
		b. etmm_data_delete_business_for_cob_date @cob_date date	-- this is PKG_ETMM_DATA_DELETE.p_delete_business_for_cob_date(cob_date)
		c. etmm_data_delete_restrict @optn varchar(64)				-- this is PKG_ETMM_DATA_DELETE.p_restrict(optn)
		
		All other subroutines are just private (as they are in Oracle) and are called in those procedures listed above.

	2. For migrating the procedure that enable/disable constraints, it is implemented using non-temp table and a procedure to refresh its contents.
		This would mean additional table into the schema and an extra procedure here.
		
		Table Name:	etmm_foreign_key
		Structure:	defined below
		
		Procedure to refresh the records in etmm_foreign_key: etmm_foreign_key_refresh @truncate_first bit
	
	3. Sybase will execute full table scan on a query with NOT EXISTS on the WHERE clause of the statement. 
		Unfortunately, this is used a lot here and this is a performance issue, so the subroutines using this style needs to be optimized.
		Optimization is still on-going.
*/

------------------------------
/* Oracle's package header */
------------------------------
-- create or replace 
-- PACKAGE "PKG_ETMM_DATA_DELETE" AS
-- 	c_all CONSTANT VARCHAR2(10) := 'ALL';
--	c_disable CONSTANT VARCHAR2(10) := 'DISABLE';
--	c_enable CONSTANT VARCHAR2(10) := 'ENABLE';

	/*
	 * restrict execution of delete procedures
	 * optn => c_enable / c_disable
	 */
--	PROCEDURE p_restrict(optn IN VARCHAR2);

	/*
	 * delete everything from specified COB
	 * cob_date => date to use
	 */
--	PROCEDURE p_delete_for_cob_date(cob_date IN cob.cob_date%TYPE);

	/*
	 * delete business data from specified COB
	 * cob_date => date to use
	 */
--	PROCEDURE p_delete_business_for_cob_date(cob_date IN cob.cob_date%TYPE);
-- END PKG_ETMM_DATA_DELETE;


------------------------------
/* Oracle's package body */
------------------------------
-- create or replace 
-- PACKAGE BODY "PKG_ETMM_DATA_DELETE" AS
	-- private constants

--	c_setting_data_delete CONSTANT etmm_system_setting.setting_name%TYPE := 'RTB_MANUAL_DATA_DELETE';
--	c_new_line CONSTANT VARCHAR2(2) := CHR(13) || CHR(10);

	-- private procedures

	/*
	 * delete data
	 *
	 * tbl => table name
	 */
--	PROCEDURE p_delete_all(tbl IN VARCHAR2) IS
--	BEGIN
--		dbms_output.put_line(tbl);
--		EXECUTE IMMEDIATE ('DELETE FROM '||tbl);
--	EXCEPTION WHEN OTHERS THEN
--		dbms_output.put_line(sqlerrm);
--		RAISE;
--	END;

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_all') and type = 'P') 
   drop procedure etmm_data_delete_all
go
create procedure etmm_data_delete_all @tbl varchar(64)
as
begin
  print @tbl
  exec ('delete from ' + @tbl)
end
go

	/*
	 * delete data for cob
	 *
	 * cob_date => cob date to use
	 * tbl => table name
	 * ops => comparison operator for cob date
	 */
--	PROCEDURE p_delete_some(cob_date IN cob.cob_date%TYPE, tbl IN VARCHAR2, ops IN VARCHAR2) IS
--	BEGIN
--		dbms_output.put_line(tbl);
--		EXECUTE IMMEDIATE ('DELETE FROM '||tbl||' WHERE cob_date '||ops||' to_date('''||cob_date||''', ''dd-mon-yy'')');
--	EXCEPTION WHEN OTHERS THEN
--		dbms_output.put_line(sqlerrm);
--		RAISE;
--	END;

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_some') and type = 'P') 
   drop procedure etmm_data_delete_some
go
create procedure etmm_data_delete_some @cob_date datetime, @tbl varchar(64), @ops varchar(10)
as
begin
  declare @stmt varchar(256)
  print @tbl
  
  set @stmt = "delete from " + @tbl + " where cob_date " + @ops + " '" + convert(varchar(64),@cob_date) + "'"
  print @stmt
  -- exec (@stmt)
end
go	
	
	/*
	 * cascades delete
	 * tbl => table to cascade delete for
	 * parent_table => cascade owner table
	 * fk_name => cascade column (FK)
	 */
--	PROCEDURE p_delete_cascade(tbl IN VARCHAR2, parent_table IN VARCHAR2, fk_name IN VARCHAR2) IS
--	BEGIN
--		dbms_output.put_line(tbl);
--		EXECUTE IMMEDIATE ('DELETE FROM '||tbl||' c WHERE NOT EXISTS ('||
--			'select 1 from '||parent_table||' p where c.'||fk_name||' = p.'||fk_name||')');
--	END;

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_cascade') and type = 'P') 
   drop procedure etmm_data_delete_cascade 
go
create procedure etmm_data_delete_cascade @tbl varchar(64), @parent_table varchar(64), @fk_name varchar(64)
as
begin
  declare @stmt varchar(256)
  print @tbl
  set @stmt = "delete from " + @tbl + " where not exists (select 1 from " + @parent_table + " p where " + @tbl + "." + @fk_name + "=p."+ @fk_name + ")"
  exec (@stmt)
end
go


	/*
	 * cascade delete ETMM audit records
	 * tbl => table name to cascade for
	 * fk_name => cascade column (FK)
	 */
--	PROCEDURE p_delete_audit(tbl IN VARCHAR2, fk_name IN VARCHAR2) IS
--	BEGIN
--		dbms_output.put_line('entity_audit');
--		EXECUTE IMMEDIATE ('DELETE FROM entity_audit c WHERE entity_name = UPPER('''||tbl||''') AND NOT EXISTS ('||
--			'select 1 from '||tbl||' p where c.entity_id = p.'||fk_name||')');
--	END;

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_audit') and type = 'P') 
   drop procedure etmm_data_delete_audit
go
create procedure etmm_data_delete_audit @tbl varchar(64), @fk_name varchar(64), @convert_to_int bit = 0
as
begin
  declare @stmt varchar(256)
  print 'entity_audit'
  set @stmt = "delete from entity_audit where entity_name = '" + UPPER(@tbl) + "' and not exists (select 1 from " + @tbl + " p where " + case @convert_to_int when 0 then "entity_audit.entity_id" else "convert(numeric(12), entity_audit.entity_id)" end + "=p." + @fk_name +")"
  exec (@stmt)
--  print @stmt
end
go
	
	
	/*
	 * delete all data except for cob
	 *
	 * cob_date => cob date to use
	 * tbl => table name
	 */
--	PROCEDURE p_delete_other(cob_date IN cob.cob_date%TYPE, tbl IN VARCHAR2) IS
--	BEGIN
--		p_delete_some(cob_date, tbl, '<>');
--	END;

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_other') and type = 'P') 
   drop procedure etmm_data_delete_other
go
create procedure etmm_data_delete_other @cob_date date, @tbl varchar(64)
as exec etmm_data_delete_some @cob_date, @tbl, '<>'
go

	/*
	 * delete data for cob
	 *
	 * cob_date => cob date to use
	 * tbl => table name
	 */
--	PROCEDURE p_delete_this(cob_date IN cob.cob_date%TYPE, tbl IN VARCHAR2) IS
--	BEGIN
--		p_delete_some(cob_date, tbl, '=');
--	END;

if exists (select 1 from sysobjects where id = object_id('etmm_data_delete_this') and type = 'P') 
   drop procedure etmm_data_delete_this 
go
create procedure etmm_data_delete_this @cob_date date, @tbl varchar(64)
as exec etmm_data_delete_some @cob_date, @tbl, '='
go

	/*
	 * delete business_data for a cob date
	 * cob_date => date to use
	 */
--	PROCEDURE p_internal_delete_business(cob_date IN cob.cob_date%TYPE) IS
--	BEGIN
--		dbms_output.put_line('Removing business data..');
--		dbms_output.put_line(c_new_line);

--      p_delete_this(cob_date, 'breach');
--      p_delete_audit('breach', 'breach_id');
--      p_delete_this(cob_date, 'feed_statistic');

--		p_delete_cascade('breach_review_action_hist', 'breach', 'breach_id');
--		p_delete_cascade('breach_issuing_reason', 'breach', 'breach_id');
--		p_delete_cascade('breach_breach_type', 'breach', 'breach_id');
--		p_delete_cascade('breach_pfe', 'breach', 'breach_id');
--		p_delete_cascade('breach_trade', 'breach', 'breach_id');
--		p_delete_cascade('margin_trade', 'breach_trade', 'etmm_trade_id');
--		p_delete_cascade('money_market_trade', 'breach_trade', 'etmm_trade_id');
--		p_delete_cascade('cb_trade', 'breach_trade', 'etmm_trade_id');
--		p_delete_cascade('breach_owner', 'breach', 'owner_id');
--		p_delete_cascade('breach_comp_attr_cache', 'breach', 'breach_id');
--		p_delete_cascade('owner_comp_attr_cache', 'breach_owner', 'owner_id');
--		dbms_output.put_line(c_new_line);

--		dbms_output.put_line('Removing audit data..');
--		dbms_output.put_line(c_new_line);
--		p_delete_cascade('entity_audit_detail', 'entity_audit', 'entity_audit_id');
--		dbms_output.put_line(c_new_line);
--	END;

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
	
	/*
	 * Manage constraints
	 *
	 * optn => enable / disable
	 * constrnt => ALL / Constraint Name
	 */
--	PROCEDURE p_manage_constraints(optn IN VARCHAR2, constrnt IN VARCHAR2) IS
--		TYPE user_constraints_table IS TABLE OF user_constraints%ROWTYPE;
--		v_constraints user_constraints_table;
--	BEGIN
--		IF(upper(constrnt)=c_all) THEN
--			SELECT * BULK COLLECT INTO v_constraints FROM user_constraints
--			WHERE constraint_name NOT LIKE '%BIN%' AND constraint_type = 'R';
--		ELSE --Just this constraint
--			SELECT * BULK COLLECT INTO v_constraints FROM user_constraints
--			WHERE constraint_name=constrnt;
--		END IF;

--		FOR i IN v_constraints.FIRST .. v_constraints.LAST
--		LOOP
--			EXECUTE IMMEDIATE 'alter table "' || v_constraints (i).table_name || '" '||optn||'  constraint "' || v_constraints (i).constraint_name || '"';
--		END LOOP;
--	EXCEPTION WHEN OTHERS THEN
--		dbms_output.put_line(sqlerrm);
--		RAISE;
--	END;

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
     print 'Error occured when refreshing etmm_foreign_key table'
  
end
go

execute etmm_foreign_key_refresh 1
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
  if @optn = 'ENABLE' execute etmm_foreign_key_refresh 0 
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

	/*
	 * Manage constraints
	 *
	 * optn => enable / disable
	 * trigger_name => ALL / Trigger Name
	 */
--	PROCEDURE p_manage_triggers(optn IN VARCHAR2, trigger_name IN VARCHAR2) IS
--		TYPE user_triggers_table IS TABLE OF user_triggers%ROWTYPE;
--		v_triggers user_triggers_table;
--	BEGIN
--		IF(upper(trigger_name)=c_all) THEN
--			SELECT * BULK COLLECT INTO v_triggers FROM user_triggers
--			WHERE trigger_name NOT LIKE '%BIN%';
--		ELSE --Just this constraint
--			SELECT * BULK COLLECT INTO v_triggers FROM user_triggers
--			WHERE trigger_name=trigger_name;
--		END IF;

--		FOR i IN v_triggers.FIRST .. v_triggers.LAST
--		LOOP
--			EXECUTE IMMEDIATE 'alter trigger ' || v_triggers (i).trigger_name || ' ' || optn;
--		END LOOP;
--	EXCEPTION WHEN OTHERS THEN
--		dbms_output.put_line(sqlerrm);
--		RAISE;
--	END;

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


--	FUNCTION f_latest_cob RETURN cob.cob_date%TYPE IS
--	   latest_cob_date cob.cob_date%TYPE;
--	BEGIN
--	    SELECT cob_date
--	    	INTO latest_cob_date
--		FROM (
--	    	SELECT cob_date, ROW_NUMBER() OVER (ORDER BY cob_date DESC) cob_order
--	    	FROM cob
--		) cob_ordered
--		WHERE cob_order = 1;

--		RETURN latest_cob_date;
--	EXCEPTION WHEN NO_DATA_FOUND THEN
--		RETURN null;
--	END;

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

	/*
	 * if system option is present or not
	 */
--	FUNCTION f_is_initialized RETURN BOOLEAN IS
--		is_initialized NUMBER := 0;
--	BEGIN
--		SELECT 1
--			INTO is_initialized
--		FROM dual
--		WHERE EXISTS (
--			SELECT * FROM etmm_system_setting WHERE setting_name=c_setting_data_delete
--		);

--		IF (is_initialized = 1) THEN
--			RETURN true;
--		ELSE
--			RETURN false;
--		END IF;
--	EXCEPTION WHEN NO_DATA_FOUND THEN
--		RETURN false;
--	END;

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


	/*
	 * if system options allow initiating delete
	 */
--	FUNCTION f_is_delete_enabled RETURN BOOLEAN IS
--		is_delete_enabled etmm_system_setting.setting_value%TYPE;
--	BEGIN
--		SELECT setting_value
--			INTO is_delete_enabled
--		FROM etmm_system_setting
--		WHERE setting_name = c_setting_data_delete;

--		IF (is_delete_enabled = '1') THEN
--			RETURN true;
--		ELSE
--			RETURN false;
--		END IF;
--	EXCEPTION WHEN NO_DATA_FOUND THEN
--		RETURN false;
--	END;

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

	-- public API

--	PROCEDURE p_restrict(optn IN VARCHAR2) IS
--	BEGIN
--		IF (NOT f_is_initialized()) THEN
--			INSERT INTO etmm_system_setting(setting_name, setting_value)
--			VALUES(c_setting_data_delete, '0');
--		END IF;

--		CASE UPPER(optn)
--			WHEN c_enable THEN
--				UPDATE etmm_system_setting SET setting_value='1' WHERE setting_name=c_setting_data_delete;
--			ELSE
--				UPDATE etmm_system_setting SET setting_value='0' WHERE setting_name=c_setting_data_delete;
--		END CASE;

--		COMMIT;
--	EXCEPTION
--		WHEN OTHERS THEN
--			dbms_output.put_line(sqlerrm);
--	END;

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


--	PROCEDURE p_delete_for_cob_date(cob_date IN cob.cob_date%TYPE) IS
--	BEGIN
--		IF (f_is_delete_enabled()) THEN
--			dbms_output.put_line('ETMM Data Delete');
--			dbms_output.put_line('This routine will remove business and history data for date:'||to_char(cob_date)||' existing in the environment: ' || USER);
--			dbms_output.put_line(c_new_line);

--			p_manage_constraints(c_disable, c_all);
--			p_manage_triggers(c_disable, c_all);

--            dbms_output.put_line('Processing '||to_char(cob_date));
--            dbms_output.put_line(c_new_line);

--            dbms_output.put_line('Removing history data..');
--            dbms_output.put_line(c_new_line);

--            p_delete_this(cob_date, 'limit_detail_hist');
--            p_delete_this(cob_date, 'exposure_detail_hist');
--            p_delete_this(cob_date, 'money_market_trade_hist');
--            p_delete_this(cob_date, 'margin_trade_hist');
--            p_delete_this(cob_date, 'cb_trade_hist');
--            p_delete_this(cob_date, 'portfolio_detail_hist');
--            p_delete_this(cob_date, 'owner_hist');

--            dbms_output.put_line(c_new_line);

--            p_internal_delete_business(cob_date);

--            dbms_output.put_line('Removing COB');
--            p_delete_this(cob_date, 'cob');
--            dbms_output.put_line(c_new_line);

--            p_manage_triggers(c_enable, c_all);
--			p_manage_constraints(c_enable, c_all);
--			COMMIT;
--		END IF;

--		p_restrict(c_disable);
--	EXCEPTION
--		WHEN OTHERS THEN
--			p_restrict(c_disable);
--			dbms_output.put_line(sqlerrm);
--	END;

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

    --1245 seconds as tested on Oracle using OCT 22 2012 cob_date.
--	PROCEDURE p_delete_business_for_cob_date(cob_date IN cob.cob_date%TYPE) IS
--	BEGIN
--		IF (f_is_delete_enabled()) THEN
--			dbms_output.put_line('ETMM Data Delete');
--			dbms_output.put_line('This routine will remove business data for date:'||to_char(cob_date)||' existing in the environment: ' || USER);
--			dbms_output.put_line(c_new_line);

--			p_manage_constraints(c_disable, c_all);
--			p_manage_triggers(c_disable, c_all);

--            dbms_output.put_line('Processing '||to_char(cob_date));
--            dbms_output.put_line(c_new_line);

--        	p_internal_delete_business(cob_date);

--			p_manage_triggers(c_enable, c_all);
--			p_manage_constraints(c_enable, c_all);

--			COMMIT;
--		END IF;

--		p_restrict(c_disable);
--	EXCEPTION
--		WHEN OTHERS THEN
--			p_restrict(c_disable);
--			dbms_output.put_line(sqlerrm);
--	END;

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
          set @msg = 'This routine will remove business data for date: ' || convert(varchar(32), @cob_date) || ' existing in the environment: ' || user_name()  
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

--END PKG_ETMM_DATA_DELETE;