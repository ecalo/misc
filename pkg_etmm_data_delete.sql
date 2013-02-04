create or replace 
PACKAGE "PKG_ETMM_DATA_DELETE" AS
	c_all CONSTANT VARCHAR2(10) := 'ALL';
	c_disable CONSTANT VARCHAR2(10) := 'DISABLE';
	c_enable CONSTANT VARCHAR2(10) := 'ENABLE';

	/*
	 * restrict execution of delete procedures
	 * optn => c_enable / c_disable
	 */
	PROCEDURE p_restrict(optn IN VARCHAR2);

	/*
	 * delete everything from specified COB
	 * cob_date => date to use
	 */
	PROCEDURE p_delete_for_cob_date(cob_date IN cob.cob_date%TYPE);

	/*
	 * delete business data from specified COB
	 * cob_date => date to use
	 */
	PROCEDURE p_delete_business_for_cob_date(cob_date IN cob.cob_date%TYPE);
END PKG_ETMM_DATA_DELETE;

create or replace 
PACKAGE BODY "PKG_ETMM_DATA_DELETE" AS
	-- private constants

	c_setting_data_delete CONSTANT etmm_system_setting.setting_name%TYPE := 'RTB_MANUAL_DATA_DELETE';
	c_new_line CONSTANT VARCHAR2(2) := CHR(13) || CHR(10);

	-- private procedures

	/*
	 * delete data
	 *
	 * tbl => table name
	 */
	PROCEDURE p_delete_all(tbl IN VARCHAR2) IS
	BEGIN
		dbms_output.put_line(tbl);
		EXECUTE IMMEDIATE ('DELETE FROM '||tbl);
	EXCEPTION WHEN OTHERS THEN
		dbms_output.put_line(sqlerrm);
		RAISE;
	END;

	/*
	 * delete data for cob
	 *
	 * cob_date => cob date to use
	 * tbl => table name
	 * ops => comparison operator for cob date
	 */
	PROCEDURE p_delete_some(cob_date IN cob.cob_date%TYPE, tbl IN VARCHAR2, ops IN VARCHAR2) IS
	BEGIN
		dbms_output.put_line(tbl);
		EXECUTE IMMEDIATE ('DELETE FROM '||tbl||' WHERE cob_date '||ops||' to_date('''||cob_date||''', ''dd-mon-yy'')');
	EXCEPTION WHEN OTHERS THEN
		dbms_output.put_line(sqlerrm);
		RAISE;
	END;

	/*
	 * cascades delete
	 * tbl => table to cascade delete for
	 * parent_table => cascade owner table
	 * fk_name => cascade column (FK)
	 */
	PROCEDURE p_delete_cascade(tbl IN VARCHAR2, parent_table IN VARCHAR2, fk_name IN VARCHAR2) IS
	BEGIN
		dbms_output.put_line(tbl);
		EXECUTE IMMEDIATE ('DELETE FROM '||tbl||' c WHERE NOT EXISTS ('||
			'select 1 from '||parent_table||' p where c.'||fk_name||' = p.'||fk_name||')');
	END;

	/*
	 * cascade delete ETMM audit records
	 * tbl => table name to cascade for
	 * fk_name => cascade column (FK)
	 */
	PROCEDURE p_delete_audit(tbl IN VARCHAR2, fk_name IN VARCHAR2) IS
	BEGIN
		dbms_output.put_line('entity_audit');
		EXECUTE IMMEDIATE ('DELETE FROM entity_audit c WHERE entity_name = UPPER('''||tbl||''') AND NOT EXISTS ('||
			'select 1 from '||tbl||' p where c.entity_id = p.'||fk_name||')');
	END;

	/*
	 * delete all data except for cob
	 *
	 * cob_date => cob date to use
	 * tbl => table name
	 */
	PROCEDURE p_delete_other(cob_date IN cob.cob_date%TYPE, tbl IN VARCHAR2) IS
	BEGIN
		p_delete_some(cob_date, tbl, '<>');
	END;

	/*
	 * delete data for cob
	 *
	 * cob_date => cob date to use
	 * tbl => table name
	 */
	PROCEDURE p_delete_this(cob_date IN cob.cob_date%TYPE, tbl IN VARCHAR2) IS
	BEGIN
		p_delete_some(cob_date, tbl, '=');
	END;

	/*
	 * delete business_data for a cob date
	 * cob_date => date to use
	 */
	PROCEDURE p_internal_delete_business(cob_date IN cob.cob_date%TYPE) IS
	BEGIN
		dbms_output.put_line('Removing business data..');
		dbms_output.put_line(c_new_line);

        p_delete_this(cob_date, 'breach');
        p_delete_audit('breach', 'breach_id');
        p_delete_this(cob_date, 'feed_statistic');

		p_delete_cascade('breach_review_action_hist', 'breach', 'breach_id');
		p_delete_cascade('breach_issuing_reason', 'breach', 'breach_id');
		p_delete_cascade('breach_breach_type', 'breach', 'breach_id');
		p_delete_cascade('breach_pfe', 'breach', 'breach_id');
		p_delete_cascade('breach_trade', 'breach', 'breach_id');
		p_delete_cascade('margin_trade', 'breach_trade', 'etmm_trade_id');
		p_delete_cascade('money_market_trade', 'breach_trade', 'etmm_trade_id');
		p_delete_cascade('cb_trade', 'breach_trade', 'etmm_trade_id');
		p_delete_cascade('breach_owner', 'breach', 'owner_id');
		p_delete_cascade('breach_comp_attr_cache', 'breach', 'breach_id');
		p_delete_cascade('owner_comp_attr_cache', 'breach_owner', 'owner_id');
		dbms_output.put_line(c_new_line);

		dbms_output.put_line('Removing audit data..');
		dbms_output.put_line(c_new_line);
		p_delete_cascade('entity_audit_detail', 'entity_audit', 'entity_audit_id');
		dbms_output.put_line(c_new_line);
	END;

	/*
	 * Manage constraints
	 *
	 * optn => enable / disable
	 * constrnt => ALL / Constraint Name
	 */
	PROCEDURE p_manage_constraints(optn IN VARCHAR2, constrnt IN VARCHAR2) IS
		TYPE user_constraints_table IS TABLE OF user_constraints%ROWTYPE;
		v_constraints user_constraints_table;
	BEGIN
		IF(upper(constrnt)=c_all) THEN
			SELECT * BULK COLLECT INTO v_constraints FROM user_constraints
			WHERE constraint_name NOT LIKE '%BIN%' AND constraint_type = 'R';
		ELSE --Just this constraint
			SELECT * BULK COLLECT INTO v_constraints FROM user_constraints
			WHERE constraint_name=constrnt;
		END IF;

		FOR i IN v_constraints.FIRST .. v_constraints.LAST
		LOOP
			EXECUTE IMMEDIATE 'alter table "' || v_constraints (i).table_name || '" '||optn||'  constraint "' || v_constraints (i).constraint_name || '"';
		END LOOP;
	EXCEPTION WHEN OTHERS THEN
		dbms_output.put_line(sqlerrm);
		RAISE;
	END;

	/*
	 * Manage constraints
	 *
	 * optn => enable / disable
	 * trigger_name => ALL / Trigger Name
	 */
	PROCEDURE p_manage_triggers(optn IN VARCHAR2, trigger_name IN VARCHAR2) IS
		TYPE user_triggers_table IS TABLE OF user_triggers%ROWTYPE;
		v_triggers user_triggers_table;
	BEGIN
		IF(upper(trigger_name)=c_all) THEN
			SELECT * BULK COLLECT INTO v_triggers FROM user_triggers
			WHERE trigger_name NOT LIKE '%BIN%';
		ELSE --Just this constraint
			SELECT * BULK COLLECT INTO v_triggers FROM user_triggers
			WHERE trigger_name=trigger_name;
		END IF;

		FOR i IN v_triggers.FIRST .. v_triggers.LAST
		LOOP
			EXECUTE IMMEDIATE 'alter trigger ' || v_triggers (i).trigger_name || ' ' || optn;
		END LOOP;
	EXCEPTION WHEN OTHERS THEN
		dbms_output.put_line(sqlerrm);
		RAISE;
	END;


	FUNCTION f_latest_cob RETURN cob.cob_date%TYPE IS
	   latest_cob_date cob.cob_date%TYPE;
	BEGIN
	    SELECT cob_date
	    	INTO latest_cob_date
		FROM (
	    	SELECT cob_date, ROW_NUMBER() OVER (ORDER BY cob_date DESC) cob_order
	    	FROM cob
		) cob_ordered
		WHERE cob_order = 1;

		RETURN latest_cob_date;
	EXCEPTION WHEN NO_DATA_FOUND THEN
		RETURN null;
	END;

	/*
	 * if system option is present or not
	 */
	FUNCTION f_is_initialized RETURN BOOLEAN IS
		is_initialized NUMBER := 0;
	BEGIN
		SELECT 1
			INTO is_initialized
		FROM dual
		WHERE EXISTS (
			SELECT * FROM etmm_system_setting WHERE setting_name=c_setting_data_delete
		);

		IF (is_initialized = 1) THEN
			RETURN true;
		ELSE
			RETURN false;
		END IF;
	EXCEPTION WHEN NO_DATA_FOUND THEN
		RETURN false;
	END;

	/*
	 * if system options allow initiating delete
	 */
	FUNCTION f_is_delete_enabled RETURN BOOLEAN IS
		is_delete_enabled etmm_system_setting.setting_value%TYPE;
	BEGIN
		SELECT setting_value
			INTO is_delete_enabled
		FROM etmm_system_setting
		WHERE setting_name = c_setting_data_delete;

		IF (is_delete_enabled = '1') THEN
			RETURN true;
		ELSE
			RETURN false;
		END IF;
	EXCEPTION WHEN NO_DATA_FOUND THEN
		RETURN false;
	END;

	-- public API

	PROCEDURE p_restrict(optn IN VARCHAR2) IS
	BEGIN
		IF (NOT f_is_initialized()) THEN
			INSERT INTO etmm_system_setting(setting_name, setting_value)
			VALUES(c_setting_data_delete, '0');
		END IF;

		CASE UPPER(optn)
			WHEN c_enable THEN
				UPDATE etmm_system_setting SET setting_value='1' WHERE setting_name=c_setting_data_delete;
			ELSE
				UPDATE etmm_system_setting SET setting_value='0' WHERE setting_name=c_setting_data_delete;
		END CASE;

		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			dbms_output.put_line(sqlerrm);
	END;

	PROCEDURE p_delete_for_cob_date(cob_date IN cob.cob_date%TYPE) IS
	BEGIN
		IF (f_is_delete_enabled()) THEN
			dbms_output.put_line('ETMM Data Delete');
			dbms_output.put_line('This routine will remove business and history data for date:'||to_char(cob_date)||' existing in the environment: ' || USER);
			dbms_output.put_line(c_new_line);

			p_manage_constraints(c_disable, c_all);
			p_manage_triggers(c_disable, c_all);

            dbms_output.put_line('Processing '||to_char(cob_date));
            dbms_output.put_line(c_new_line);

            dbms_output.put_line('Removing history data..');
            dbms_output.put_line(c_new_line);

            p_delete_this(cob_date, 'limit_detail_hist');
            p_delete_this(cob_date, 'exposure_detail_hist');
            p_delete_this(cob_date, 'money_market_trade_hist');
            p_delete_this(cob_date, 'margin_trade_hist');
            p_delete_this(cob_date, 'cb_trade_hist');
            p_delete_this(cob_date, 'portfolio_detail_hist');
            p_delete_this(cob_date, 'owner_hist');

            dbms_output.put_line(c_new_line);

            p_internal_delete_business(cob_date);

            dbms_output.put_line('Removing COB');
            p_delete_this(cob_date, 'cob');
            dbms_output.put_line(c_new_line);

            p_manage_triggers(c_enable, c_all);
			p_manage_constraints(c_enable, c_all);
			COMMIT;
		END IF;

		p_restrict(c_disable);
	EXCEPTION
		WHEN OTHERS THEN
			p_restrict(c_disable);
			dbms_output.put_line(sqlerrm);
	END;
    --1245 seconds as tested on Oracle using OCT 22 2012 cob_date.
	PROCEDURE p_delete_business_for_cob_date(cob_date IN cob.cob_date%TYPE) IS
	BEGIN
		IF (f_is_delete_enabled()) THEN
			dbms_output.put_line('ETMM Data Delete');
			dbms_output.put_line('This routine will remove business data for date:'||to_char(cob_date)||' existing in the environment: ' || USER);
			dbms_output.put_line(c_new_line);

			p_manage_constraints(c_disable, c_all);
			p_manage_triggers(c_disable, c_all);

            dbms_output.put_line('Processing '||to_char(cob_date));
            dbms_output.put_line(c_new_line);

        	p_internal_delete_business(cob_date);

			p_manage_triggers(c_enable, c_all);
			p_manage_constraints(c_enable, c_all);

			COMMIT;
		END IF;

		p_restrict(c_disable);
	EXCEPTION
		WHEN OTHERS THEN
			p_restrict(c_disable);
			dbms_output.put_line(sqlerrm);
	END;

END PKG_ETMM_DATA_DELETE;