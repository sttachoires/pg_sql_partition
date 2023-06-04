--
-- Some utilities to help partition handling
--

DROP SCHEMA IF EXISTS admin CASCADE;
CREATE SCHEMA IF NOT EXISTS admin;

SET search_path='report';

CREATE FUNCTION admin.check_them_all()
RETURNS TABLE (
	"schema"	TEXT,
	"function"	TEXT,
	"report"	TEXT)
LANGUAGE SQL
AS
$$
SELECT n.nspname, p.proname, plpgsql_check_function(p.oid)
   FROM pg_catalog.pg_namespace n
   JOIN pg_catalog.pg_proc p ON pronamespace = n.oid
   JOIN pg_catalog.pg_language l ON p.prolang = l.oid
  WHERE l.lanname = 'plpgsql' AND p.prorettype <> 2279
$$;

CREATE FUNCTION admin.array_sort(a ANYARRAY)
RETURNS ANYARRAY
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (SELECT ARRAY(SELECT unnest(a) ORDER BY 1));
END
$$;

CREATE TYPE admin.qualified_name AS (
	nsname		TEXT,
	relname		TEXT
);

CREATE FUNCTION admin.make_qualname(ns TEXT, rel TEXT)
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
	qualname    admin.qualified_name;
	
BEGIN
	IF ((rel <> '') IS NOT TRUE)
	THEN
		RETURN NULL;
	END IF;	

	qualname.nsname=ns;
	qualname.relname=rel;

	RETURN qualname;
END
$$;

CREATE FUNCTION admin.qualname_to_string(qualname admin.qualified_name)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
BEGIN
	RETURN format('%I.%I',(qualname).nsname,(qualname).relname);
END
$$;

CREATE FUNCTION admin.string_to_qualname(tabqname TEXT)
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
	tabaname	TEXT[];
    qualname    admin.qualified_name;
	pgversion	BIGINT;

BEGIN
	RAISE DEBUG 'admin.string_to_qualname tabqname ''%''',tabqname;
	IF ((tabqname <> '') IS NOT TRUE)
	THEN
		RETURN NULL;
	END IF;

	tabaname=pg_catalog.parse_ident(tabqname);	
	RAISE DEBUG 'admin.string_to_qualname tabaname ''%''',tabaname;

	IF (pg_catalog.array_length(tabaname,1) = 2)
	THEN
		qualname=admin.make_qualname(tabaname[1],tabaname[2]);
		RAISE DEBUG 'admin.string_to_qualname qualname ''%''',qualname;

	ELSIF (pg_catalog.array_length(tabaname,1) = 1)
	THEN
		SELECT setting::BIGINT INTO pgversion FROM pg_settings WHERE name = 'server_version_num';
		RAISE DEBUG 'admin.string_to_qualname pgversion ''%''',pgversion;

		IF (pgversion >= 150000)
		THEN
			qualname=admin.make_qualname(current_role,tabaname[1]);
		ELSE
			qualname=admin.make_qualname('public',tabaname[1]);
		END IF;
		RAISE DEBUG 'admin.string_to_qualname qualname ''%''',qualname;
	ELSE
		RAISE EXCEPTION 'hill formed relation name "%"',tabqname;
	END IF;

	RAISE DEBUG 'admin.string_to_qualname qualname ''%''',qualname;
	RETURN qualname;
END
$$;

CREATE FUNCTION admin.generate_table_name(nsname TEXT, prefix TEXT='t_')
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
	tabname		TEXT;
	rndm		TEXT;
	name		admin.qualified_name;

BEGIN
    RAISE DEBUG 'admin.generate_table_name prefix %',prefix;

	rndm=(SELECT md5(random()::text));
	tabname=concat(prefix,pg_catalog.substr(rndm,1,3));
	name=admin.make_qualname(nsname,tabname);
	
	WHILE admin.table_exists(name)
	LOOP
		rndm=(SELECT md5(random()::text));
		tabname=concat(prefix,pg_catalog.substr(rndm,1,4));
		name=admin.make_qualname(nsname,tabname);
	END LOOP;
	
	RETURN name;
END
$$;

CREATE FUNCTION admin.generate_partition_name(tabname admin.qualified_name, prefix TEXT='_')
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
    tbname		TEXT;
	iter		INTEGER=1;
    name        admin.qualified_name;

BEGIN
    RAISE DEBUG 'admin.generate_partition_name tabname % prefix %',tabname,prefix;

    tbname=concat(tabname.relname,prefix,iter);
    name=admin.make_qualname(tabname.nsname,tbname);

    WHILE admin.table_exists(name)
    LOOP
		iter=iter+1;
		tbname=concat(tabname.relname,prefix,iter);
        name=admin.make_qualname(tabname.nsname,tbname);
    END LOOP;
    
    RETURN name;
END
$$;

CREATE FUNCTION admin.generate_default_partition_name(tabname admin.qualified_name, prefix TEXT='_d')
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
    tbname		TEXT;
    iter        INTEGER=1;
    name        admin.qualified_name;

BEGIN
    RAISE DEBUG 'admin.generate_default_partition_name tabname % prefix %',tabname,prefix;

    tbname=concat(tabname.relname,prefix);
    name=admin.make_qualname(tabname.nsname,tbname);

    WHILE admin.table_exists(name)
    LOOP
        iter=iter+1;
        tbname=concat(tabname.relname,prefix,iter);
        name=admin.make_qualname(tabname.nsname,tbname);
    END LOOP;

    RETURN name;
END
$$;


CREATE CAST (TEXT AS admin.qualified_name)
	WITH FUNCTION admin.string_to_qualname(text);

CREATE CAST (admin.qualified_name AS TEXT)
	WITH FUNCTION admin.qualname_to_string(admin.qualified_name);

CREATE FUNCTION admin.table_exists(tabqname admin.qualified_name)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS
$$
DECLARE
	tblexists	BOOLEAN;
BEGIN
	RAISE DEBUG 'SELECT TRUE FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace WHERE ns.nspname = ''%'' AND c.relname = ''%''',(tabqname).nsname,(tabqname).relname;
	
	EXECUTE format('SELECT TRUE FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace WHERE ns.nspname = ''%s'' AND c.relname = ''%s''',(tabqname).nsname,(tabqname).relname) INTO tblexists;

	RAISE DEBUG 'admin.table_exists tblexists ''%''',tblexists;
	IF (tblexists IS NULL)
	THEN
		tblexists=false;
	END IF;
	RAISE DEBUG 'admin.table_exists tblexists ''%''',tblexists;
	RETURN tblexists;
END
$$;

CREATE FUNCTION admin.column_exists(tabqname admin.qualified_name, col TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS
$$
DECLARE
	colexists	BOOLEAN=false;
BEGIN
	IF (admin.table_exists(tabqname))
	THEN
	    EXECUTE format('SELECT TRUE FROM pg_catalog.pg_attribute a WHERE a.attrelid = ''%I.%I''::regclass AND a.attname = ''%s'' AND NOT a.attisdropped',
			(tabqname).nsname,(tabqname).relname,col) INTO colexists;
	ELSE
		RAISE EXCEPTION 'relation % unknown',tabqname;
	END IF;
	IF (colexists IS NULL)
	THEN
		colexists=false;
	END IF;
    RETURN colexists;
END
$$;

CREATE FUNCTION admin.get_column_info(tabqname admin.qualified_name, VARIADIC names TEXT[]=NULL)
RETURNS TABLE (colname TEXT, coltype TEXT)
LANGUAGE plpgsql
AS
$$
BEGIN
	RAISE DEBUG 'admin.get_column_info tabqname %',tabqname;
	RAISE DEBUG 'admin.get_column_info names %',names;

	IF (names IS NULL)
	THEN
		RETURN QUERY SELECT a.attname::TEXT AS "colname",
						pg_catalog.format_type(a.atttypid, a.atttypmod) AS "coltype"
	    FROM
	        pg_catalog.pg_attribute a
	    WHERE
	        a.attnum > 0
	        AND NOT a.attisdropped
	        AND a.attrelid = (
	            SELECT c.oid
	            FROM pg_catalog.pg_class c
	                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	            WHERE n.nspname = tabqname.nsname
				  AND c.relname = tabqname.relname
				);
	ELSE
		RETURN QUERY SELECT a.attname::TEXT AS "colname",
		                pg_catalog.format_type(a.atttypid, a.atttypmod) AS "coltype"
        FROM
            pg_catalog.pg_attribute a
        WHERE
            a.attnum > 0
            AND NOT a.attisdropped
            AND a.attrelid = (
                SELECT c.oid
                FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = tabqname.nsname
                  AND c.relname = tabqname.relname)
			AND a.attname = ANY (names);
	END IF;
END
$$;

CREATE FUNCTION admin.rowcount(tabqname admin.qualified_name, ponly BOOLEAN DEFAULT false)
RETURNS bigint
LANGUAGE plpgsql
AS
$$
DECLARE
	lc bigint;

BEGIN
    
	IF ( ponly )
    THEN
	    EXECUTE format('SELECT count(*) FROM ONLY %s',admin.qualname_to_string(tabqname)) INTO lc;
	ELSE
		EXECUTE format('SELECT count(*) FROM %s',admin.qualname_to_string(tabqname)) INTO lc;
	END IF;

	RETURN lc;
END
$$;

CREATE FUNCTION admin.swap_tables(tabname admin.qualified_name, newname admin.qualified_name)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	tabid		pg_catalog.oid;
	newid		pg_catalog.oid;
	tmpname		admin.qualified_name;
	tbreffk		TEXT[];
	tbrefcstr	TEXT[][];

BEGIN
	RAISE DEBUG 'admin.swap_tables tabname %',tabname;
    RAISE DEBUG 'admin.swap_tables newname %',newname;

	SELECT t.oid INTO tabid
	FROM pg_catalog.pg_class t
    JOIN pg_catalog.pg_namespace ns ON t.relnamespace = ns.oid
	WHERE t.relname = tabname.relname
	  AND ns.nspname = tabname.nsname;
    RAISE DEBUG 'admin.swap_tables tabid %',tabid;

	SELECT t.oid INTO newid
    FROM pg_catalog.pg_class t
    JOIN pg_catalog.pg_namespace ns ON t.relnamespace = ns.oid
    WHERE t.relname = newname.relname
      AND ns.nspname = newname.nsname;
    RAISE DEBUG 'admin.swap_tables newid %',newid;

	tmpname=admin.generate_table_name(tabname.nsname);
    RAISE DEBUG 'admin.swap_tables tmpname %',tmpname;

	SELECT	pg_catalog.array_agg(ARRAY[ns.nspname,tf.relname,
			cn.conname, 
			pg_catalog.pg_get_constraintdef(cn.oid)]) INTO tbrefcstr
	FROM	pg_catalog.pg_constraint	cn
	JOIN	pg_catalog.pg_class			tf
		ON 	tf.oid = cn.conrelid
	JOIN 	pg_catalog.pg_class			tb
		ON	tb.oid = cn.confrelid
	JOIN	pg_catalog.pg_namespace		ns
		ON	ns.oid = tb.relnamespace
	WHERE	cn.contype = 'f'
	  AND	ns.nspname = tabname.nsname
	  AND	tb.relname = tabname.relname;

	RAISE DEBUG 'admin.swap_tables tbrefcstr %',tbrefcstr;

	-- drop all fk
	IF (tbrefcstr IS NOT NULL)
	THEN
		FOREACH tbreffk SLICE 1 IN ARRAY tbrefcstr
		LOOP
			RAISE DEBUG 'admin.swap_tables tbreffk  %',tbreffk;
			RAISE DEBUG 'admin.swap_tables ALTER TABLE %.% DROP CONSTRAINT %',tbreffk[1],tbreffk[2],tbreffk[3];
			EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',tbreffk[1],tbreffk[2],tbreffk[3]);
		END LOOP;
	END IF;

	-- swap table names
	RAISE DEBUG 'admin.swap_tables ALTER TABLE %.% RENAME TO %',tabname.nsname,tabname.relname,tmpname.relname;
	EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',tabname.nsname,tabname.relname,tmpname.relname);
	RAISE DEBUG 'admin.swap_tables ALTER TABLE %.% RENAME TO %',newname.nsname,newname.relname,tabname.relname;
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',newname.nsname,newname.relname,tabname.relname);
    RAISE DEBUG 'admin.swap_tables ALTER TABLE %.% RENAME TO %',tmpname.nsname,tmpname.relname,newname.relname;
	EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',tmpname.nsname,tmpname.relname,newname.relname);

	-- recreate fks
	IF (tbrefcstr IS NOT NULL)
    THEN
	    FOREACH tbreffk SLICE 1 IN ARRAY tbrefcstr
	    LOOP
	        RAISE DEBUG 'admin.swap_tables tbreffk  %',tbreffk;
	        RAISE DEBUG 'admin.swap_tables ALTER TABLE %.% ADD CONSTRAINT % %',tbreffk[1],tbreffk[2],tbreffk[3],tbreffk[4];
			EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',tbreffk[1],tbreffk[2],tbreffk[3],tbreffk[4]);
	    END LOOP;
	END IF;
END
$$;


CREATE FUNCTION admin.rename_table(tabname admin.qualified_name, newname admin.qualified_name)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
	RAISE DEBUG 'ALTER TABLE %.% RENAME TO %',tabname.nsname,tabname.relname,newname.relname;
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',tabname.nsname,tabname.relname,newname.relname);
END
$$;

CREATE FUNCTION admin.copy_rows(srcname admin.qualified_name, dstname admin.qualified_name, qualifier TEXT=NULL)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
	IF ((qualifier <> '') IS NOT TRUE)
	THEN
		RAISE DEBUG 'INSERT INTO %.% (SELECT * FROM %.%)',dstname.nsname,dstname.relname,srcname.nsname,srcname.relname;
    	EXECUTE format('INSERT INTO %I.%I (SELECT * FROM %I.%I)',dstname.nsname,dstname.relname,srcname.nsname,srcname.relname);
	ELSE
		RAISE DEBUG 'INSERT INTO %.% (SELECT * FROM %.% WHERE %)',dstname.nsname,dstname.relname,srcname.nsname,srcname.relname,qualifier;
		EXECUTE format('INSERT INTO %I.%I (SELECT * FROM %I.%I WHERE %s)',dstname.nsname,dstname.relname,srcname.nsname,srcname.relname,qualifier);
	END IF;
END
$$;

CREATE FUNCTION admin.move_rows(srcname admin.qualified_name, dstname admin.qualified_name, qualifier TEXT=NULL)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
	IF ((qualifier <> '') IS NOT TRUE)
    THEN
		EXECUTE format('WITH rows AS (DELETE FROM %I.%I RETURNING *) INSERT INTO %I.%I SELECT * FROM rows',(srcname).nsname,(srcname).relname,(dstname).nsname,(dstname).relname);
    ELSE
		EXECUTE format('WITH rows AS (DELETE FROM %I.%I WHERE %s RETURNING *) INSERT INTO %I.%I SELECT * FROM rows',(srcname).nsname,(srcname).relname,qualifier,(dstname).nsname,(dstname).relname);
    END IF;
END
$$;

CREATE FUNCTION admin.get_partition_parent(partqname admin.qualified_name)
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
    tabqname    admin.qualified_name;
BEGIN
    RAISE DEBUG 'admin.get_partition_parent partqname %',partqname;

    SELECT ns.nspname,p.relname INTO tabqname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace
        JOIN pg_catalog.pg_inherits inh ON inh.inhrelid = c.oid
        JOIN pg_catalog.pg_class p ON p.oid = inh.inhparent
        WHERE p.relkind = 'p'
          AND ns.nspname = (partqname).nsname
          AND c.relname = (partqname).relname;

    RAISE DEBUG 'admin.get_partition_parent tabqname %',tabqname;

    RETURN tabqname;
END
$$;

CREATE FUNCTION admin.drop_table_default_values(tabqname admin.qualified_name)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    rec         RECORD;
    seqname     TEXT;

BEGIN
    FOR rec IN (
        SELECT a.attname    AS "colname"
        FROM pg_catalog.pg_attribute    a
        JOIN pg_catalog.pg_class        t
            ON a.attrelid = t.oid
        JOIN pg_catalog.pg_namespace    ns
            ON ns.oid = t.relnamespace
        WHERE a.atthasdef
          AND ns.nspname = (tabqname).nsname
          AND t.relname  = (tabqname).relname
        )
    LOOP
RAISE DEBUG 'admin.drop_table_default_values pg_get_serial_sequence(%,%)',tabqname,rec.colname;
        SELECT pg_catalog.pg_get_serial_sequence(tabqname::TEXT,rec.colname) INTO seqname;
RAISE DEBUG 'admin.drop_table_default_values seqname %',seqname;
RAISE DEBUG 'admin.drop_table_default_values ALTER TABLE %.% ALTER COLUMN % DROP DEFAULT',(tabqname).nsname,(tabqname).relname,rec.colname;
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN %I DROP DEFAULT',(tabqname).nsname,(tabqname).relname,rec.colname);
        IF (seqname IS NOT NULL)
        THEN
RAISE DEBUG 'admin.drop_table_default_values ALTER SEQUENCE % OWNED BY NONE',seqname;
            EXECUTE format('ALTER SEQUENCE %s OWNED BY NONE',seqname);
        END IF;
    END LOOP;
END
$$;

CREATE FUNCTION admin.drop_table_constraints(tabqname admin.qualified_name)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    rec     RECORD;

BEGIN
RAISE DEBUG 'admin.drop_table_constraints %.%',(tabqname).nsname,(tabqname).relname;
    FOR rec IN (
        SELECT  rel.relname AS "tabname",
                con.conname AS "constname"
        FROM        pg_catalog.pg_constraint    con
        INNER JOIN  pg_catalog.pg_class         rel
            ON rel.oid = con.conrelid
        INNER JOIN pg_catalog.pg_namespace      nsp
            ON nsp.oid = connamespace
        WHERE nsp.nspname = (tabqname).nsname
          AND rel.relname = (tabqname).relname
        )
    LOOP
RAISE DEBUG 'admin.drop_table_constraints ALTER TABLE % DROP CONSTRAINT %',rec.tabname,rec.constname;
        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',(tabqname).nsname,(tabqname).relname,rec.constname );
    END LOOP;
END
$$;

CREATE FUNCTION admin.drop_table(tabqname admin.qualified_name)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER ALL',(tabqname).nsname,(tabqname).relname);
    PERFORM admin.drop_table_constraints(tabqname);
    PERFORM admin.drop_table_default_values(tabqname);

    EXECUTE format('DROP TABLE %I.%I CASCADE',(tabqname).nsname,(tabqname).relname);
END
$$;

--DROP VIEW IF EXISTS admin.partitions;
CREATE VIEW admin.partitions AS (
    SELECT * FROM (
    WITH RECURSIVE ancestor(name) AS (SELECT c.relname AS "name" FROM pg_catalog.pg_class c WHERE c.relkind = 'p') 
    SELECT	a.relname                                   AS "parent",
			'PARENT'                                    AS "type",
		   	ancest.name                                 AS "name",
            admin.rowcount(
				admin.make_qualname(n.nspname,
									p.relname))    		AS "rows",
            pg_get_partkeydef(p.oid)                    AS "range",
			pg_size_pretty(pg_relation_size(p.oid))     AS "size"
		FROM ancestor ancest
		JOIN pg_catalog.pg_class p ON p.relname = ancest.name
        LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = p.oid
        LEFT JOIN pg_catalog.pg_class a ON a.oid = i.inhparent
		JOIN pg_catalog.pg_namespace n ON n.oid = p.relnamespace
        WHERE p.relkind IN ('p', 'r')
    UNION ALL
	SELECT 	parent.relname                              AS "parent",
			'CHILD'                                     AS "type",
			child.relname                               AS "name",
			admin.rowcount(
                admin.make_qualname(namespace.nspname,
									child.relname)) 	AS "rows",
			pg_get_expr(child.relpartbound, child.oid)  AS "range",
			pg_size_pretty(pg_relation_size(child.oid)) AS "size"
		FROM ancestor ancest
		JOIN pg_catalog.pg_class parent ON parent.relname = ancest.name
		JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parent.oid
        JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = child.relnamespace
        WHERE parent.relkind IN ('p', 'r')
    UNION ALL
    SELECT  sp.relname                                  AS "parent",
            ''                                          AS "type",
            c.relname                                   AS "name",
			admin.rowcount(
                admin.make_qualname(nsp.nspname,
									c.relname))    		AS "rows",
            'SUSPECTED ORPHAN'                          AS "range",
            pg_size_pretty(pg_relation_size(c.oid))     AS "size"
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_class sp ON c.relname LIKE sp.relname||'%'
		JOIN pg_catalog.pg_namespace nsp ON nsp.oid = c.relnamespace
        WHERE c.relkind IN ('r','p')
          AND sp.relname = 'ptable'
          AND c.oid NOT IN (SELECT inhrelid FROM pg_inherits)
          AND c.oid NOT IN (SELECT inhparent FROM pg_inherits)
	) AS dp
	ORDER BY "parent" NULLS FIRST, "type" DESC, "name"
);

CREATE VIEW admin.relations AS (
	SELECT 	n.nspname AS "schema",
			c.relname AS "name",
	  		CASE c.relkind
				WHEN 'r' THEN 'table'
				WHEN 'v' THEN 'view'
				WHEN 'm' THEN 'materialized view'
				WHEN 'i' THEN 'index'
				WHEN 'S' THEN 'sequence'
				WHEN 't' THEN 'TOAST table'
				WHEN 'f' THEN 'foreign table'
				WHEN 'p' THEN 'partitioned table'
				WHEN 'I' THEN 'partitioned index'
			END as "type",
			pg_catalog.pg_get_userbyid(c.relowner) as "owner"
	FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
	WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
	ORDER BY 1,2
);

