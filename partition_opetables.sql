--
-- Table operations
--

\i partition_strings.sql

CREATE FUNCTION admin.get_table_constraint_defs(tabname admin.qualified_name)
RETURNS TEXT[]
LANGUAGE plpgsql
AS
$$
DECLARE
	constraints	TEXT[];

BEGIN

	SELECT pg_catalog.array_agg(c.conname||' '||pg_catalog.pg_get_constraintdef(c.oid)) INTO constraints
       FROM pg_catalog.pg_constraint c
            INNER JOIN pg_catalog.pg_namespace n
                       ON n.oid = c.connamespace
            CROSS JOIN LATERAL unnest(c.conkey) ak(k)
            INNER JOIN pg_attribute a
                       ON a.attrelid = c.conrelid
                          AND a.attnum = ak.k
       WHERE c.conrelid::regclass::text = tabname.relname
		 AND n.nspname = tabname.nsname;

	RETURN constraints;
END
$$;

--	SELECT  c.conrelid::regclass,c.conname,pg_get_constraintdef(c.oid),pg_get_expr(c.conbin,c.conrelid::regclass::oid)
--       FROM pg_constraint c
--            INNER JOIN pg_namespace n
--                       ON n.oid = c.connamespace
--            CROSS JOIN LATERAL unnest(c.conkey) ak(k)
--            INNER JOIN pg_attribute a
--                       ON a.attrelid = c.conrelid
--                          AND a.attnum = ak.k
--       WHERE c.conrelid::regclass::text = 'tb';
--
--	ALTER TABLE public.tb ADD CONSTRAINT tb_reg_id CHECK (substr(regionid,1,3) IN ('EUR','ASI','AFR','AMN')) NOT VALID;
--
--	RAISE DEBUG 'admin.get_table_constraints qualifier %',qualifiers;
--	RETURN qualifiers;
--END
--$$;

CREATE FUNCTION admin.get_table_constraint_qualifier(tabname admin.qualified_name)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
    qualifier   TEXT;

BEGIN
	SELECT array_to_string((SELECT array_agg(pg_get_expr(c.conbin,c.conrelid::regclass::oid)))::TEXT[],' AND ') INTO qualifier
       FROM pg_constraint c
            INNER JOIN pg_namespace n
                       ON n.oid = c.connamespace
            CROSS JOIN LATERAL unnest(c.conkey) ak(k)
            INNER JOIN pg_attribute a
                       ON a.attrelid = c.conrelid
                          AND a.attnum = ak.k
       WHERE c.conrelid::regclass::text = tabname.relname
		 AND n.nspname = tabname.nsname;

    RAISE DEBUG 'admin.get_table_constraint_qualifier qualifier %',qualifier;
    RETURN qualifier;
END
$$;

CREATE FUNCTION admin.get_table_default_partition_name(tabqname admin.qualified_name)
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
    deffullname admin.qualified_name;
BEGIN
    RAISE DEBUG 'SELECT dns.nspname AS "nsname", d.relname AS "relname" FROM pg_catalog.pg_partitioned_table pt JOIN pg_catalog.pg_class t ON t.oid = pt.partrelid JOIN pg_catalog.pg_namespace ns ON ns.oid = t.relnamespace JOIN pg_catalog.pg_class d ON d.oid = pt.partdefid JOIN pg_catalog.pg_namespace dns ON dns.oid = d.relnamespace WHERE ns.nspname = ''%'' AND t.relname = ''%'' LIMIT 1',(tabqname).nsname,(tabqname).relname;
    EXECUTE format('SELECT dns.nspname AS "nsname", d.relname AS "relname" FROM pg_catalog.pg_partitioned_table pt JOIN pg_catalog.pg_class t ON t.oid = pt.partrelid JOIN pg_catalog.pg_namespace ns ON ns.oid = t.relnamespace JOIN pg_catalog.pg_class d ON d.oid = pt.partdefid JOIN pg_catalog.pg_namespace dns ON dns.oid = d.relnamespace WHERE ns.nspname = ''%s'' AND t.relname = ''%s'' LIMIT 1',(tabqname).nsname,(tabqname).relname) INTO deffullname;
    RAISE DEBUG 'admin.get_table_default_partition deffullname ''%''',deffullname;

	IF ((deffullname IS NULL) OR ((deffullname.relname <> '') IS NOT TRUE))
	THEN
		RETURN NULL;
	END IF;
    RETURN deffullname;
END
$$;

CREATE FUNCTION admin.get_table_partition_names(tabqname admin.qualified_name)
RETURNS admin.qualified_name[]
LANGUAGE plpgsql
AS
$$
DECLARE
    partnames   admin.qualified_name[];
BEGIN
	RAISE DEBUG 'admin.get_table_partition_names tabqname %',tabqname;
    SELECT array_agg(admin.make_qualname(ns.nspname,c.relname)) INTO partnames
        FROM pg_catalog.pg_class p
        JOIN pg_catalog.pg_namespace ns ON ns.oid = p.relnamespace
        JOIN pg_catalog.pg_inherits inh ON inh.inhparent = p.oid
        JOIN pg_catalog.pg_class c ON c.oid = inh.inhrelid
        WHERE p.relkind = 'p'
          AND ns.nspname = (tabqname).nsname
          AND p.relname = (tabqname).relname;

	RAISE DEBUG 'admin.get_table_partition_names partnames %',partnames;
    RETURN partnames;
END
$$;

CREATE FUNCTION admin.get_partition_bound(tabqname admin.qualified_name)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
    bs          TEXT;
    bound       admin.partition_bound;
BEGIN
    RAISE DEBUG 'admin.get_partition_bound tabqname %',tabqname;
    SELECT pg_get_expr(c.relpartbound, c.oid) INTO bs
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace ns
        ON ns.oid = c.relnamespace
    WHERE ns.nspname = (tabqname).nsname
      AND c.relname = (tabqname).relname;

    RAISE DEBUG 'admin.get_partition_bound bs: %',bs;

	IF ((bs <> '') IS NOT TRUE)
	THEN
		RETURN NULL;
	END IF;

    bound=admin.string_to_partition_bound(admin.get_partition_keyspec(admin.get_partition_parent(tabqname)),bs);
    RAISE DEBUG 'admin.get_partition_bound bound: %',bound;

    RETURN bound;
END
$$;

CREATE FUNCTION admin.get_partition_bounds(tabqnames admin.qualified_name[])
RETURNS admin.partition_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
    name        admin.qualified_name;
    bounds      admin.partition_bound[];
BEGIN
    RAISE DEBUG 'admin.get_partition_bounds tabqnames: %',tabqnames;
    IF (tabqnames IS NULL)
    THEN
        RETURN NULL;
    END IF;
    FOREACH name IN ARRAY tabqnames
    LOOP
RAISE DEBUG 'admin.get_partition_bounds name: %',name;
        bounds=pg_catalog.array_append(bounds,admin.get_partition_bound(name));
    END LOOP;
RAISE DEBUG 'admin.get_partition_bounds bounds: %',bounds;
    RETURN bounds;
END
$$;

CREATE FUNCTION admin.get_partition(srcname admin.qualified_name)
RETURNS admin.partition
LANGUAGE plpgsql
AS
$$
DECLARE
    partbound   admin.partition_bound;
    part	    admin.partition;

BEGIN
	RAISE DEBUG 'admin.get_partition srcname %',srcname;

	partbound=admin.get_partition_bound(srcname);

	RAISE DEBUG 'admin.get_partition partbound %',partbound;

	IF (partbound IS NULL)
	THEN
		RETURN NULL;
	END IF;

	part=admin.make_partition(srcname,partbound);

	RAISE DEBUG 'admin.get_partition part %',part;
	RETURN part;
END
$$;

CREATE FUNCTION admin.get_partitions(VARIADIC srcnamelist admin.qualified_name[])
RETURNS admin.partition[]
LANGUAGE plpgsql
AS
$$
DECLARE
	srcname		admin.qualified_name;
    partbound   admin.partition_bound;
    part        admin.partition;
	parts		admin.partition[];

BEGIN
	RAISE DEBUG 'admin.get_partitions srcnamelist %',srcnamelist;

	FOREACH srcname IN ARRAY srcnamelist
	LOOP
		RAISE DEBUG 'admin.get_partitions srcname %',srcname;

	    partbound=admin.get_partition_bound(srcname);

		RAISE DEBUG 'admin.get_partitions partbound %',partbound;
    
    	part=admin.make_partition(srcname,partbound);

		RAISE DEBUG 'admin.get_partitions part %',part;

		parts=pg_catalog.array_append(parts,part);
		RAISE DEBUG 'admin.get_partitions parts %',parts;
	END LOOP;

	RETURN parts;
END
$$;

CREATE FUNCTION admin.detach_partition(tabqname admin.qualified_name, partqname admin.qualified_name, withdefault BOOLEAN=false)
RETURNS admin.qualified_name
LANGUAGE plpgsql
AS
$$
DECLARE
	defname	admin.qualified_name;

BEGIN
	RAISE DEBUG 'admin.detach_partition tabqname %',tabqname;
    RAISE DEBUG 'admin.detach_partition partqname %',partqname;
    RAISE DEBUG 'admin.detach_partition withdefault %',withdefault;

	defname=admin.get_table_default_partition_name(tabqname);

	RAISE DEBUG 'admin.detach_partition defname %',defname;

	IF ( NOT admin.table_exists(tabqname) )
	THEN
		RAISE EXCEPTION 'relation % does not exists',tabqname;
	ELSIF ( NOT (partqname = ANY(admin.get_table_partition_names(tabqname))))
	THEN
		RAISE DEBUG 'admin.detach_partition partqname ''%'' partitions(%) ''%''',partqname,tabqname,admin.get_table_partition_names(tabqname);
		RAISE EXCEPTION 'relation % is not a partition of %',partqname,tabqname;
	END IF;

	RAISE DEBUG 'admin.detach_partition ALTER TABLE %.% DETACH PARTITION %.%',(tabqname).nsname,(tabqname).relname,(partqname).nsname,(partqname).relname;
	EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',(tabqname).nsname,(tabqname).relname,(partqname).nsname,(partqname).relname);

	IF ((withdefault = true) AND ((defname.relname <> '') IS NOT TRUE) AND (partqname != defname))
	THEN
		RAISE DEBUG 'admin.detach_partition ALTER TABLE %.% DETACH PARTITION %.%',(tabqname).nsname,(tabqname).relname,(defname).nsname,(defname).relname;
		EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I',(tabqname).nsname,(tabqname).relname,(defname).nsname,(defname).relname);
	END IF;

	RETURN defname;
END
$$;

CREATE FUNCTION admin.attach_partition(tabqname admin.qualified_name, partqname admin.qualified_name, bound admin.partition_bound, defqname admin.qualified_name=NULL)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
	RAISE DEBUG 'admin.attach_partition tabqname %',tabqname;
    RAISE DEBUG 'admin.attach_partition partqname %',partqname;
    RAISE DEBUG 'admin.attach_partition bound %',bound;
    RAISE DEBUG 'admin.attach_partition defqname %',defqname;

    IF ( NOT admin.table_exists(tabqname) )
    THEN
        RAISE EXCEPTION 'relation % does not exists',tabqname;
    ELSIF ( SELECT partqname = ANY(admin.get_table_partition_names(tabqname)))
	THEN
        RAISE EXCEPTION 'relation %.% is already partition of %.%',(partqname).nsname,(partqname).relname,(tabqname).nsname,(tabqname).relname;
    END IF;

	EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I %s',(tabqname).nsname,(tabqname).relname,(partqname).nsname,(partqname).relname,admin.partition_bound_to_string(bound));

	RAISE DEBUG 'admin.attach_partition defqname ''%'' partqname ''%''',defqname,partqname;
    IF ((defqname IS NOT NULL) AND ((defqname.relname <> '') IS NOT TRUE) AND (partqname != defqname) AND (admin.get_table_default_partition_name(tabqname) IS NULL))
    THEN
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I DEFAULT',(tabqname).nsname,(tabqname).relname,(defqname).nsname,(defqname).relname);
    END IF;
END
$$;

CREATE FUNCTION admin.create_partition(tabname admin.qualified_name, part admin.partition)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	partname		admin.qualified_name;
	partbound		admin.partition_bound;
	partstring		TEXT;
	cmd				TEXT;

BEGIN
    RAISE DEBUG 'admin.create_partition tabname %',tabname;
	RAISE DEBUG 'admin.create_partition part %',part;
	
	partname=part.partname;
	RAISE DEBUG 'admin.create_partition partname %',partname;

	partbound=part.partbound;
	RAISE DEBUG 'admin.create_partition partbound %',partbound;

	partstring=admin.partition_bound_to_string(partbound);

	RAISE DEBUG 'admin.create_partition partstring %',partstring;

	IF (admin.table_exists(partname))
	THEN
		PERFORM admin.attach_partition(tabname,partname,partbound);
	ELSE
		cmd=format('CREATE TABLE %I.%I PARTITION OF %I.%I %s',partname.nsname,partname.relname,tabname.nsname,tabname.relname,partstring);
		RAISE DEBUG 'admin.create_partition cmd %',cmd;
		EXECUTE cmd;
	END IF;
END
$$;

CREATE FUNCTION admin.create_table(tabname admin.qualified_name, tplname admin.qualified_name, partkey admin.partition_keyspec=NULL, VARIADIC parts admin.partition[]=NULL)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	part		admin.partition;
	
BEGIN
    RAISE DEBUG 'admin.create_table tabname %',tabname;
    RAISE DEBUG 'admin.create_table tplname %',tplname;
    RAISE DEBUG 'admin.create_table partkey %',partkey;
    RAISE DEBUG 'admin.create_table parts %',parts;

    IF ((partkey IS NULL) IS NOT TRUE)
    THEN
        RAISE DEBUG 'admin.create_table partkey %',partkey;

        RAISE NOTICE 'create table %.% like %.% partition by %',tabname.nsname,tabname.relname,tplname.nsname,tplname.relname,admin.partition_keyspec_to_string(partkey);
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL) PARTITION BY %s',tabname.nsname,tabname.relname,tplname.nsname,tplname.relname,admin.partition_keyspec_to_string(partkey));

        IF ((parts IS NULL) IS NOT TRUE)
        THEN
			RAISE DEBUG 'admin.create_table parts %',parts;
            FOREACH part IN ARRAY parts
            LOOP
                RAISE DEBUG 'admin.create_table part %',part;
                PERFORM admin.create_partition(tabname,part);
            END LOOP;
        END IF;
    ELSE
        RAISE NOTICE 'create table %.% like %.%',tabname.nsname,tabname.relname,tplname.nsname,tplname.relname;
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)',tabname.nsname,tabname.relname,tplname.nsname,tplname.relname);
    END IF;
END
$$;


