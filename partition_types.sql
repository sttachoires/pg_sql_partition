--
-- Types used by the partition frameworks
-- 	make and build from catalog
--

\i partition_utils.sql

CREATE TYPE admin.partition_strategy AS ENUM(
'list','range','hash');

CREATE TYPE admin.partition_keyspec AS (
	strategy	admin.partition_strategy,
	colnames	TEXT[],
	coltypes	TEXT[]
);

CREATE FUNCTION admin.make_partition_keyspec(partstrat TEXT,partcols TEXT[], partcoltypes TEXT[])
RETURNS admin.partition_keyspec
LANGUAGE plpgsql
AS
$$
DECLARE
    keyspec     admin.partition_keyspec;
	iter		BIGINT=0;

BEGIN
	keyspec.strategy=lower(trim(BOTH FROM partstrat::TEXT))::admin.partition_strategy;

	FOR iter IN 1..pg_catalog.array_length(partcols,1)
	LOOP
		RAISE DEBUG 'admin.make_partition_keyspec iter %',iter;
		keyspec.colnames=pg_catalog.array_append(keyspec.colnames,lower(trim(BOTH FROM partcols[iter])));
		keyspec.coltypes=pg_catalog.array_append(keyspec.coltypes,lower(trim(BOTH FROM partcoltypes[iter])));
	END LOOP;
	RAISE DEBUG 'admin.make_partition_keyspec iter %',iter;
	RETURN keyspec;
END
$$;

CREATE FUNCTION admin.get_partition_keyspec(tabqname admin.qualified_name)
RETURNS admin.partition_keyspec
LANGUAGE plpgsql
AS
$$
DECLARE
	partkeydef	TEXT;
	keyspec		admin.partition_keyspec;
	readkeydef  TEXT[];
	strategy    admin.partition_strategy;
	colname		TEXT;
	colnames	TEXT[];
	coltype		TEXT;
	coltypes	TEXT[];
BEGIN
	RAISE DEBUG 'admin.get_partition_keyspec tabqname %',tabqname;
	RAISE DEBUG 'pg_catalog.pg_get_partkeydef(format(''%%I.%%I'',tabqname.nsname,tabqname.relname)::regclass)';
	RAISE DEBUG 'admin.get_partition_keyspec %',format('%I.%I',tabqname.nsname,tabqname.relname);
	partkeydef=pg_catalog.pg_get_partkeydef(format('%I.%I',(tabqname).nsname,(tabqname).relname)::regclass);
	RAISE DEBUG 'admin.get_partition_keyspec partkeydef ''%''',partkeydef;
	
	IF (partkeydef IS NOT NULL)
	THEN
		readkeydef=regexp_match(partkeydef,'([^[:space:]]+)[[:space:]]*\(([^)]+)\)[[:space:]]*');
		strategy=lower(trim(BOTH readkeydef[1]))::admin.partition_strategy;
		RAISE DEBUG 'admin.get_partition_keyspec readkeydef %',readkeydef;
	
		FOREACH colname IN ARRAY pg_catalog.string_to_array(readkeydef[2],',')
		LOOP
			colname=lower(trim(BOTH FROM colname));
			SELECT * FROM admin.get_column_info(tabqname,colname) INTO colname,coltype;
			colnames=pg_catalog.array_append(colnames,colname);
	        coltypes=pg_catalog.array_append(coltypes,coltype);
		END LOOP;
	
		keyspec=admin.make_partition_keyspec(strategy::TEXT,colnames,coltypes);
	
		RETURN keyspec;
	ELSE
		RETURN NULL;
	END IF;
END
$$;

CREATE TYPE admin.hash_bound AS (
    modul   BIGINT,
    remain	BIGINT
);

CREATE FUNCTION admin.make_hash_bound(bmod BIGINT, brem BIGINT)
RETURNS admin.hash_bound
LANGUAGE plpgsql
AS $$
DECLARE
	hashbound	admin.hash_bound;

BEGIN
	IF (bmod < 1)
	THEN
		RAISE EXCEPTION 'modulus cannot be 0';
	END IF;

	hashbound.modul=bmod;
	hashbound.remain=brem;

	RETURN hashbound;
END
$$;

CREATE TYPE admin.range_bound AS (
	subtyp	TEXT,
	fromb	TEXT,
	tob		TEXT
);

CREATE FUNCTION admin.make_range_bound(bfrom ANYELEMENT,bto ANYELEMENT)
RETURNS admin.range_bound
LANGUAGE plpgsql
AS $$
DECLARE
    rangebound	admin.range_bound;

BEGIN
	rangebound.subtyp=pg_catalog.pg_typeof(bfrom);
	rangebound.fromb=bfrom;
	rangebound.tob=bto;

    RETURN rangebound;
END
$$;

CREATE TYPE admin.list_bound AS (
	subtyp  TEXT,
	elems	TEXT[]
);

CREATE FUNCTION admin.make_list_bound(comps ANYCOMPATIBLEARRAY)
RETURNS admin.list_bound
LANGUAGE plpgsql
AS $$
DECLARE
    listbound	admin.list_bound;

BEGIN
	RAISE DEBUG 'admin.make_list_bound comps %',comps;
	
	listbound.subtyp=pg_catalog.pg_typeof(comps);

	RAISE DEBUG 'admin.make_list_bound listbound %',listbound;

	SELECT admin.array_sort(comps) INTO listbound.elems;

	RAISE DEBUG 'admin.make_list_bound listbound %',listbound;

	RETURN listbound;
END
$$;

CREATE TYPE admin.partition_bound AS (
	isdefault	BOOLEAN,
	keyspec		admin.partition_keyspec,
	hashbound	admin.hash_bound,
	rangebound	admin.range_bound[],
	listbound	admin.list_bound
);

CREATE FUNCTION admin.make_default_partition_bound(pkeyspec admin.partition_keyspec)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS $$
DECLARE
    bound       admin.partition_bound;

BEGIN
	bound.keyspec=pkeyspec;
	bound.isdefault=true;

	RETURN bound;
END
$$;

CREATE FUNCTION admin.make_range_partition_bound(pkeyspec admin.partition_keyspec, rbounds admin.range_bound[])
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS $$
DECLARE
	bound	admin.partition_bound;

BEGIN
	bound.isdefault=false;
	bound.keyspec=pkeyspec;
	bound.rangebound=rbounds;

	RETURN bound;
END
$$;

CREATE FUNCTION admin.make_list_partition_bound(pkeyspec admin.partition_keyspec, lbound admin.list_bound)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS $$
DECLARE
    bound       admin.partition_bound;

BEGIN
	bound.isdefault=false;
    bound.keyspec=pkeyspec;
    bound.listbound=lbound;

	RETURN bound;
END
$$;

CREATE FUNCTION admin.make_hash_partition_bound(pkeyspec admin.partition_keyspec, hbound admin.hash_bound)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS $$
DECLARE
    bound       admin.partition_bound;

BEGIN
	bound.isdefault=false;
    bound.keyspec=pkeyspec;
    bound.hashbound=hbound;

    RETURN bound;
END
$$;

CREATE TYPE admin.partition AS (
    partname    admin.qualified_name,
    partbound   admin.partition_bound
);

CREATE FUNCTION admin.make_partition(pname admin.qualified_name, pbound admin.partition_bound)
RETURNS admin.partition
LANGUAGE plpgsql
AS
$$
DECLARE
	part	admin.partition;

BEGIN
	part.partname=pname;
	part.partbound=pbound;

	RETURN part;
END
$$;

CREATE TYPE admin.auto_strategy AS ENUM(
'modulus','list','interval','steps','bounds');
