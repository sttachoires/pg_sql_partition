--
-- Partition handling commands for end user
--
\i partition_managers.sql


CREATE FUNCTION admin.alter_table_unpartition(tabqname TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	tabname		admin.qualified_name;
	partname	admin.qualified_name;
	childs		admin.qualified_name[];
	parts		admin.partition[];
	dstpart		admin.partition;

BEGIN
	RAISE DEBUG 'admin.alter_table_unpartition tabqname %',tabqname;
	tabname=admin.string_to_qualname(tabqname);
	RAISE DEBUG 'admin.alter_table_unpartition tabname %',tabname;

	childs=admin.get_table_partition_names(tabname);

	RAISE DEBUG 'admin.alter_table_unpartition childs %',childs;

	parts=admin.get_partitions(VARIADIC childs);

	RAISE DEBUG 'admin.alter_table_unpartition parts %',parts;

	dstpart=parts[1];

	RAISE DEBUG 'admin.alter_table_unpartition dstpart %',dstpart;

	parts=pg_catalog.array_remove(parts,dstpart);

	RAISE DEBUG 'admin.alter_table_unpartition parts %',parts;

	IF (pg_catalog.array_length(parts,1) > 0)
	THEN
		RAISE DEBUG 'admin.alter_table_unpartition admin.merge_partitions(%,%,%)',tabname, dstpart, parts;
		PERFORM admin.merge_partitions(tabname, dstpart, VARIADIC parts);

		childs=admin.get_table_partition_names(tabname);
	    RAISE DEBUG 'admin.alter_table_unpartition childs %',childs;

	    parts=admin.get_partitions(VARIADIC childs);
    	RAISE DEBUG 'admin.alter_table_unpartition parts %',parts;

		partname=(parts[1]).partname;
	END IF;
	RAISE DEBUG 'admin.alter_table_unpartition: parts %',parts;
	RAISE DEBUG 'admin.alter_table_unpartition: partname %',partname;

	IF (array_length(parts,1) != 1)
	THEN
		RAISE EXCEPTION 'bad merging';
	END IF;

	RAISE DEBUG 'admin.alter_table_unpartition admin.detach_partition(%,%)',tabname,partname;
	PERFORM admin.detach_partition(tabname,partname);

	RAISE DEBUG 'admin.alter_table_unpartition admin.drop_table(%)',tabname;
	PERFORM admin.drop_table(tabname);

	PERFORM admin.rename_table(partname,tabname);
END
$$;

CREATE FUNCTION admin.alter_table_partition_by(tabqname TEXT, skeyspec TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS 
$$
DECLARE
	tblexists	BOOLEAN;
	qualname	admin.qualified_name;
	defname		admin.qualified_name;
	keyspec		admin.partition_keyspec;
	defbound	admin.partition_bound;

BEGIN
	RAISE DEBUG 'admin.alter_table_partition_by tabqname ''%'' skeyspec ''%''',tabqname,skeyspec;
	qualname=admin.string_to_qualname(tabqname);
	keyspec=admin.string_to_partition_keyspec(qualname,skeyspec);

	RAISE DEBUG 'admin.alter_table_partition_by qualname ''%''', qualname;
	RAISE DEBUG 'admin.alter_table_partition_by keyspec ''%''', keyspec;

	-- Check table exists
	tblexists=admin.table_exists(qualname);
	IF (NOT tblexists)
	THEN
	    RAISE EXCEPTION 'table % does not exists', qualname;
	END IF;
	RAISE DEBUG 'table ''%'' does not exists',qualname;

	-- Get default name
	defname=admin.generate_default_partition_name(qualname);
	RAISE DEBUG 'admin.alter_table_partition_by defname %',defname;

	defbound=admin.make_default_partition_bound(keyspec);
	RAISE DEBUG 'admin.alter_table_partition_by defbound %',defbound;

	PERFORM format('ALTER TABLE %I.%I ADD CONSTRAINT CHECK "%s_check_part" (%s) NOT VALID',
		qualname.nsname,qualname.relname,qualname.relname,admin.partition_bound_to_qualifier(qualname,defbound));

	-- Create partitionned table from initial table but with new parttype(partkeys)
	PERFORM admin.create_table(defname,qualname,keyspec);
	
	-- Swap the two tables
	PERFORM admin.swap_tables(qualname,defname);

	PERFORM admin.attach_partition(qualname,defname,defbound);

	PERFORM format('ALTER TABLE %I.%I DROP CONSTRAINT "%s_check_part"',
		qualname.nsname,qualname.relname,qualname.relname);

END
$$;

CREATE FUNCTION admin.alter_table_split_partition(tabqname TEXT, srcqname TEXT, VARIADIC partspecs TEXT[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	tabname		admin.qualified_name;
	srcname		admin.qualified_name;
	srcpart		admin.partition;
	keyspec		admin.partition_keyspec;
	partspec	TEXT;
	part		admin.partition;
	parts		admin.partition[];

BEGIN
	RAISE DEBUG 'admin.alter_table_split_partition tabqname %',tabqname;
	tabname=admin.string_to_qualname(tabqname);
    RAISE DEBUG 'admin.alter_table_split_partition tabname %',tabname;

    RAISE DEBUG 'admin.alter_table_split_partition srcqname %',srcqname;
	srcname=admin.string_to_qualname(srcqname);
    RAISE DEBUG 'admin.alter_table_split_partition srcname %',srcname;

	srcpart=admin.get_partition(srcname);
    RAISE DEBUG 'admin.alter_table_split_partition srcpart %',srcpart;

	keyspec=admin.get_partition_keyspec(tabname);
    RAISE DEBUG 'admin.alter_table_split_partition keyspec %',keyspec;

	FOREACH partspec IN ARRAY partspecs
	LOOP
		RAISE DEBUG 'admin.alter_table_split_partition partspec %',partspec;
		part=admin.string_to_partition(keyspec,partspec);
		RAISE DEBUG 'admin.alter_table_split_partition part %',part;
		parts=pg_catalog.array_append(parts,part);
		RAISE DEBUG 'admin.alter_table_split_partition parts %',parts;
	END LOOP;
	
	IF (keyspec.strategy = 'list')
	THEN
		PERFORM admin.split_list_partition(tabname, srcpart, VARIADIC parts);
	ELSIF (keyspec.strategy = 'range')
	THEN
		PERFORM admin.split_range_partition(tabname, srcpart, VARIADIC parts);
	ELSIF (keyspec.strategy = 'hash')
	THEN
		PERFORM admin.split_hash_partition(tabname, srcpart, VARIADIC parts);
	ELSE
		RAISE EXCEPTION 'unknown partition strategy %',keyspec.strategy;
	END IF;
END
$$;

--CREATE PROCEDURE admin.alter_table_split_partition_concurently(tabqname TEXT, srcqname TEXT, VARIADIC partspecs TEXT[])
--LANGUAGE plpgsql
--AS
--$$
--DECLARE
--    tabname     admin.qualified_name;
--    srcname     admin.qualified_name;
--    srcpart     admin.partition;
--    keyspec     admin.partition_keyspec;
--    partspec    TEXT;
--    part        admin.partition;
--    parts       admin.partition[];
--
--BEGIN
--    RAISE DEBUG 'admin.alter_table_split_partition tabqname %',tabqname;
--    tabname=admin.string_to_qualname(tabqname);
--    RAISE DEBUG 'admin.alter_table_split_partition tabname %',tabname;
--
--    RAISE DEBUG 'admin.alter_table_split_partition srcqname %',srcqname;
--    srcname=admin.string_to_qualname(srcqname);
--    RAISE DEBUG 'admin.alter_table_split_partition srcname %',srcname;
--
--    srcpart=admin.get_partition(srcname);
--    RAISE DEBUG 'admin.alter_table_split_partition srcpart %',srcpart;
--
--    keyspec=admin.get_partition_keyspec(tabname);
--    RAISE DEBUG 'admin.alter_table_split_partition keyspec %',keyspec;
--
--    FOREACH partspec IN ARRAY partspecs
--    LOOP
--        RAISE DEBUG 'admin.alter_table_split_partition partspec %',partspec;
--        part=admin.string_to_partition(keyspec,partspec);
--        RAISE DEBUG 'admin.alter_table_split_partition part %',part;
--        parts=pg_catalog.array_append(parts,part);
--        RAISE DEBUG 'admin.alter_table_split_partition parts %',parts;
--    END LOOP;
--
--    IF (keyspec.strategy = 'list')
--    THEN
--        PERFORM admin.split_list_partition(tabname, srcpart, VARIADIC parts);
--    ELSIF (keyspec.strategy = 'range')
--    THEN
--        CALL admin.split_range_partition_concurently(tabname, srcpart, VARIADIC parts);
--    ELSIF (keyspec.strategy = 'hash')
--    THEN
--        PERFORM admin.split_hash_partition(tabname, srcpart, VARIADIC parts);
--    ELSE
--        RAISE EXCEPTION 'unknown partition strategy %',keyspec.strategy;
--    END IF;
--END
--$$;

CREATE FUNCTION admin.alter_table_merge_partitions(tabqname TEXT, dstqname TEXT, VARIADIC srcnamelist TEXT[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	qualname	admin.qualified_name;
	dstname		admin.qualified_name;
	dstpart		admin.partition;
    keyspec		admin.partition_keyspec;
    partstrat	admin.partition_strategy;
	part		admin.partition;
	parts		admin.partition[];
    name		TEXT;
	srcname		admin.qualified_name;

BEGIN
	RAISE DEBUG 'admin.alter_table_merge_partitions tabqname %',tabqname;
	qualname=admin.string_to_qualname(tabqname);
	RAISE DEBUG 'admin.alter_table_merge_partitions qualname %',qualname;
	RAISE DEBUG 'admin.alter_table_merge_partitions dstqname %',dstqname;
	dstname=admin.string_to_qualname(dstqname);
	RAISE DEBUG 'admin.alter_table_merge_partitions dstname %',dstname;

	RAISE DEBUG 'admin.alter_table_merge_partitions srcnamelist %',srcnamelist;

    -- Check partition specifications
	keyspec=admin.get_partition_keyspec(qualname);
	RAISE DEBUG 'admin.alter_table_merge_partitions keyspec %',keyspec;
	partstrat=keyspec.strategy;
	RAISE DEBUG 'admin.alter_table_merge_partitions partstrat %',partstrat;
    RAISE DEBUG 'admin.alter_table_merge_partitions dstname %',dstname;
	dstpart=admin.get_partition(dstname);
	RAISE DEBUG 'admin.alter_table_merge_partitions dstpart %',dstpart;
	FOREACH name IN ARRAY srcnamelist
	LOOP
		RAISE DEBUG 'admin.alter_table_merge_partitions name %',name;
		srcname=admin.string_to_qualname(name);
		RAISE DEBUG 'admin.alter_table_merge_partitions srcname %',srcname;
		IF (srcname <> dstname)
		THEN
			part=admin.get_partition(srcname);
			RAISE DEBUG 'admin.alter_table_merge_partitions part %',part;
			parts=array_append(parts,part);
		END IF;
		RAISE DEBUG 'admin.alter_table_merge_partitions parts %',parts;
	END LOOP;
	parts=admin.sort_partitions(parts);
	RAISE DEBUG 'admin.alter_table_merge_partitions parts %',parts;

	PERFORM admin.merge_partitions(qualname,dstpart,VARIADIC parts);
END
$$;

CREATE FUNCTION admin.create_table_like(tabqname TEXT, tplqname TEXT, partqkey TEXT=NULL, VARIADIC qparts TEXT[]=NULL)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
	tabname		admin.qualified_name;
	tplname		admin.qualified_name;
	partkey		admin.partition_keyspec=NULL;
	qpart		TEXT;
	part		admin.partition;
	parts		admin.partition[]=NULL;

BEGIN
	RAISE DEBUG 'admin.create_table_like tabqname %',tabqname;
	RAISE DEBUG 'admin.create_table_like tplqname %',tplqname;
	RAISE DEBUG 'admin.create_table_like partqkey %',partqkey;
    RAISE DEBUG 'admin.create_table_like qparts %',qparts;

	tabname=admin.string_to_qualname(tabqname);

	RAISE DEBUG 'admin.create_table_like tabname %',tabname;

	tplname=admin.string_to_qualname(tplqname);

    RAISE DEBUG 'admin.create_table_like tplname %',tplname;

	IF ((partqkey <> '') IS TRUE)
	THEN
		partkey=admin.string_to_partition_keyspec(tabname,partqkey);
		RAISE DEBUG 'admin.create_table_like partkey %',partkey;

		IF ((qparts[1] <> '') IS TRUE)
		THEN
			FOREACH qpart IN ARRAY qparts
			LOOP
				RAISE DEBUG 'admin.create_table_like qpart %',qpart;

				part=admin.string_to_partition(partkey,qpart);
				RAISE DEBUG 'admin.create_table_like part %',part;

				parts=pg_catalog.array_append(parts,part);
			END LOOP;
		END IF;
	END IF;

	RAISE DEBUG 'admin.create_table_like admin.create_table(%,%,%,%)',tabname,tplname,partkey,parts;
	PERFORM admin.create_table(tabname,tplname,partkey,VARIADIC parts);
END
$$;

CREATE FUNCTION admin.create_automatic_table_like(tab TEXT, tpl TEXT, VARIADIC partdescs TEXT[])
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    tabname     admin.qualified_name;
    tplname     admin.qualified_name;
	iter		BIGINT;
    partkey     admin.partition_keyspec=NULL;
    parts       admin.partition[]=NULL;

BEGIN
	tabname=admin.string_to_qualname(tab);
	tplname=admin.string_to_qualname(tpl);
	RAISE DEBUG 'admin.create_automatic_table_like(%,%,%)',tabname,tplname,pg_catalog.array_to_string(partdescs,',');

	-- Decode partdesc
	FOR iter IN pg_catalog.array_length(partdescs,1)..2 BY 2
	LOOP
		partkey=admin.string_to_partition_keyspec(tplname,partdescs[iter-1]);
		IF (partkey IS NULL)
		THEN
			RAISE EXCEPTION 'unknow partition key description %',partdescs[iter-1];
		END IF;
		parts=admin.string_to_automatic_partitions(tabname,partkey,partdescs[iter]);
		IF (parts IS NULL)
		THEN
			RAISE EXCEPTION 'unknow automatic partition description % for partition key description %',partdescs[iter],partdescs[iter-1];
		END IF;
	END LOOP;

	RAISE DEBUG 'admin.create_automatic_table_like admin.create_table(%,%,%,%)',tabname,tplname,partkey,pg_catalog.array_to_string(parts,',');
	PERFORM admin.create_table(tabname,tplname,partkey,VARIADIC parts);
END
$$;
