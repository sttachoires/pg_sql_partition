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
	colexists	BOOLEAN;
	qualname	admin.qualified_name;
	defname		admin.qualified_name;
	col			TEXT;
	keyspec		admin.partition_keyspec;
	defbound	admin.partition_bound;

BEGIN
	RAISE DEBUG 'admin.alter_table_partition_by tabqname ''%'' skeyspec ''%''',tabqname,skeyspec;
	qualname=admin.string_to_qualname(tabqname);
	keyspec=admin.string_to_partition_keyspec(skeyspec);

	RAISE DEBUG 'admin.alter_table_partition_by qualname ''%''', qualname;
	RAISE DEBUG 'admin.alter_table_partition_by keyspec ''%''', keyspec;

	-- Check table exists
	tblexists=admin.table_exists(qualname);
	IF (NOT tblexists)
	THEN
	    RAISE EXCEPTION 'table % does not exists', qualname;
	END IF;
	RAISE DEBUG 'table ''%'' does not exists',qualname;

	-- Check partitionning columns exists
	FOREACH col IN ARRAY keyspec.colnames
	LOOP
		colexists=admin.column_exists(qualname,col);
	    IF (NOT colexists)
	    THEN
	        RAISE EXCEPTION 'Column % of table % does not exists',col,tabqname;
	    END IF;
	END LOOP;
	
	-- Get default name
	defname=admin.generate_default_partition_name(qualname);
	RAISE DEBUG 'admin.alter_table_partition_by defname %',defname;

	-- Rename table to default name
	PERFORM admin.rename_table(qualname,defname);

	-- Create partitionned table from initial table but with new parttype(partkeys)
	PERFORM admin.create_table(qualname,defname,keyspec);
	
	defbound=admin.make_default_partition_bound(keyspec);
	RAISE DEBUG 'admin.alter_table_partition_by defbound %',defbound;

	RAISE DEBUG 'admin.alter_table_partition_by admin.attach_partition(%,%,%)',qualname,defname,defbound;
    -- Attach default if defbound is not null
	PERFORM admin.attach_partition(qualname,defname,defbound);
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
		partkey=admin.string_to_partition_keyspec(partqkey);
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


