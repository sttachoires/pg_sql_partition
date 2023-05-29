--
-- Complex partition operations
--

\i partition_mngbounds.sql

CREATE FUNCTION admin.split_range_partition(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	srcname			admin.qualified_name;
    srcbound    	admin.partition_bound;
    dstpart			admin.partition;
    dstname     	admin.qualified_name;
    dstbound    	admin.partition_bound;
    defname     	admin.qualified_name;
	partboundqual	TEXT;

BEGIN
	srcname=srcpart.partname;
	srcbound=srcpart.partbound;

    FOREACH dstpart IN ARRAY dstparts
    LOOP
        dstname=(dstpart).partname;
		dstbound=(dstpart).partbound;
		srcbound=admin.split_range_partition_bound(srcbound,dstbound);

		IF (NOT admin.table_exists(dstname))
		THEN
			PERFORM admin.create_table(dstname,srcname);
		ELSE
			defname=admin.detach_partition(tabqname,dstname);
		END IF;
        defname=admin.detach_partition(tabqname,srcname,true);

		partboundqual=admin.range_partition_bound_to_qualifier(dstname,dstbound);

		PERFORM admin.move_rows(srcname,dstname,partboundqual);

        PERFORM admin.attach_partition(tabqname,srcname,srcbound);

        PERFORM admin.attach_partition(tabqname,dstname,dstbound,defname);
    END LOOP;
END
$$;

CREATE FUNCTION admin.split_hash_partition(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    keyspec         admin.partition_keyspec;
	nbpart			BIGINT;
    srcbound        admin.partition_bound;
    srcname         admin.qualified_name;
    dstname         admin.qualified_name;
    defname         admin.qualified_name;
    dstpart			admin.partition;
    dstbound        admin.partition_bound;
	partboundqual	TEXT;

BEGIN
    keyspec=admin.get_partition_keyspec(tabqname);
    RAISE DEBUG 'admin.alter_table_split_hash_partition keyspec ''%''',keyspec;
	nbpart=pg_catalog.array_length(dstparts,1);

	srcname=srcpart.partname;
    srcbound=admin.split_hash_partition_bound(srcpart.partbound,(nbpart+1));

	-- detach source
    defname=admin.detach_partition(tabqname,srcname,true);

	FOREACH dstpart IN ARRAY dstparts
    LOOP
		-- detach or create split partition
        dstname=(dstpart).partname;
		dstbound=(dstpart).partbound;
		partboundqual=admin.hash_partition_bound_to_qualifier(tabqname,dstbound);

		IF (admin.table_exists(dstname))
		THEN
			PERFORM admin.detach_partition(tabqname,srcname,false);
		ELSE
			PERFORM admin.create_table(dstname,srcname);
		END IF;

		-- split source to partition
		PERFORM admin.move_rows(srcname,dstname,partboundqual);
    END LOOP;

	-- attach every one
	FOREACH dstpart IN ARRAY dstparts
    LOOP
        dstname=(dstpart).partname;
        PERFORM admin.attach_partition(tabqname,dstname,(dstpart).partbound);
	END LOOP;
    PERFORM admin.attach_partition(tabqname,srcname,srcbound,defname);
END
$$;

CREATE FUNCTION admin.split_list_partition(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    dstname			admin.qualified_name;
    defname			admin.qualified_name;
	keyspec			admin.partition_keyspec;
	dstpart			admin.partition;
	srcname			admin.qualified_name;
	srcbound		admin.partition_bound;
	qualifier		TEXT;

BEGIN
	keyspec=admin.get_partition_keyspec(tabqname);
	RAISE DEBUG 'admin.alter_table_split_list_partition keyspec ''%''',keyspec;
	
	srcname=srcpart.partname;
	srcbound=srcpart.partbound;

	FOREACH dstpart IN ARRAY dstparts
	LOOP
		dstname=(dstpart).partname;
		
		srcbound=admin.split_list_partition_bound(srcbound,(dstpart).partbound);

		PERFORM admin.create_table(dstname,srcname);

		defname=admin.detach_partition(tabqname,srcname,true);

		qualifier=admin.list_partition_bound_to_qualifier((dstpart).partname,(dstpart).partbound);

		PERFORM admin.move_rows(srcname,dstname,qualifier);

		PERFORM admin.attach_partition(tabqname,srcname,srcbound);

		PERFORM admin.attach_partition(tabqname,dstname,(dstpart).partbound,defname);
	END LOOP;
END
$$;

CREATE FUNCTION admin.split_partition(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	keyspec		admin.partition_keyspec;

BEGIN
    keyspec=admin.get_partition_keyspec(tabqname);
    RAISE DEBUG 'admin.split_partition keyspec ''%''',keyspec;

	IF (keyspec.strategy = 'range')
	THEN
		PERFORM admin.split_range_partition(tabqname, srcpart, VARIADIC dstparts);
	ELSIF (keyspec.strategy = 'list')
    THEN
		PERFORM admin.split_list_partition(tabqname, srcpart, VARIADIC dstparts);
    ELSIF (keyspec.strategy = 'hash')
	THEN
		PERFORM admin.split_hash_partition(tabqname, srcpart, VARIADIC dstparts);
	ELSE
		RAISE EXCEPTION 'unknow partition strategy %',keyspec.strategy;
	END IF;
END
$$;

CREATE FUNCTION admin.merge_list_partitions(tabqname admin.qualified_name, dstpart admin.partition, VARIADIC parts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    dstname     admin.qualified_name;
    dstbound    admin.partition_bound;
    part        admin.partition;
    sparts      admin.partition[];
    srcbound    admin.partition_bound;
    srcname     admin.qualified_name;
    defname     admin.qualified_name;

BEGIN
    RAISE DEBUG 'admin.merge_list_partitions tabqname %',tabqname;
    RAISE DEBUG 'admin.merge_list_partitions dstpart %',dstpart;
	RAISE DEBUG 'admin.merge_list_partitions parts %',parts;

    -- Check partition specifications
    sparts=admin.sort_partitions(parts);
	dstname=dstpart.partname;
	dstbound=dstpart.partbound;

    FOREACH part IN ARRAY sparts
    LOOP
        srcname=(part).partname;
        IF ( srcname != dstname )
        THEN
            srcbound=(part).partbound;

            dstbound=admin.merge_list_partition_bounds(dstbound,srcbound);

            RAISE DEBUG 'admin.merge_list_partitions: dstbound %',dstbound;


            PERFORM admin.detach_partition(tabqname,dstname);
            defname=admin.detach_partition(tabqname,srcname, true);

			PERFORM admin.copy_rows(srcname,dstname);

            PERFORM admin.drop_table(srcname);

            PERFORM admin.attach_partition(tabqname,dstname,dstbound,defname);
        END IF;
    END LOOP;
END
$$;

CREATE FUNCTION admin.merge_range_partitions(tabqname admin.qualified_name, dstpart admin.partition, VARIADIC parts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    dstname     admin.qualified_name;
    dstbound    admin.partition_bound;
    part        admin.partition;
    sparts      admin.partition[];
    srcbound    admin.partition_bound;
    srcname     admin.qualified_name;
    defname     admin.qualified_name;

BEGIN
    RAISE DEBUG 'admin.merge_range_partitions tabqname %',tabqname;
    RAISE DEBUG 'admin.merge_range_partitions dstpart %',dstpart;
    RAISE DEBUG 'admin.merge_range_partitions parts %',parts;

    -- Check partition specifications
    sparts=admin.sort_partitions(parts);
    dstname=dstpart.partname;
    dstbound=dstpart.partbound;

    FOREACH part IN ARRAY sparts
    LOOP
        srcname=(part).partname;
        IF ( srcname != dstname )
        THEN
            srcbound=(part).partbound;

            dstbound=admin.merge_range_partition_bounds(dstbound,srcbound);

            RAISE DEBUG 'admin.merge_range_partitions dstbound %',dstbound;


            PERFORM admin.detach_partition(tabqname,dstname);
            defname=admin.detach_partition(tabqname,srcname, true);

			PERFORM admin.copy_rows(srcname,dstname);

            PERFORM admin.drop_table(srcname);

            PERFORM admin.attach_partition(tabqname,dstname,dstbound,defname);
        END IF;
    END LOOP;
END
$$;

CREATE FUNCTION admin.merge_hash_partitions(tabqname admin.qualified_name, dstpart admin.partition, VARIADIC parts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
    dstname     admin.qualified_name;
    dstbound    admin.partition_bound;
    part        admin.partition;
    sparts      admin.partition[];
    srcbound    admin.partition_bound;
    srcbounds   admin.partition_bound[];
    srcname     admin.qualified_name;
    defname     admin.qualified_name;

BEGIN
    RAISE DEBUG 'admin.merge_hash_partitions tabqname %',tabqname;
    RAISE DEBUG 'admin.merge_hash_partitions dstpart %',dstpart;
    RAISE DEBUG 'admin.merge_hash_partitions parts %',parts;

    -- Check partition specifications
    sparts=admin.sort_partitions(parts);
    dstname=dstpart.partname;
    dstbound=dstpart.partbound;

    defname=admin.detach_partition(tabqname,dstname,true);
    FOREACH part IN ARRAY sparts
    LOOP
        srcname=(part).partname;
        IF ( srcname != dstname )
        THEN
            srcbound=(part).partbound;
			srcbounds=pg_catalog.array_append(srcbounds,srcbound);

            PERFORM admin.detach_partition(tabqname,srcname);

			PERFORM admin.copy_rows(srcname,dstname);

            PERFORM admin.drop_table(srcname);

        END IF;
    END LOOP;

	RAISE DEBUG 'admin.merge_hash_partitions dstbound=admin.merge_hash_partition_bounds(%,VARIADIC %)',dstbound, srcbounds;
    dstbound=admin.merge_hash_partition_bounds(VARIADIC pg_catalog.array_prepend(dstbound, srcbounds));
    PERFORM admin.attach_partition(tabqname,dstname,dstbound,defname);
END
$$;

CREATE FUNCTION admin.merge_partitions(tabqname admin.qualified_name, dstpart admin.partition, VARIADIC parts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
	IF (((dstpart.partbound).keyspec).strategy = 'list')
	THEN
	    PERFORM admin.merge_list_partitions(tabqname, dstpart, VARIADIC parts);
	ELSIF (((dstpart.partbound).keyspec).strategy = 'range')
	THEN
	    PERFORM admin.merge_range_partitions(tabqname, dstpart, VARIADIC parts);
	ELSIF (((dstpart.partbound).keyspec).strategy = 'hash')
	THEN
	    PERFORM admin.merge_hash_partitions(tabqname, dstpart, VARIADIC parts);
	ELSE
	    RAISE EXCEPTION 'unknown partition type %',((dstpart.partbound).keyspec).strategy;
	END IF;
END
$$;
