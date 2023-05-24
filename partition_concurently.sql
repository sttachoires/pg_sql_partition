-
-- Complex partition operations concurently
--

\i partition_managers.sql

CREATE PROCEDURE admin.merge_list_partitions_concurently_concurently(tabqname admin.qualified_name, dstpart admin.partition, VARIADIC parts admin.partition[])
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
    RAISE DEBUG 'admin.merge_list_partitions_concurently tabqname %',tabqname;
    RAISE DEBUG 'admin.merge_list_partitions_concurently dstpart %',dstpart;
    RAISE DEBUG 'admin.merge_list_partitions_concurently parts %',parts;

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

            RAISE DEBUG 'admin.merge_list_partitions_concurently: dstbound %',dstbound;


            PERFORM admin.detach_partition(tabqname,dstname);
            defname=admin.detach_partition(tabqname,srcname, true);

			CALL admin.move_rows_concurently(srcname,dstname);

            PERFORM admin.drop_table(srcname);

            CALL admin.attach_partition_concurently(tabqname,dstname,dstbound,defname);
        END IF;
		COMMIT;
    END LOOP;
END
$$;


CREATE PROCEDURE admin.merge_partitions_concurently(tabqname admin.qualified_name, dstpart admin.partition, VARIADIC parts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
    IF (((dstpart.partbound).keyspec).strategy = 'list')
    THEN
        CALL admin.merge_list_partitions_concurently(tabqname, dstpart, VARIADIC parts);
    ELSIF (((dstpart.partbound).keyspec).strategy = 'range')
    THEN
        CALL admin.merge_range_partitions_concurently(tabqname, dstpart, VARIADIC parts);
    ELSIF (((dstpart.partbound).keyspec).strategy = 'hash')
    THEN
        CALL admin.merge_hash_partitions_concurently(tabqname, dstpart, VARIADIC parts);
    ELSE
        RAISE EXCEPTION 'unknown partition type %',((dstpart.partbound).keyspec).strategy;
    END IF;
END
$$;

