--
-- Bound operations
--
\i partition_opetables.sql


CREATE FUNCTION admin.list_partition_bound_to_qualifier(parent admin.qualified_name, bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
    qualifier   TEXT;
BEGIN
    RAISE DEBUG 'admin.list_partition_bound_to_qualifier parent %',parent;
    RAISE DEBUG 'admin.list_partition_bound_to_qualifier bound %',bound;

    IF (bound.isdefault)
    THEN
        qualifier='(true)';
	ELSE
    	qualifier=format('((%I IS NOT NULL) AND (%I = ANY (''%s''::%s)))',(bound.keyspec).colnames[1],(bound.keyspec).colnames[1],(bound.listbound).elems,(bound.listbound).subtyp);
    END IF;

    RETURN qualifier;
END
$$;

CREATE FUNCTION admin.hash_partition_bound_to_qualifier(parent admin.qualified_name, bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
    modul       BIGINT;
    remain      BIGINT;
    qualifier   TEXT;

BEGIN
    IF (bound.isdefault)
    THEN
        qualifier='(true)';
	ELSE
	    modul=(bound.hashbound).modul;
	    remain=(bound.hashbound).remain;
	    -- hashtextextended('value', 8816678312871386365)::numeric + 5305509591434766563) % 8
	
	    qualifier=format('satisfies_hash_partition(''%I.%I''::regclass::oid,%s,%s,%I)',parent.nsname,parent.relname,modul,remain,array_to_string((bound.keyspec).colnames,','));
	    qualifier=concat('(',qualifier,')');
	    RAISE DEBUG 'admin.hash_partition_bound_to_qualifier qualifier ''%''',qualifier;
    END IF;
    RETURN qualifier;
END
$$;

CREATE FUNCTION admin.range_partition_bound_to_qualifier(parent admin.qualified_name, bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
    iter        BIGINT;
    reiter      BIGINT;
    colname     TEXT;
    qualifier   TEXT;

BEGIN
	RAISE DEBUG 'admin.range_partition_bound_to_qualifier parent %',parent;
	RAISE DEBUG 'admin.range_partition_bound_to_qualifier bound %',bound;

    IF (bound.isdefault)
    THEN
        RETURN '(true)';
    END IF;

    -- NOT NULL clause part
    FOR iter IN 1..pg_catalog.array_length((bound.keyspec).colnames,1)
    LOOP
        RAISE DEBUG 'admin.range_partition_bound_to_qualifier not null clause part iter %',iter;
        colname=trim(BOTH (bound.keyspec).colnames[iter]);
        IF (iter = 1)
        THEN
            qualifier=format('(%I IS NOT NULL)',colname);
        ELSE
            qualifier=qualifier||format(' AND (%I IS NOT NULL)',colname);
        END IF;
    END LOOP;

    -- from bound clause part
    FOR iter IN 1..pg_catalog.array_length((bound.keyspec).colnames,1)
    LOOP
        RAISE DEBUG 'admin.range_partition_bound_to_qualifier from bound clause part iter %',iter;
        IF (iter = 1)
        THEN
            IF (pg_catalog.array_length((bound.keyspec).colnames,1) = 1)
            THEN
                qualifier=qualifier||format(' AND (%I >= ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[iter]),(bound.rangebound[iter]).fromb,(bound.keyspec).coltypes[iter]);
            ELSE
                qualifier=qualifier||format(' AND ((%I > ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[iter]),(bound.rangebound[iter]).fromb,(bound.keyspec).coltypes[iter]);
            END IF;
        ELSE
            qualifier=qualifier||' OR (';
            FOR reiter IN 1..iter
            LOOP
                RAISE DEBUG 'admin.range_partition_bound_to_qualifier from bound clause part iter / reiter %/%',iter,reiter;
                IF (reiter = 1)
                THEN
                    qualifier=qualifier||format('(%I = ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[reiter]),(bound.rangebound[reiter]).fromb,(bound.keyspec).coltypes[reiter]);
                ELSIF (reiter < iter)
                THEN
                    qualifier=qualifier||format(' AND (%I = ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[reiter]),(bound.rangebound[reiter]).fromb,(bound.keyspec).coltypes[reiter]);
                ELSIF (reiter = iter)
                THEN
                    qualifier=qualifier||format(' AND (%I >= ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[reiter]),(bound.rangebound[reiter]).fromb,(bound.keyspec).coltypes[reiter]);
                ELSE
                    RAISE EXCEPTION 'that should not happens here : admin.range_partition_bound_to_qualifier iter/reiter %/%',iter,reiter;
                END IF;
            END LOOP;
            qualifier=qualifier||')';
        END IF;
    END LOOP;
    IF (pg_catalog.array_length((bound.keyspec).colnames,1) > 1)
    THEN
        qualifier=qualifier||')';
    END IF;

    -- to bound clause part
    FOR iter IN 1..pg_catalog.array_length((bound.keyspec).colnames,1)
    LOOP
        RAISE DEBUG 'admin.range_partition_bound_to_qualifier to bound clause part iter %',iter;
        IF (iter = 1)
        THEN
            IF (pg_catalog.array_length((bound.keyspec).colnames,1) = 1)
            THEN
                qualifier=qualifier||format(' AND (%I < ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[iter]),(bound.rangebound[iter]).tob,(bound.keyspec).coltypes[iter]);

            ELSE
                qualifier=qualifier||format(' AND ((%I < ''%s''::%s)',trim(BOTH (bound.keyspec).colnames[iter]),(bound.rangebound[iter]).tob,(bound.keyspec).coltypes[iter]);
            END IF;
        ELSE
            qualifier=qualifier||' OR (';
            FOR reiter IN 1..iter
            LOOP
                RAISE DEBUG 'admin.range_partition_bound_to_qualifier to bound clause part iter / reiter %/%',iter,reiter;
                IF (reiter = 1)
                THEN
                    qualifier=qualifier||format('(%I = ''%s''::%s )',trim(BOTH (bound.keyspec).colnames[reiter]),(bound.rangebound[reiter]).tob,(bound.keyspec).coltypes[reiter]);
                ELSIF (reiter < iter)
                THEN
                    qualifier=qualifier||format(' AND (%I = ''%s''::%s )',trim(BOTH (bound.keyspec).colnames[reiter]),(bound.rangebound[reiter]).tob,(bound.keyspec).coltypes[reiter]);
                ELSIF (reiter = iter)
                THEN
                    qualifier=qualifier||format(' AND (%I < ''%s''::%s )',trim(BOTH (bound.keyspec).colnames[reiter]),(bound.rangebound[reiter]).tob,(bound.keyspec).coltypes[reiter]);
                ELSE
                    RAISE EXCEPTION 'that should not happens here : admin.range_partition_bound_to_qualifier iter/reiter %/%',iter,reiter;
                END IF;
            END LOOP;
            qualifier=qualifier||' )';
        END IF;
        RAISE DEBUG 'admin.range_partition_bound_to_qualifier qualifier ''%''',qualifier;
    END LOOP;
    IF (pg_catalog.array_length((bound.keyspec).colnames,1) > 1)
    THEN
        qualifier=qualifier||' )';
    END IF;

    qualifier=concat('(',qualifier,')');
    RAISE DEBUG 'admin.range_partition_bound_to_qualifier qualifier ''%''',qualifier;
    RETURN qualifier;
END
$$;

CREATE FUNCTION admin.partition_bound_to_qualifier(tabname admin.qualified_name, bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    qualifier	TEXT;

BEGIN
	IF ((bound.keyspec).strategy = 'range')
	THEN
		qualifier=admin.range_partition_bound_to_qualifier(tabname, bound);
	ELSIF ((bound.keyspec).strategy = 'list')
    THEN
		qualifier=admin.list_partition_bound_to_qualifier(tabname, bound);
    ELSIF ((bound.keyspec).strategy = 'hash')
    THEN
		qualifier=admin.hash_partition_bound_to_qualifier(tabname, bound);
    ELSE
		RAISE EXCEPTION 'unknown partition strategy %',(bound.keyspec).strategy;
	END IF;

	RETURN qualifier;
END
$$;

CREATE FUNCTION admin.sort_partition_bounds(bounds admin.partition_bound[])
RETURNS admin.partition_bound[]
LANGUAGE plpgsql
AS $$
DECLARE
	sortedbounds	admin.partition_bound[];
BEGIN
	RAISE DEBUG 'admin.sort_partition_bounds bounds %',bounds;

	IF ( ((bounds[1]).keyspec).strategy = 'range' )
    THEN
		EXECUTE format('SELECT pg_catalog.array_agg(bnd::admin.partition_bound) FROM (
                            SELECT * FROM unnest($1) AS b ORDER BY ((b.rangebound)[1]).fromb::%s,
                                                                   ((b.rangebound)[1]).tob::%s
                            ) AS bnd',
                        ((bounds[1]).keyspec).coltypes[1],((bounds[1]).keyspec).coltypes[1])
            USING bounds
            INTO sortedbounds;
	ELSIF ( ((bounds[1]).keyspec).strategy = 'list' )
    THEN
        EXECUTE format('SELECT pg_catalog.array_agg(bnd::admin.partition_bound) FROM (
                            SELECT * FROM unnest($1) AS b ORDER BY (b.listbound).elems::%s
                            ) AS bnd',
                        ((bounds[1]).keyspec).coltypes[1])
            USING bounds
            INTO sortedbounds;
	ELSIF ( ((bounds[1]).keyspec).strategy = 'hash' )
    THEN
		EXECUTE 'SELECT pg_catalog.array_agg(bnd::admin.partition_bound) FROM (
                    SELECT * FROM unnest($1) AS b ORDER BY (b.hashbound).modul,
                                                           (b.hashbound).remain
                    ) AS bnd'
            USING bounds
            INTO sortedbounds;
	ELSE
		RAISE EXCEPTION 'unknown partition strategy %',((bounds[1]).keyspec).strategy;
	END IF;

	RAISE DEBUG 'admin.sort_partition_bounds sortedbounds %',sortedbounds;

	RETURN sortedbounds;
END
$$;

CREATE FUNCTION admin.sort_partitions(parts admin.partition[])
RETURNS admin.partition[]
LANGUAGE plpgsql
AS
$$
DECLARE
	sortedparts	admin.partition[];

BEGIN
	IF ( ((parts[1].partbound).keyspec).strategy = 'range' )
	THEN
		EXECUTE format('SELECT pg_catalog.array_agg(bnd::admin.partition) FROM (
							SELECT * FROM unnest($1) AS p ORDER BY (((p.partbound).rangebound)[1]).fromb::%s,
																   (((p.partbound).rangebound)[1]).tob::%s
							) AS bnd',
						(((parts[1]).partbound).keyspec).coltypes[1], (((parts[1]).partbound).keyspec).coltypes[1])
			USING parts
			INTO sortedparts;
	ELSIF ( ((parts[1].partbound).keyspec).strategy = 'list' )
	THEN
		EXECUTE format('SELECT pg_catalog.array_agg(bnd::admin.partition) FROM (
							SELECT * FROM unnest($1) AS p ORDER BY ((p.partbound).listbound).elems::%s[]
							) AS bnd',
						(((parts[1]).partbound).keyspec).coltypes[1])
			USING parts
			INTO sortedparts;
	ELSIF ( ((parts[1].partbound).keyspec).strategy = 'hash' )
    THEN
		EXECUTE 'SELECT pg_catalog.array_agg(bnd::admin.partition) FROM (
					SELECT * FROM unnest($1) AS p ORDER BY ((p.partbound).hashbound).modul,
														   ((p.partbound).hashbound).remain
					) AS bnd'
			USING parts
			INTO sortedparts;
	ELSE
		RAISE EXCEPTION 'unknow partition strategy %',((parts[1].partbound).keyspec).strategy;
	END IF;

	RETURN sortedparts;
END
$$;

