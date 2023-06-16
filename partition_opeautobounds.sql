--
-- automatic bound operations
--

\i partition_opebounds.sql

CREATE FUNCTION admin.generate_hash_bounds(modulus INT)
RETURNS admin.hash_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	iter		INT;
	bound		admin.hash_bound;
    bounds		admin.hash_bound[];

BEGIN
	IF (modulus < 1)
	THEN
		RETURN NULL;
	END IF;

	FOR iter IN 0..(modulus - 1)
	LOOP
		bound=admin.make_hash_bound(modulus, iter);
		bounds=pg_catalog.array_append(bounds,bound);
	END LOOP;

    RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_list_bounds(VARIADIC list ANYCOMPATIBLEARRAY)
RETURNS admin.list_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	iter	INT;
	bound	admin.list_bound;
	bounds	admin.list_bound[];

BEGIN
	IF (list IS NULL)
	THEN
		RETURN NULL;
	END IF;

	IF (pg_catalog.array_ndims(list) = 1)
	THEN
		FOR iter IN 1..pg_catalog.array_length(list,1)
		LOOP
			bound=admin.make_list_bound(ARRAY[list[iter]]);
			bounds=pg_catalog.array_append(bounds,bound);
		END LOOP;
	ELSIF (pg_catalog.array_ndims(list) = 2)
	THEN
		FOR iter IN 1..pg_catalog.array_length(list,1)
		LOOP
			bound=admin.make_list_bound(list[iter]);
			bounds=pg_catalog.array_append(bounds,bound);
        END LOOP;
	ELSE
		RAISE EXCEPTION 'list partition should be an array or an array of array';
	END IF;

	RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_range_bounds(steps ANYELEMENT, center ANYCOMPATIBLE, back INT=0, ahead INT=0)
RETURNS admin.range_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	iter		INT;
	typ			TEXT;
	upperb		BIGINT;
	lowerb		BIGINT;
	bound		admin.range_bound;
	bounds		admin.range_bound[];

BEGIN
	typ = pg_catalog.pg_typeof(center);
	
	-- bounds ahead
	lowerb = center::BIGINT;
	upperb = lowerb + steps::BIGINT;
	FOR iter IN 1..ahead
	LOOP
		RAISE DEBUG 'admin.generate_range_partition_bounds admin.make_range_bound(%::%,%::%)',lowerb,typ,upperb,typ;
		EXECUTE format('SELECT * FROM admin.make_range_bound($1::%s,$2::%s)',typ,typ) USING lowerb, upperb INTO bound;
		bounds=pg_catalog.array_append(bounds,bound);
		lowerb=upperb;
		upperb=lowerb + steps::BIGINT;
	END LOOP;

	-- bounds back
	upperb = center::BIGINT;
	lowerb = upperb - steps::BIGINT;
	FOR iter IN 1..back
	LOOP
		RAISE DEBUG 'admin.generate_range_partition_bounds admin.make_range_bound(%::%,%::%)',lowerb,typ,upperb,typ;
		EXECUTE format('SELECT * FROM admin.make_range_bound($1::%s,$2::%s)',typ,typ) USING lowerb,upperb INTO bound;
		bounds=pg_catalog.array_append(bounds,bound);
		upperb=lowerb;
		lowerb=upperb - steps::BIGINT;
	END LOOP;

	RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_range_bounds(interb INTERVAL, center ANYELEMENT, back INT=0, ahead INT=0)
RETURNS admin.range_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
    iter        INT;
    typ         TEXT;
    upperb      TIMESTAMP WITH TIME ZONE;
    lowerb      TIMESTAMP WITH TIME ZONE;
    bound       admin.range_bound;
    bounds      admin.range_bound[];

BEGIN
    typ = pg_catalog.pg_typeof(center);

    -- bounds ahead
    lowerb = center::TIMESTAMP WITH TIME ZONE;
    upperb = lowerb + interb;
    FOR iter IN 1..ahead
    LOOP
        RAISE DEBUG 'admin.generate_range_partition_bounds admin.make_range_bound(%::%,%::%)',lowerb,typ,upperb,typ;
        EXECUTE format('SELECT * FROM admin.make_range_bound($1::%s,$2::%s)',typ,typ) USING lowerb, upperb INTO bound;
        bounds=pg_catalog.array_append(bounds,bound);
        lowerb=upperb;
        upperb=lowerb + interb;
    END LOOP;

    -- bounds back
    upperb = center::TIMESTAMP WITH TIME ZONE;
    lowerb = upperb - interb;
    FOR iter IN 1..back
    LOOP
        RAISE DEBUG 'admin.generate_range_partition_bounds admin.make_range_bound(%::%,%::%)',lowerb,typ,upperb,typ;
        EXECUTE format('SELECT * FROM admin.make_range_bound($1::%s,$2::%s)',typ,typ) USING lowerb,upperb INTO bound;
        bounds=pg_catalog.array_append(bounds,bound);
        upperb=lowerb;
        lowerb=upperb - interb;
    END LOOP;

    RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_range_bounds(boundlist ANYARRAY)
RETURNS admin.range_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
    iter        INT;
	nbbounds	INT;

    typ         TEXT;
    upperb      TIMESTAMP WITH TIME ZONE;
    lowerb      TIMESTAMP WITH TIME ZONE;
    bound       admin.range_bound;
    bounds      admin.range_bound[];

BEGIN
	nbbounds=pg_catalog.array_length(boundlist,1);
	
	IF (nbbounds >= 2)
	THEN
	    typ = pg_catalog.pg_typeof(boundlist[1]);
		FOR iter IN 1..(nbbounds-1)
		LOOP
			bound=admin.make_range_bound(boundlist[iter],boundlist[iter+1]);
			bounds=pg_catalog.array_append(bounds,bound);
		END LOOP;
	ELSE
		RAISE EXCEPTION 'it must be at least 2 bounds';
	END IF;

	RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_partition_bounds(keyspec admin.partition_keyspec, modulus INT, withdefault BOOLEAN=true)
RETURNS admin.partition_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	hbounds		admin.hash_bound[];
	hbound		admin.hash_bound;
	bound		admin.partition_bound;
	bounds		admin.partition_bound[];

BEGIN
	IF (keyspec.strategy <> 'hash')
	THEN
		RETURN NULL;
	END IF;

	hbounds=admin.generate_hash_bounds(modulus);

	IF (hbounds IS NULL)
	THEN
		RETURN NULL;
	END IF;

	FOREACH hbound IN ARRAY hbounds
	LOOP
		bound=admin.make_hash_partition_bound(keyspec,hbound);
		bounds=pg_catalog.array_append(bounds,bound);
	END LOOP;

	IF (withdefault)
	THEN
		bound=admin.make_default_partition_bound(keyspec);
		bounds=pg_catalog.array_append(bounds,bound);
	END IF;

	RETURN bounds;
END
$$;


