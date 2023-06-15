--
-- automatic bound operations
--

\i partition_opebounds.sql

CREATE FUNCTION admin.generate_hash_partition_bounds(modulus INT)
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

CREATE FUNCTION admin.generate_list_partition_bounds(VARIADIC list ANYCOMPATIBLEARRAY)
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

CREATE FUNCTION admin.generate_range_partition_bounds(boundrange ANYELEMENT, center ANYELEMENT, back INT=0, ahead INT=0)
RETURNS admin.range_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	iter		INT;
	typ			pg_catalog.pg_type;
	upperb		BIGINT;
	lowerb		BIGINT;
	bound		admin.range_bound;
	bounds		admin.range_bound[];

BEGIN
	typ = pg_catalog.pg_typeof(center);
	
	-- bounds ahead
	lowerb = center::BIGINT;
	upperb = lowerb + boundrange::BIGINT;
	FOR iter IN 0..ahead
	LOOP
		RAISE DEBUG 'admin.generate_range_partition_bounds(%::%,%::%)',lowerb,typ,upperb,typ;
		EXECUTE format('admin.make_range_bound(lowerb::%$1s,upperb::%$1s)',typ) INTO bound;
		bounds=pg_catalog.array_append(bounds,bound);
		lowerb=upperb;
		upperb=lowerb + boundrange::BIGINT;
	END LOOP;

	-- bounds back
	upperb = center::BIGINT;
	lowerb = upperb - boundrange::BIGINT;
	FOR iter IN 0..back
	LOOP
		EXECUTE format('admin.make_range_bound(lowerb::%$1I,upperb::%$1I)',typ) INTO bound;
		bounds=pg_catalog.array_append(bounds,bound);
		upperb=lowerb;
		lowerb=upperb - boundrange::BIGINT;
	END LOOP;

	RETURN bounds;
END
$$;

