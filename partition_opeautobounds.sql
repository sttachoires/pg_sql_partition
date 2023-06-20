--
-- automatic bound operations
--

\i partition_opebounds.sql

CREATE FUNCTION admin.generate_hash_bounds(modul INT)
RETURNS admin.hash_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	iter		INT;
	bound		admin.hash_bound;
    bounds		admin.hash_bound[];

BEGIN
	RAISE DEBUG 'admin.generate_hash_bounds(%)',modul;
	IF (modul < 1)
	THEN
		RAISE EXCEPTION 'Cannot generate % hash partitions',modul;
	END IF;

	FOR iter IN 0..(modul - 1)
	LOOP
		bound=admin.make_hash_bound(modul, iter);
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
	btype	TEXT;
	bound	admin.list_bound;
	bounds	admin.list_bound[];

BEGIN
	RAISE DEBUG 'admin.generate_list_bounds list %',list;

	IF (list IS NULL)
	THEN
		RETURN NULL;
	END IF;

	IF (list[1] IS NULL)
	THEN
		RETURN NULL;
	END IF;

	IF (pg_catalog.array_ndims(list) = 1)
	THEN
		btype=pg_catalog.pg_typeof(list[1]);
		RAISE DEBUG 'admin.generate_list_bounds btype %',btype;
		FOR iter IN 1..pg_catalog.array_length(list,1)
		LOOP
			RAISE DEBUG 'admin.generate_list_bounds ARRAY[list[%]]=%',iter,ARRAY[list[iter]];
			bound=admin.make_list_bound(ARRAY[list[iter]]);
			bounds=pg_catalog.array_append(bounds,bound);
		END LOOP;
	ELSIF (pg_catalog.array_ndims(list) = 2)
	THEN
		btype=pg_catalog.pg_typeof(list[1]);
		RAISE DEBUG 'admin.generate_list_bounds btype %',btype;
		FOR iter IN 1..pg_catalog.array_length(list,1)
		LOOP
			RAISE DEBUG 'admin.generate_list_bounds list[%]=%',iter,list[iter];
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
	RAISE DEBUG 'admin.generate_range_bounds interb % center % back % ahead %',interb,center,back,ahead;

	RAISE DEBUG 'admin.generate_range_bounds pg_catalog.pg_typeof(%)=%',center,pg_catalog.pg_typeof(center);
    typ=pg_catalog.pg_typeof(center);
	RAISE DEBUG 'admin.generate_range_partition_bounds typ %',typ;

    -- bounds ahead
    lowerb=center::TIMESTAMP WITH TIME ZONE;
    upperb=lowerb + interb;
    FOR iter IN 1..ahead
    LOOP
        RAISE DEBUG 'admin.generate_range_partition_bounds admin.make_range_bound(%::%,%::%)',lowerb,typ,upperb,typ;
        EXECUTE format('SELECT * FROM admin.make_range_bound($1::%s,$2::%s)',typ,typ) USING lowerb, upperb INTO bound;
        bounds=pg_catalog.array_append(bounds,bound);
        lowerb=upperb;
        upperb=lowerb + interb;
    END LOOP;

    -- bounds back
    upperb=center::TIMESTAMP WITH TIME ZONE;
    lowerb=upperb - interb;
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

CREATE FUNCTION admin.generate_range_bounds(VARIADIC boundlist ANYARRAY)
RETURNS admin.range_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
    iter        INT;
	nbbounds	INT;

    bound       admin.range_bound;
    bounds      admin.range_bound[];

BEGIN
	RAISE DEBUG 'admin.generate_range_bounds boundlist %',boundlist;

	nbbounds=pg_catalog.array_length(boundlist,1);
	RAISE DEBUG 'admin.generate_range_bounds nbbounds %',nbbounds;	
	IF (nbbounds > 1)
	THEN
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

CREATE FUNCTION admin.generate_partition_bounds(keyspec admin.partition_keyspec, modulus INT, withdefault BOOLEAN=false)
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
		RAISE EXCEPTION 'could not build % partition bounds with hash specifications',keyspec.strategy;
	END IF;

	IF (withdefault)
	THEN
		RAISE EXCEPTION 'hash partition can not have default';
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

	RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_partition_bounds(keyspec admin.partition_keyspec, boundlist ANYARRAY, withdefault BOOLEAN=true)
RETURNS admin.partition_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
    lbounds     admin.list_bound[];
    lbound      admin.list_bound;
    rbounds     admin.range_bound[];
    rbound      admin.range_bound;
    bound       admin.partition_bound;
    bounds      admin.partition_bound[];

BEGIN
    IF (keyspec.strategy = 'list')
	THEN
		lbounds=admin.generate_list_bounds(VARIADIC boundlist);

	    IF (lbounds IS NULL)
	    THEN
	        RETURN NULL;
	    END IF;
	
	    FOREACH lbound IN ARRAY lbounds
	    LOOP
	        bound=admin.make_list_partition_bound(keyspec,lbound);
	        bounds=pg_catalog.array_append(bounds,bound);
	    END LOOP;
	ELSIF (keyspec.strategy = 'range')
	THEN
		rbounds=admin.generate_range_bounds(VARIADIC boundlist);

        IF (rbounds IS NULL)
        THEN
            RETURN NULL;
        END IF;

        FOREACH rbound IN ARRAY rbounds
        LOOP
            bound=admin.make_range_partition_bound(keyspec,rbound);
            bounds=pg_catalog.array_append(bounds,bound);
        END LOOP;
	ELSE
		RAISE EXCEPTION 'unhandle automatic description for %',keyspec.strategy;
	END IF;

	IF (withdefault)
	THEN
		bound=admin.make_default_partition_bound(keyspec);
		bounds=pg_catalog.array_append(bounds,bound);
	END IF;

	RETURN bounds;
END
$$;

CREATE FUNCTION admin.generate_partition_bounds(keyspec admin.partition_keyspec, inter INTERVAL, center ANYELEMENT, back INT=0, ahead INT=0, withdefault BOOLEAN=true)
RETURNS admin.partition_bound[]
LANGUAGE plpgsql
AS
$$
DECLARE
	rbounds		admin.range_bound[];
	rbound		admin.range_bound;
	bounds		admin.partition_bound[];
	bound		admin.partition_bound;
	
BEGIN
	RAISE DEBUG 'admin.generate_partition_bounds(%,%,%,%,%,%)',keyspec,inter,center,back,ahead,withdefault;
	IF (keyspec.strategy <> 'range')
	THEN
		RAISE EXCEPTION 'Could not build % partition bounds with range specifications',keyspec.strategy;
	END IF;
	RAISE DEBUG 'admin.generate_partition_bounds admin.generate_range_bounds(%,%,%,%)',inter,center,back,ahead;
	rbounds=admin.generate_range_bounds(inter,center,back,ahead);
	IF (rbounds IS NULL)
    THEN
        RETURN NULL;
    END IF;

    FOREACH rbound IN ARRAY rbounds
    LOOP
        bound=admin.make_range_partition_bound(keyspec,ARRAY[rbound]);
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

CREATE FUNCTION admin.string_to_automatic_partitions(parentname admin.qualified_name, partkey admin.partition_keyspec, partdesc TEXT)
RETURNS admin.partition[]
LANGUAGE plpgsql
AS
$$
DECLARE
	pd			TEXT;
	pdp			TEXT;
	autostrat	admin.auto_strategy;
	modul		INT;
	wdef		BOOLEAN;
	lst			TEXT;
	bound		admin.partition_bound;
	bounds		admin.partition_bound[];
	partname	admin.qualified_name;
	part		admin.partition;
	parts		admin.partition[];

BEGIN
    RAISE DEBUG 'admin.string_to_automatic_partitions(%,%,%)',parentname,partkey,partdesc;
	
	-- decode partdesc
	-- HASH(columns),	[OVER AUTOMATIC] MODULUS	integer
	-- LIST(column),	[OVER AUTOMATIC] LIST		(list)																[WITH DEFAULT]
	-- RANGE(columns),	[OVER AUTOMATIC] INTERVAL	intval1,...	[CENTER AT]	(values)	[BACK]	integer	[AHEAD]	integer [WITH DEFAULT]
	-- RANGE(columns),	[OVER AUTOMATIC] STEPS		steps		[CENTER AT]	(values)	[BACK]	integer	[AHEAD]	integer [WITH DEFAULT]
	-- RANGE(columns),	[OVER AUTOMATIC] BOUNDS		(list1)...(listn)													[WITH DEFAULT]

	-- remove eventual OVER AUTOMATIC
	pd=trim(pg_catalog.regexp_replace(trim(partdesc),'[[:space:]]*OVER[[:space:]]*AUTOMATIC[[:space:]]*',' ','i'));
	RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;

	-- get automatic strategy (modulus,list,interval,steps,bounds)
	pdp=pg_catalog.substring(pd,'^[[:space:]]*[^[:space:]]+');
	autostrat=lower(trim(pdp));
	RAISE DEBUG 'admin.string_to_automatic_partitions autostrat %',autostrat;

	IF (partkey.strategy = 'hash')
	THEN
		-- HASH(columns),	[OVER AUTOMATIC] MODULUS	integer
		-- check and remove MODULUS keyword
		IF (autostrat <> 'modulus')
		THEN
			RAISE EXCEPTION 'hash automatic partition could not be describe as %',autostrat;
		-- ^[[:space:]]*MODULUS[[:space:]]*[[:digit:]]+
		ELSE
			pd=trim(pg_catalog.regexp_replace(pd,'[[:space:]]*'||autostrat||'[[:space:]]*',' ','i'));
			RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
		END IF;

		-- get automatic modulus params
		RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
		modul=trim(pg_catalog.substring(pd,'^[[:space:]]*[[:digit:]]+'))::INT;
		RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
		RAISE DEBUG 'admin.string_to_automatic_partitions modul %',modul;

		-- generate automatic partition_bounds
		bounds=admin.generate_partition_bounds(partkey,modul);
	ELSIF (partkey.strategy = 'list')
    THEN
		-- LIST(column),    [OVER AUTOMATIC] LIST       (list)
		-- check and remove LIST keyword
		IF (autostrat <> 'list')
        THEN
            RAISE EXCEPTION 'list automatic partition could not be describe as %',autostrat;
		ELSE
            pd=trim(pg_catalog.regexp_replace(pd,'[[:space:]]*'||autostrat||'[[:space:]]*',' ','i'));
            RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
        END IF;

		-- check and remove default specs
		--wdef=pg_catalog.regexp_like(pd,'.*WITH[[:space:]]+DEFAULT.*','i');
		IF (pd ~* 'WITH[[:space:]]+DEFAULT')
		THEN
			wdef=true;
			pd=trim(pg_catalog.regexp_replace(trim(pd),'WITH[[:space:]]+DEFAULT','i'));
			RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
		END IF;
		-- get automatic list params
		RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
        lst=trim(pg_catalog.substring(trim(pd),'^(.*)'));
		lst=trim(LEADING '(' FROM lst);
		lst=trim(TRAILING ')' FROM lst);

		-- generate automatic partition_bounds
		bounds=admin.generate_partition_bounds(partkey,pg_catalog.string_to_array(lst,','),wdef);
    ELSIF (partkey.strategy = 'range')
    THEN
		IF (autostrat = 'interval')
		THEN
			-- RANGE(columns),  [OVER AUTOMATIC] INTERVAL (intvals) [CENTER AT (values)] [BACK integer] [AHEAD integer] [WITH DEFAULT]
			-- remove INTERVAL keyword
			pd=trim(pg_catalog.regexp_replace(pd,'[[:space:]]*'||autostrat||'[[:space:]]*',' ','i'));
            RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
			-- check and remove default specs
			IF (pd ~* 'WITH[[:space:]]+DEFAULT')
	        THEN
    	        wdef=true;
        	    pd=trim(pg_catalog.regexp_replace(trim(pd),'WITH[[:space:]]+DEFAULT','i'));
            	RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;
	        END IF;
			-- get automatic intervalparams
			intervl=trim(pg_catalog.substring(trim(pd),'^(.*)'));
	        intervl=trim(LEADING '(' FROM intervl);
    	    intervl=trim(TRAILING ')' FROM intervl);
			pd=trim(pg_catalog.regexp_replace(pd,'^(.*)'));
			RAISE DEBUG 'admin.string_to_automatic_partitions pd %',pd;

			
		ELSIF (autostrat = 'steps')
		THEN
	    	-- RANGE(columns),  [OVER AUTOMATIC] STEPS      steps       [CENTER AT] (values)    [BACK]  integer [AHEAD] integer [WITH DEFAULT]
		ELSIF (autostrat = 'bounds')
        THEN
		    -- RANGE(columns),  [OVER AUTOMATIC] BOUNDS     (list1)...(listn)
		ELSE
			RAISE EXCEPTION 'range automatic partition could not be describe as %',autostrat;
		END IF;
	ELSE
		RAISE EXCEPTION 'unknown partition strategy %',partkey.strategy;
	END IF;

	-- build partitions
	RAISE DEBUG 'admin.string_to_automatic_partitions bounds %',bounds;
	IF (pg_catalog.array_length(bounds,1) > 0)
	THEN
		FOREACH bound IN ARRAY bounds
		LOOP
			RAISE DEBUG 'admin.string_to_automatic_partitions bound %',bound;
			IF (bound.isdefault)
			THEN
				partname=admin.generate_default_partition_name(parentname);
			ELSE
				partname=admin.generate_partition_name(parentname);
			END IF;
			part=admin.make_partition(partname,bound);
			RAISE DEBUG 'admin.string_to_automatic_partitions part %',part;
			parts=pg_catalog.array_append(parts,part);
		END LOOP;
	ELSE
		RETURN NULL;
	END IF;

	RAISE DEBUG 'admin.string_to_automatic_partitions parts %',parts;
	-- search default 
    RETURN parts;
END
$$;

