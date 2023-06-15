--
-- Complex bound operations
--

\i partition_opeautobounds.sql

CREATE FUNCTION admin.merge_list_partition_bounds(VARIADIC boundlist admin.partition_bound[])
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
    newbound	admin.partition_bound;
	bound		admin.partition_bound;
	elems		TEXT[];
	elem		TEXT[];

BEGIN
    -- If default is present in the merge, then dest will be default
	newbound.keyspec=(boundlist[1]).keyspec;
	
	FOREACH bound IN ARRAY boundlist
	LOOP
		IF (bound.isdefault)
		THEN
			newbound.isdefault=true;
			RETURN newbound;
		END IF;
		elem=((bound).listbound).elems;
		RAISE DEBUG 'admin.merge_list_partition_bounds elem %',elem;
		elems=pg_catalog.array_cat(elems,elem);
		RAISE DEBUG 'admin.merge_list_partition_bounds elems %',elems;
	END LOOP;

	newbound.listbound.subtyp=(boundlist[1]).listbound.subtyp;
	newbound.listbound.elems=admin.array_sort(elems);

	RAISE DEBUG 'admin.merge_list_partition_bounds newbound %',newbound;

	RETURN newbound;
END
$$;

CREATE FUNCTION admin.merge_range_partition_bounds(VARIADIC boundlist admin.partition_bound[])
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
	bound			admin.partition_bound;
	rbound			admin.range_bound;
	newbound		admin.partition_bound;
	bounds			admin.partition_bound[];
	boundinf		TEXT[];
	boundsup		TEXT[];
	boundtypes		TEXT[];

BEGIN
	RAISE DEBUG 'admin.merge_range_partition_bounds boundlist %',boundlist;

	-- Sort them
    bounds=admin.sort_partition_bounds(boundlist);

	RAISE DEBUG 'admin.merge_range_partition_bounds bounds %',bounds;

	-- Check bounds are connected
	-- Keep minimum and maximum bounds
	FOREACH bound IN ARRAY bounds
	LOOP
		IF (bound.isdefault)
		THEN
			RETURN admin.make_default_partition_bound(bound.keyspec);
		END IF;
	END LOOP;

	FOREACH rbound IN ARRAY bounds[1].rangebound
	LOOP
        RAISE DEBUG 'admin.merge_range_partition_bounds rbound %',rbound;
		boundinf=pg_catalog.array_append(boundinf,rbound.fromb);
		RAISE DEBUG 'admin.merge_range_partition_bounds boundinf %',boundinf;
		boundtypes=pg_catalog.array_append(boundtypes,rbound.subtyp);
        RAISE DEBUG 'admin.merge_range_partition_bounds boundtypes %',boundtypes;
	END LOOP;

	FOREACH rbound IN ARRAY bounds[pg_catalog.array_length(bounds,1)].rangebound
	LOOP
        RAISE DEBUG 'admin.merge_range_partition_bounds rbound %',rbound;
		boundsup=pg_catalog.array_append(boundsup,rbound.tob);
		RAISE DEBUG 'admin.merge_range_partition_bounds boundsup %',boundsup;
	END LOOP;

	FOR iter IN 1..pg_catalog.array_length(boundtypes,1)
	LOOP
		RAISE DEBUG 'admin.merge_range_partition_bounds iter %',iter;
		rbound.subtyp=boundtypes[iter];
		rbound.fromb=boundinf[iter];
		rbound.tob=boundsup[iter];
		RAISE DEBUG 'admin.merge_range_partition_bounds rbound %',rbound;
		newbound.rangebound=pg_catalog.array_append(newbound.rangebound,rbound);
		RAISE DEBUG 'admin.merge_range_partition_bounds newbound %',newbound;
	END LOOP;
	newbound.isdefault=false;
	newbound.keyspec=bounds[1].keyspec;

	RAISE DEBUG 'admin.merge_range_partition_bounds newbound %',newbound;
	RETURN newbound;
END
$$;

CREATE FUNCTION admin.merge_hash_partition_bounds(VARIADIC boundlist admin.partition_bound[])
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
	bound		admin.partition_bound;
	nmodulus	BIGINT=0;
	nremainder	BIGINT=0;
	gmodulus	BIGINT=0;
	bmodulus	BIGINT=0;
	bremainder	BIGINT=0;
	nbbounds	BIGINT=0;
BEGIN
	RAISE DEBUG 'admin.merge_hash_partition_bounds boundlist:%',boundlist;
	nbbounds=array_length(boundlist,1);
	RAISE DEBUG 'admin.merge_hash_partition_bounds nbbounds:%',nbbounds;

	-- all partition should have same modulus
	FOREACH bound IN ARRAY boundlist
	LOOP
		RAISE DEBUG 'admin.merge_hash_partition_bounds bound:%',bound;
		-- get modulus and reminder
		RAISE DEBUG 'admin.merge_hash_partition_bounds boundlist:%',boundlist;
		bmodulus=(bound.hashbound).modul;
		bremainder=(bound.hashbound).remain;

		-- check modulus is all the same
		IF (gmodulus = 0)
		THEN
			gmodulus=bmodulus;
			-- check modulus is a multiple of number of partition to merge
			IF ((gmodulus % nbbounds) = 0)
			THEN
				nmodulus=gmodulus/nbbounds;
			ELSE
				RAISE EXCEPTION 'modulus % cannot be merged from % partitions',nmodulus,nbbounds;
			END IF;
		ELSIF (gmodulus != bmodulus)
		THEN
			RAISE EXCEPTION 'all partitions modulus should be the same';
		END IF;
		-- check remainder
		-- remainder with new modulus should be the same for all
		IF (nremainder = 0)
		THEN
			nremainder=bremainder % nmodulus;
		ELSIF ((bremainder % nmodulus) != nremainder)
		THEN
			RAISE EXCEPTION 'partition with modulus % and remainder % cannot be merged into modulus % and remainder %.',bmodulus,bremainder,nmodulus,nremainder;
		END IF;
	END LOOP;

	RETURN admin.make_hash_partition_bound(boundlist[1].keyspec,admin.make_hash_bound(nmodulus,nremainder));
END
$$;

CREATE FUNCTION admin.merge_partition_bounds(keyspec admin.partition_keyspec, VARIADIC bounds admin.partition_bound[])
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
    mbound  admin.partition_bound;
BEGIN
    IF (keyspec.strategy = 'list')
    THEN
        mbound=admin.merge_list_partition_bounds(VARIADIC bounds);
    ELSIF (keyspec.strategy = 'range')
    THEN
        mbound=admin.merge_range_partition_bounds(VARIADIC bounds);
    ELSIF (keyspec.strategy = 'hash')
    THEN
        mbound=admin.merge_hash_partition_bounds(VARIADIC bounds);
    ELSE
        RAISE EXCEPTION 'Unknown partition strategy %',keyspec.strategy;
    END IF;
    RETURN mbound;
END
$$;

CREATE FUNCTION admin.split_list_partition_bound(frombound admin.partition_bound, tobound admin.partition_bound)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
	newbound	admin.partition_bound;
	elems		TEXT[];
	elem		TEXT;
BEGIN
	IF (frombound.isdefault)
	THEN
		RETURN admin.make_default_partition_bound(frombound.keyspec);
	ELSIF (tobound.isdefault)
	THEN
		RAISE EXCEPTION 'list cannot split to DEFAULT';
	END IF;
	elems=(frombound.listbound).elems;
	FOREACH elem IN ARRAY (tobound.listbound).elems
	LOOP
		elems=pg_catalog.array_remove(elems,elem);
	END LOOP;

	--newbound=admin.make_list_partition(frombound.keyspec,admin.make_list_bound(elems));
	RAISE DEBUG 'SELECT * FROM admin.make_list_partition_bound($1,admin.make_list_bound($2::%)', (frombound.listbound).subtyp;
	EXECUTE format('SELECT * FROM admin.make_list_partition_bound($1,admin.make_list_bound($2::%s))',
				   (frombound.listbound).subtyp)
		USING (frombound.keyspec), elems
		INTO newbound;

	RAISE DEBUG 'admin.split_list_partition_bounds newbound ''%''',newbound;
	RETURN newbound;
END
$$;

CREATE FUNCTION admin.split_range_partition_bound(frombound admin.partition_bound, tobound admin.partition_bound)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
    newbound	    admin.partition_bound;
	rbound			admin.range_bound;
	rbounds			admin.range_bound[];
	rtype			TEXT;
	rfb				TEXT;
	rtb				TEXT;
	cmd				TEXT;

BEGIN
	IF (frombound.isdefault)
    THEN
        RETURN admin.make_default_partition_bound(frombound.keyspec);
    ELSIF (tobound.isdefault)
    THEN
		RAISE EXCEPTION 'range cannot be splitted to DEFAULT';
    ELSIF (tobound = frombound)
	THEN
		RAISE EXCEPTION 'range cannot be the same';
	ELSE
		FOR iter IN 1..pg_catalog.array_length(frombound.rangebound,1)
		LOOP
			RAISE DEBUG 'admin.split_range_partition_bounds iter %',iter;
			rtype=tobound.rangebound[iter].subtyp;
			rbound=admin.make_range_bound(0,0);
			RAISE DEBUG 'admin.split_range_partition_bounds rbound %',rbound;
			rbound.subtyp=rtype;
			RAISE DEBUG 'admin.split_range_partition_bounds rbound %',rbound;
			RAISE DEBUG 'admin.split_range_partition_bounds rtype % tobound.rangebound[iter].fromb %, frombound.rangebound[iter].fromb %, tobound.rangebound[iter].tob %, frombound.rangebound[iter].tob %',rtype,tobound.rangebound[iter].fromb, frombound.rangebound[iter].fromb, tobound.rangebound[iter].tob, frombound.rangebound[iter].tob;

			cmd=format('SELECT CASE
                                WHEN (($1::%s = $2::%s) AND ($3::%s < $4::%s))
                                    THEN $3
                                WHEN (($1::%s > $2::%s) AND ($3::%s = $4::%s))
                                    THEN $2
                                ELSE
                                    NULL
                                END', rtype,rtype,rtype,rtype,rtype,rtype,rtype,rtype);

			RAISE DEBUG '%',cmd;

			EXECUTE concat(cmd)
				USING tobound.rangebound[iter].fromb, frombound.rangebound[iter].fromb, tobound.rangebound[iter].tob, frombound.rangebound[iter].tob
				INTO rfb;

			RAISE DEBUG 'admin.split_range_partition_bounds rfb %',rfb;

			cmd=format('SELECT  CASE
                                WHEN (($1::%s = $2::%s) AND ($3::%s < $4::%s))
                                    THEN $4
                                WHEN (($1::%s > $2::%s) AND ($3::%s = $4::%s))
                                    THEN $1
                                ELSE
                                    NULL
                                END',
                            rtype,rtype,rtype,rtype,rtype,rtype,rtype,rtype);

			RAISE DEBUG '%',cmd;

            EXECUTE concat(cmd)
                USING tobound.rangebound[iter].fromb, frombound.rangebound[iter].fromb, tobound.rangebound[iter].tob, frombound.rangebound[iter].tob
                INTO rtb;

            RAISE DEBUG 'admin.split_range_partition_bounds rtb %',rtb;

			rbound.fromb=rfb;
			rbound.tob=rtb;

			RAISE DEBUG 'admin.split_range_partition_bounds rbound %',rbound;
			IF (rbound IS NULL)
			THEN
				RAISE EXCEPTION 'range is not compatible';
			END IF;
			rbounds=pg_catalog.array_append(rbounds,rbound);
			RAISE DEBUG 'admin.split_range_partition_bounds rbounds %',rbounds;
		END LOOP;
		newbound=admin.make_range_partition_bound(frombound.keyspec,rbounds);
	END IF;
    RAISE DEBUG 'admin.split_range_partition_bound newbound ''%''',newbound;
    RETURN newbound;
END
$$;

CREATE FUNCTION admin.split_hash_partition_bound(srcbound admin.partition_bound, nbpart BIGINT)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
	newbound	admin.partition_bound;
	hbound		admin.hash_bound;
	newmodul	BIGINT;
	newremain	BIGINT;

BEGIN
    -- check number of split partitions is multiple or modulus of the source
    IF ((((srcbound.hashbound).modul % nbpart) != 0) AND ((nbpart % (srcbound.hashbound).modul) != 0))
    THEN
        RAISE EXCEPTION 'modulus % hash partition could not split into % partitions',(srcbound.hashbound).modul, nbpart;
    END IF;

	newmodul=(srcbound.hashbound).modul * nbpart;
	newremain=(srcbound.hashbound).remain;
	hbound=admin.make_hash_bound(newmodul,newremain);
	newbound=admin.make_hash_partition_bound(srcbound.keyspec,hbound);

	RAISE DEBUG 'admin.split_hash_partition_bound newbound %',newbound;

	RETURN newbound;
END
$$;
