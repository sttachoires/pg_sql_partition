--
-- String partition operations
--
\i partition_types.sql


CREATE FUNCTION admin.string_to_partition_keyspec(tabname admin.qualified_name, stringkeydef TEXT)
RETURNS admin.partition_keyspec
LANGUAGE plpgsql
AS
$$
DECLARE
	readkeydef	TEXT[];
	strategy    admin.partition_strategy;
	col         TEXT;
	partcol     TEXT[];
	coltype		TEXT;
	coltypes	TEXT[];
	keyspec     admin.partition_keyspec;

BEGIN
	RAISE DEBUG 'admin.string_to_partition_keyspec stringkeydef %',stringkeydef;
    readkeydef=regexp_match(stringkeydef,'([^[:space:]]+)[[:space:]]*\(([^)]+)\)[[:space:]]*');
	RAISE DEBUG 'admin.string_to_partition_keyspec readkeydef %',readkeydef;

    strategy=lower(trim(BOTH readkeydef[1]))::admin.partition_strategy;
    FOREACH col IN ARRAY string_to_array(readkeydef[2],',')
    LOOP
        col=trim(BOTH col);
        partcol=array_append(partcol,col);
		IF (admin.table_exists(tabname))
		THEN
			SELECT * FROM admin.get_column_info(tabname,col) INTO col,coltype;
			coltypes=array_append(coltypes,coltype);
		END IF;
    END LOOP;
	keyspec.strategy=strategy;
	keyspec.colnames=partcol;
	keyspec.coltypes=coltypes;

	RAISE DEBUG 'admin.get_keyspec keyspec %',keyspec;

    RETURN keyspec;
END
$$;

CREATE FUNCTION admin.partition_keyspec_to_string(pkeyspec admin.partition_keyspec)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
	stringpartspec	TEXT;

BEGIN
	RAISE DEBUG 'admin.partition_keyspec_to_string pkeyspec %',pkeyspec;

	IF (pkeyspec IS NULL)
	THEN
		RETURN NULL;
	END IF;

	stringpartspec=format('%s(%s)',pkeyspec.strategy,pg_catalog.array_to_string(pkeyspec.colnames,','));
	RAISE DEBUG 'admin.partition_keyspec_to_string stringpartspec %',stringpartspec;

	RETURN stringpartspec;
END
$$;

CREATE FUNCTION admin.string_to_range_partition_bound(pkeyspec admin.partition_keyspec, stringbound TEXT)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS $$
DECLARE
    sbs		    TEXT[];
	sbmin		TEXT[];
	sbmax		TEXT[];
	sb			TEXT;
	nbbounds	BIGINT;
	rbound		admin.range_bound;
	rbounds		admin.range_bound[];
	bound		admin.partition_bound;

BEGIN
	bound.isdefault=false;
    bound.keyspec=pkeyspec;
	RAISE DEBUG 'admin.string_to_range_partition_bound bound %',bound;

    sbs=pg_catalog.regexp_match(stringbound,'\((.*)\)[^\(]*\((.*)\)');
	RAISE DEBUG 'admin.string_to_range_partition_bound sbs %',sbs;

	FOREACH sb IN ARRAY pg_catalog.string_to_array(sbs[1],',')
	LOOP
		sbmin=pg_catalog.array_append(sbmin,trim(BOTH FROM sb));
	END LOOP;
	FOREACH sb IN ARRAY pg_catalog.string_to_array(sbs[2],',')    LOOP
        sbmax=pg_catalog.array_append(sbmax,trim(BOTH FROM sb));
    END LOOP;
	RAISE DEBUG 'admin.string_to_range_partition_bound sbmin %',sbmin;
	RAISE DEBUG 'admin.string_to_range_partition_bound sbmax %',sbmax;
	nbbounds=pg_catalog.array_length(sbmin,1);

	FOR iter IN 1..nbbounds
	LOOP
		RAISE DEBUG 'SELECT admin.make_range_bound(%::%,%::%)',sbmin[iter],pkeyspec.coltypes[iter],sbmax[iter],pkeyspec.coltypes[iter];
		EXECUTE format('SELECT * FROM admin.make_range_bound(%s::%s,%s::%s)',trim(BOTH FROM sbmin[iter]),pkeyspec.coltypes[iter],trim(BOTH FROM sbmax[iter]),pkeyspec.coltypes[iter]) INTO rbound;
		--rbound=admin.make_range_bound(sbmin[iter],sbmax[iter]);
		RAISE DEBUG 'admin.string_to_range_partition_bound rbound %',rbound;
		rbounds=pg_catalog.array_append(rbounds,rbound);
	END LOOP;

	bound.rangebound=rbounds;
    RETURN bound;
END
$$;

CREATE FUNCTION admin.range_partition_bound_to_string(bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
	minbound	TEXT='';
	maxbound	TEXT='';
	sbound		TEXT;
	rbound		admin.range_bound;
	iter		BIGINT;

BEGIN
	RAISE DEBUG 'admin.range_partition_bound_to_string bound ''%''',bound;

	IF (bound.isdefault)
	THEN
		RETURN 'DEFAULT';
	ELSE
		iter=1;
		rbound=bound.rangebound[iter];
		RAISE DEBUG 'admin.range_partition_bound_to_string rbound ''%''',rbound;

		FOR iter IN 1..array_length(bound.rangebound,1)
		LOOP
			rbound=bound.rangebound[iter];
			minbound=concat(minbound,',''',(rbound).fromb,'''');
			maxbound=concat(maxbound,',''',(rbound).tob,'''');
			RAISE DEBUG 'admin.range_partition_bound_to_string minbound %',minbound;
			RAISE DEBUG 'admin.range_partition_bound_to_string maxbound %',maxbound;
		END LOOP;
		RAISE DEBUG 'admin.range_partition_bound_to_string minbound %',minbound;
		RAISE DEBUG 'admin.range_partition_bound_to_string maxbound %',maxbound;
		sbound=concat('FROM (',trim(LEADING ',' FROM minbound),') TO (',trim(LEADING ',' FROM maxbound),')');
		RAISE DEBUG 'admin.range_bound_to_string sbound ''%''',sbound;
		RETURN sbound;
	END IF;
END
$$;

CREATE FUNCTION admin.string_to_list_partition_bound(pkeyspec admin.partition_keyspec, stringbound TEXT)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
    bs          TEXT;
	bound		admin.partition_bound;
	cmd			TEXT;
BEGIN
	bound.isdefault=false;

	RAISE DEBUG 'admin.string_to_list_partition_bound bound %',bound;

	bound.keyspec=pkeyspec;

	RAISE DEBUG 'admin.string_to_list_partition_bound bound %',bound;

    bs=trim(BOTH replace(stringbound,'IN',''));
	RAISE DEBUG 'admin.string_to_list_partition_bound bs %',bs;
	IF (pg_catalog.starts_with(bs,'('))
	THEN
		RAISE DEBUG 'admin.string_to_list_partition_bound bs %',bs;
		bs=pg_catalog.substring(bs,2,length(bs)-2); -- remove leading and trailing parenthesis
	END IF;

	RAISE DEBUG 'admin.string_to_list_partition_bound bs %',bs;

	bs=pg_catalog.regexp_replace(bs,'[[:space:]]*,[[:space:]]*',',','g');

	RAISE DEBUG 'admin.string_to_list_partition_bound bs %',bs;

	--cmd=format('SELECT * FROM admin.make_list_bound((%s)::%s)',bs,(pkeyspec).coltypes[1]);
	IF ((pkeyspec.coltypes[1] <> '' ) IS NOT TRUE)
	THEN
		pkeyspec.coltypes[1]=pg_catalog.pg_typeof((pg_catalog.string_to_array(bs,','))[1]);
	END IF;
	cmd=format('SELECT admin.make_list_bound(ARRAY[%s]::%s[])',bs,(pkeyspec).coltypes[1]);
	RAISE DEBUG 'admin.string_to_list_partition_bound cmd %',cmd;
	EXECUTE cmd INTO bound.listbound;

	RETURN bound;
END
$$;

CREATE FUNCTION admin.list_partition_bound_to_string(bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
DECLARE
	sbound	TEXT;
	cmd		TEXT;

BEGIN
	RAISE DEBUG 'admin.list_partition_bound_to_string bound %',bound;

	IF (bound.isdefault)
	THEN
		RETURN 'default';
	ELSE
		cmd=format('SELECT pg_catalog.array_to_string(''%s''::%s,'','')',(bound.listbound).elems,(bound.listbound).subtyp);
	
		RAISE DEBUG 'admin.list_partition_bound_to_string cmd %',cmd;
	
		EXECUTE cmd INTO sbound;
	
		RAISE DEBUG 'admin.list_partition_bound_to_string sbound %',sbound;
	
		sbound=format('in (%L)',sbound);
	
		RAISE DEBUG 'admin.list_partition_bound_to_string sbound %',sbound;
	
		RETURN sbound;
	END IF;
END
$$;

CREATE FUNCTION admin.string_to_hash_partition_bound(pkeyspec admin.partition_keyspec, stringbound TEXT)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
    bs          TEXT;
	sbounds		TEXT[];
    bound       admin.partition_bound;
BEGIN
	bound.isdefault=false;
	bound.keyspec=pkeyspec;

	RAISE DEBUG 'admin.string_to_hash_partition_bound stringbound %',stringbound;

	bs=trim(BOTH pg_catalog.replace(stringbound,'WITH',''));
	RAISE DEBUG 'admin.string_to_hash_partition_bound bs %',bs;
	bs=regexp_replace(bs,'(modulus|remainder)','','ig');
	RAISE DEBUG 'admin.string_to_hash_partition_bound bs %',bs;
	bs=regexp_replace(bs,'^\([[:space:]]*','(','g');
	bs=regexp_replace(bs,'[[:space:]]*\)$',')','g');
	bs=regexp_replace(bs,'[[:space:]]*,[[:space:]]*',',','g');
	RAISE DEBUG 'admin.string_to_hash_partition_bound bs %',bs;

	sbounds=pg_catalog.regexp_match(bs,'\([[:space:]]*([[:digit:]]+)[[:space:]]*,[[:space:]]*([[:digit:]]+)\)','i');
	RAISE DEBUG 'admin.string_to_hash_partition_bound sbounds %',sbounds;
	bound.hashbound=admin.make_hash_bound(sbounds[1]::BIGINT,sbounds[2]::BIGINT);

	RAISE DEBUG 'admin.string_to_hash_bound bound:%',bound;
	RETURN bound;
END
$$;

CREATE FUNCTION admin.hash_partition_bound_to_string(bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS
$$
BEGIN
	IF (bound.isdefault)
	THEN
		RETURN 'WITH (MODULUS 1, REMAINDER 0)';
	ELSE
		RETURN concat('WITH (MODULUS ',(bound.hashbound).modul,', REMAINDER ',(bound.hashbound).remain,')');
	END IF;
END
$$;

CREATE FUNCTION admin.string_to_partition_bound(pkeyspec admin.partition_keyspec, stringpartbound TEXT)
RETURNS admin.partition_bound
LANGUAGE plpgsql
AS
$$
DECLARE
	stringbound	TEXT;
	bound		admin.partition_bound;

BEGIN
	RAISE DEBUG 'admin.string_to_partition partstrat ''%''',pkeyspec;
	RAISE DEBUG 'admin.string_to_partition stringpartbound ''%''',stringpartbound;

    stringbound=trim(BOTH replace(stringpartbound,'FOR VALUES ',''));
	RAISE DEBUG 'admin.string_to_partition stringbound: %',stringbound;

    IF (pg_catalog.starts_with(stringbound,'DEFAULT'))
	THEN
		bound=admin.make_default_partition_bound(pkeyspec);
	ELSIF (pkeyspec.strategy = 'list')
	THEN
		bound=admin.string_to_list_partition_bound(pkeyspec,stringbound);
	ELSIF (pkeyspec.strategy = 'range')
	THEN
		bound=admin.string_to_range_partition_bound(pkeyspec,stringbound);
	ELSIF (pkeyspec.strategy = 'hash')
	THEN
		bound=admin.string_to_hash_partition_bound(pkeyspec,stringbound);
	ELSE
		RAISE EXCEPTION 'unkown partition bound description "%"',bound;
	END IF;

	RAISE DEBUG 'admin.string_to_partition bound: %',bound;
	RETURN bound;
END
$$;

CREATE FUNCTION admin.partition_bound_to_string(bound admin.partition_bound)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
	stringbound	TEXT;

BEGIN
	RAISE DEBUG 'admin.partition_bound_to_string bound %',bound;
	IF (bound.isdefault)
	THEN
		IF ((bound.keyspec).strategy = 'hash')
		THEN
			stringbound='for values '||admin.hash_partition_bound_to_string(bound);
		ELSE
			stringbound='default';
		END IF;
	ELSIF ((bound.keyspec).strategy = 'hash')
	THEN
		stringbound='for values '||admin.hash_partition_bound_to_string(bound);
	ELSIF ((bound.keyspec).strategy = 'range')
    THEN
		stringbound='for values '||admin.range_partition_bound_to_string(bound);
    ELSIF ((bound.keyspec).strategy = 'list')
    THEN
		stringbound='for values '||admin.list_partition_bound_to_string(bound);
    ELSE
		RAISE EXCEPTION 'unknown partition strategy %',(bound.keyspec).strategy;
	END IF;

	RAISE DEBUG 'admin.partition_bound_to_string stringbound %',stringbound;
	RETURN stringbound;
END
$$;

CREATE FUNCTION admin.string_to_partition(pkeyspec admin.partition_keyspec, tpartspec TEXT)
RETURNS admin.partition
LANGUAGE plpgsql
AS
$$
DECLARE
    ps    		TEXT[];
	partspec	admin.partition;

BEGIN
	RAISE DEBUG 'admin.string_to_partition_spec tpartspec ''%''',tpartspec;
	ps=pg_catalog.regexp_match(tpartspec,'^[[:space:]]*([^[:space:]\(]+)[[:space:]]*(.+)', 'i');

	RAISE DEBUG 'admin.string_to_partition_spec ps ''%''',ps[1];
	partspec.partname=admin.string_to_qualname(trim(BOTH FROM ps[1]));

	RAISE DEBUG 'admin.string_to_partition_spec ps ''%''',ps[2];
	partspec.partbound=admin.string_to_partition_bound(pkeyspec, ps[2]);

    RETURN partspec;
END
$$;
