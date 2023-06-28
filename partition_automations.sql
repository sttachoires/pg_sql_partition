--
-- automatic commands for end user
--

\i partition_managers.sql

CREATE FUNCTION admin.create_automatic_table_like(tab TEXT, tpl TEXT, VARIADIC partdescs TEXT[])
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    tabname     admin.qualified_name;
    tplname     admin.qualified_name;
	newname		admin.qualified_name;
	newnames	admin.qualified_name[];
    iter        BIGINT;
    partkey     admin.partition_keyspec=NULL;
    parts       admin.partition[]=NULL;

BEGIN
    tabname=admin.string_to_qualname(tab);
    tplname=admin.string_to_qualname(tpl);
    RAISE DEBUG 'admin.create_automatic_table_like(%,%,%)',tabname,tplname,pg_catalog.array_to_string(partdescs,',');

    -- Decode partdesc
	RAISE DEBUG 'admin.create_automatic_table_like % #%',pg_catalog.array_to_string(partdescs,','),pg_catalog.array_length(partdescs,1);

	IF ((pg_catalog.array_length(partdescs,1) >= 2) AND ((pg_catalog.array_length(partdescs,1) % 2) = 0))
	THEN
		-- if not topmost partitionned table (first one, last iteration), create new name
		-- else tabname will be the top table name
		newname=tabname;
		FOR iter IN 1..pg_catalog.array_length(partdescs,1) BY 2
		LOOP
			newnames=pg_catalog.array_append(newnames,newname);
			newname=admin.generate_partition_name(newname);
		END LOOP;

	    FOR iter IN REVERSE pg_catalog.array_length(partdescs,1)..2 BY 2
	    LOOP
			RAISE DEBUG 'admin.create_automatic_table_like partdescs[%-1]=%',iter,partdescs[iter-1];
	        partkey=admin.string_to_partition_keyspec(tplname,partdescs[iter-1]);
	        IF (partkey IS NULL)
	        THEN
	            RAISE EXCEPTION 'unknow partition key description %',partdescs[iter-1];
	        END IF;
			RAISE DEBUG 'admin.create_automatic_table_like partdescs[%]=%',iter,partdescs[iter];

			newname=newnames[iter/2];
	        parts=admin.string_to_automatic_partitions(newname,partkey,partdescs[iter]);
	        IF (parts IS NULL)
	        THEN
	            RAISE EXCEPTION 'unknow automatic partition description % for partition key description %',partdescs[iter],partdescs[iter-1];
	        END IF;
	
			-- create_automatic_table_like (newname,tplname,keypecn, partspecn)
	    	PERFORM admin.create_table(newname,tplname,partkey,VARIADIC parts);

			-- if not original tpl table, drop it, i was just created to be a template of this one
			IF (tplname <> admin.string_to_qualname(tpl))
			THEN
				RAISE NOTICE'admin.create_automatic_table_like admin.drop_table(%)',tplname;
				--PERFORM admin.drop_table(tplname);
			END IF;
			-- this partionned table will be the next template
			tplname=newname;
	    END LOOP;
	ELSE
		RAISE EXCEPTION 'problem with "%" that is not automatic partition description',pg_catalog.array_to_string(partdescs,',');
	END IF;
END
$$;

