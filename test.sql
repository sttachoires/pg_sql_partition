--
-- Init schema for bench check
--

SET client_min_messages = error;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

CREATE TABLE public.tb (
	id				BIGSERIAL NOT NULL,
	expire			DATE,
	closed			BOOLEAN,
	regionid		VARCHAR(6)
);

CREATE FUNCTION public.fill_tb(nb INT, clos BOOLEAN=TRUE)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
	iter		INT;
	reg			VARCHAR(6);
	reglst		TEXT[]='{EUR,ASI,AFR,AMN,AMS}';
	sreg		TEXT;
	dt			DATE;
	intrvl		TEXT;
	rndm		TEXT;

BEGIN
	FOR iter IN 1..nb
	LOOP
	RAISE NOTICE 'fill %',iter;
		RAISE DEBUG 'public.fill_tb nb %',nb;
		intrvl=concat((iter % 12),' month');
		RAISE DEBUG 'public.fill_tb intrvl %',intrvl;
		dt=(current_date - intrvl::INTERVAL)::DATE;
		RAISE DEBUG 'public.fill_tb dt %',dt;
		FOREACH sreg IN ARRAY reglst
		LOOP
			rndm=substr(md5(random()::text),1,3);
			reg=format('%3s%3s',sreg,rndm);
			RAISE DEBUG 'public.fill_tb reg %',reg;
			INSERT INTO public.tb (expire,closed,regionid) VALUES (dt, clos, reg);
		END LOOP;
	END LOOP;
END
$$;

SET client_min_messages = NOTICE;
SELECT * FROM public.fill_tb(1000000,false);

SELECT * FROM admin.partitions;
SELECT * FROM pg_sleep(2);

--SELECT public.fill_tb(1000,false) FROM pg_catalog.generate_series(1,100000);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);
SELECT * FROM public.fill_tb(10000,true);

\quit

SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(expire)');
SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_1 FOR VALUES FROM (''2023-03-01'') TO (''2023-04-01'')');
SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_1 FOR VALUES FROM (''2023-03-01'') TO (''2023-04-01'')','public.tb_4 FOR VALUES FROM (''2023-04-01'') TO (''2023-05-01'')','public.tb_z FOR VALUES FROM (''2023-06-01'') TO (''2023-07-01'')');
\quit
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(comptype)');

\quit
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(comptype)');
