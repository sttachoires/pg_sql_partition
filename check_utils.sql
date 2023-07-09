--
-- Regression test for partition_utils.sql
--

SET client_min_messages=notice;
\set ON_ERROR_STOP on
\pset pager off
\i partition_utils.sql

SELECT 'admin.array_sort',*
FROM admin.array_sort(ARRAY[0,100,'42']);

SELECT 'admin.array_sort',*
FROM admin.array_sort(ARRAY['2023-01-01','2023-10-02','2023-03-04']);

SELECT 'admin.make_qualname',*
FROM admin.make_qualname('pub lic','tb 4');

SELECT 'admin.qualname_to_string',*
FROM admin.qualname_to_string(admin.make_qualname('pub-lic','tb-4'));

SELECT 'admin.string_to_qualname',*
FROM admin.string_to_qualname('"pub lic"."tb 4"');

SELECT 'admin.string_to_qualname',*
FROM admin.string_to_qualname('admin.tbi');

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY LIST(region_id);

SELECT 'admin.table_exists',*
FROM admin.table_exists(admin.make_qualname('public','tb'));

SELECT 'admin.column_exists',*
FROM admin.column_exists(admin.make_qualname('public','tb'),'region_id');

SELECT 'admin.get_column_info',*
FROM admin.get_column_info(admin.make_qualname('public','tb'));
SELECT 'admin.get_column_info',*
FROM admin.get_column_info(admin.make_qualname('public','tb'),VARIADIC ARRAY['region_id','id']::TEXT[]);

SELECT 'admin.rowcount',*
FROM admin.rowcount(admin.make_qualname('public','tb'),true);

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY LIST(region_id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES IN (1);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES IN (2,3);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES IN (4,5,6);
CREATE TABLE public.tb_7 PARTITION OF public.tb FOR VALUES IN (7,8,9);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SET client_min_messages = NOTICE;
SELECT 'admin.get_partition_parent',*
FROM admin.get_partition_parent(admin.string_to_qualname('public.tb_1'));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY RANGE(region_id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES FROM (1) TO (2);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES FROM (2) TO (4);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES FROM (4) TO (7);
CREATE TABLE public.tb_7 PARTITION OF public.tb FOR VALUES FROM (7) TO (9);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SET client_min_messages = NOTICE;
SELECT 'admin.get_partition_parent',*
FROM admin.get_partition_parent(admin.string_to_qualname('public.tb_1'));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 0);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 1);
CREATE TABLE public.tb_3 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 2);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 3);
CREATE TABLE public.tb_5 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 4);
CREATE TABLE public.tb_6 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 5);
CREATE TABLE public.tb_7 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 6);
CREATE TABLE public.tb_8 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 7);
CREATE TABLE public.tb_9 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 8);
CREATE TABLE public.tb_a PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 9);
CREATE TABLE public.tb_b PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 10);
CREATE TABLE public.tb_c PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 11);
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 50));
SET client_min_messages = NOTICE;
SELECT 'admin.get_partition_parent',*
FROM admin.get_partition_parent(admin.string_to_qualname('public.tb_1'));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 0);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 1);
CREATE TABLE public.tb_3 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 2);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 3);
CREATE TABLE public.tb_5 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 4);
CREATE TABLE public.tb_6 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 5);
CREATE TABLE public.tb_7 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 6);
CREATE TABLE public.tb_8 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 7);
CREATE TABLE public.tb_9 PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 8);
CREATE TABLE public.tb_a PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 9);
CREATE TABLE public.tb_b PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 10);
CREATE TABLE public.tb_c PARTITION OF public.tb FOR VALUES WITH (MODULUS 12 ,REMAINDER 11);
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 50));
SET client_min_messages = NOTICE;
SELECT 'admin.drop_table_default_values',*
FROM admin.drop_table_default_values(admin.make_qualname('public','tb_8'));

SELECT 'admin.drop_table_constraints',*
FROM admin.drop_table_constraints(admin.make_qualname('public','tb_7'));

SELECT 'admin.drop_table',*
FROM admin.drop_table(admin.make_qualname('public','tb_b'));

SET client_min_messages = notice;

SELECT 'admin.generate_table_name',*
FROM admin.generate_table_name('public');

SELECT 'admin.generate_table_name',*
FROM admin.generate_table_name('public','thisisalongprefix');

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_1 (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_d (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_partition (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_default (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);

SET client_min_messages = NOTICE;
SELECT 'admin.generate_partition_name',*
FROM admin.generate_partition_name(admin.make_qualname('public','tb'));

SET client_min_messages = NOTICE;
SELECT 'admin.generate_partition_name',*
FROM admin.generate_partition_name(admin.make_qualname('public','tb'),'_partition');

SET client_min_messages = NOTICE;
SELECT 'admin.generate_default_partition_name',*
FROM admin.generate_default_partition_name(admin.make_qualname('public','tb'));

SET client_min_messages = NOTICE;
SELECT 'admin.generate_default_partition_name',*
FROM admin.generate_default_partition_name(admin.make_qualname('public','tb'),'_default');

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
SELECT * FROM admin.relations;
SET client_min_messages = NOTICE;
SELECT 'admin.rename_table',*
FROM admin.rename_table(admin.make_qualname('public','tb'),admin.make_qualname('public','renamed_tb'));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.t1 (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
CREATE TABLE public.t2 (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
INSERT INTO public.t1 (region_id) VALUES (generate_series(1, 10));
INSERT INTO public.t2 (region_id) VALUES (generate_series(1, 10));

SELECT admin.rowcount(admin.make_qualname('public','t1')),admin.rowcount(admin.make_qualname('public','t2'));

SELECT 'admin.copy_rows',*
FROM admin.copy_rows(admin.make_qualname('public','t1'),admin.make_qualname('public','t2'),'region_id <= 5');

SELECT admin.rowcount(admin.make_qualname('public','t1')),admin.rowcount(admin.make_qualname('public','t2'));

SELECT 'admin.copy_rows',*
FROM admin.copy_rows(admin.make_qualname('public','t1'),admin.make_qualname('public','t2'));

SELECT admin.rowcount(admin.make_qualname('public','t1')),admin.rowcount(admin.make_qualname('public','t2'));

SELECT 'admin.move_rows',*
FROM admin.move_rows(admin.make_qualname('public','t2'),admin.make_qualname('public','t1'),'region_id <= 5');

SELECT admin.rowcount(admin.make_qualname('public','t1')),admin.rowcount(admin.make_qualname('public','t2'));

SELECT 'admin.move_rows',*
FROM admin.move_rows(admin.make_qualname('public','t1'),admin.make_qualname('public','t2'));

SELECT admin.rowcount(admin.make_qualname('public','t1')),admin.rowcount(admin.make_qualname('public','t2'));

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT,
    PRIMARY KEY (id));
CREATE TABLE public.regtb1 (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, tb_id BIGINT REFERENCES public.tb(id));
CREATE TABLE public.regtb2 (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, tb_id BIGINT REFERENCES public.tb(id));
CREATE TABLE public.tbp (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT,
    PRIMARY KEY (id))
    PARTITION BY HASH(id);

SET client_min_messages = NOTICE;
SELECT 'admin.swap_tables',*
FROM admin.swap_tables(admin.make_qualname('public','tb'),admin.make_qualname('public','tbp'));

SET client_min_messages = notice;
SELECT 'admin.check_them_all',*
FROM admin.check_them_all();

