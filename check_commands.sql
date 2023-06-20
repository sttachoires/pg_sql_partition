SET client_min_messages = notice;
\pset pager off
\set ON_ERROR_STOP on
\i partition_commands.sql

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));

SET client_min_messages = NOTICE;
SELECT 'admin.alter_table_partition_by',*
FROM admin.alter_table_partition_by('public.tb'::TEXT,'RANGE(region_id,stamp)'::TEXT);
SET client_min_messages = notice;

SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SELECT * FROM public.tb;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SET client_min_messages = debug;
SELECT 'admin.alter_table_partition_by',*
FROM admin.alter_table_partition_by('public.tb','RANGE(region_id)');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SET client_min_messages = debug;
SELECT 'admin.alter_table_partition_by',*
FROM admin.alter_table_partition_by('public.tb','HASH(stamp)');

SET client_min_messages = notice;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SELECT * FROM public.tb;

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
SELECT 'admin.alter_table_merge_partitions',*
FROM admin.alter_table_merge_partitions('public.tb','public.tb_1', 'public.tb_2', 'public.tb_7');
SET client_min_messages = notice;

SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.alter_table_merge_partitions',*
FROM admin.alter_table_merge_partitions('public.tb','public.tb_4', 'public.tb_d');
SET client_min_messages = notice;

INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SELECT * FROM public.tb;

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
SELECT 'admin.alter_table_merge_partitions',*
FROM admin.alter_table_merge_partitions('public.tb','public.tb_1', 'public.tb_d');

SET client_min_messages = notice;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SELECT * FROM public.tb;

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
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.alter_table_merge_partitions',*
FROM admin.alter_table_merge_partitions('public.tb','public.tb_2', 'public.tb_4', 'public.tb_6', 'public.tb_8', 'public.tb_a', 'public.tb_c');

SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SET client_min_messages = notice;

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
SELECT 'admin.alter_table_unpartition',*
FROM admin.alter_table_unpartition('public.tb');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

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
SELECT 'admin.alter_table_unpartition',*
FROM admin.alter_table_unpartition('public.tb');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

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
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.alter_table_unpartition',*
FROM admin.alter_table_unpartition('public.tb');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY LIST(region_id);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.alter_table_split_partition',*
FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_1 FOR VALUES IN (1,2)','public.tb_d3 IN(3)','public.tb_4(4,5)');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY RANGE(id,region_id);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.alter_table_split_partition',*
FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_1 FOR VALUES FROM (1,1) TO (2, 4)','public.tb_5 FROM (3,4) TO (5, 7)','public.td_7(6,8)(8,9)');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table_like',*
FROM admin.create_table_like('public.tb', 'public.tb_tpl');

SET client_min_messages = notice;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table_like',*
FROM admin.create_table_like('public.tb', 'public.tb_tpl', 'Range(region_id,id,stamp)');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_table_like',*
FROM admin.create_table_like('public.tb', 'public.tb_tpl', 'lIST(region_id)');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_table_like',*
FROM admin.create_table_like('public.tb', 'public.tb_tpl', 'HASH(region_id,id,stamp)');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_table_like',*
FROM admin.create_table_like('public.tb', 'public.tb_tpl', 'HASH(region_id,id,stamp)','public.tb_1 FOR VALUES (MODULUS 2, REMAINDER 0)','public.tb_2(2,1)');

SET client_min_messages = notice;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table_like',*
FROM admin.create_table_like('public.tl', 'public.tb', 'LIST(region_id)','public.tl1 FOR VALUES (1,2,3)','public.tl2 FOR VALUES (4,5,6)','public.td DEFAULT');

SET client_min_messages = notice;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id CHAR(3));
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_automatic_table_like(public.tbh, public.tb_tpl, hash(id), modulus 2)',*
FROM admin.create_automatic_table_like('public.tbh', 'public.tb_tpl', 'hash(id)', 'modulus 2');
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_automatic_table_like(public.tbl, public.tb_tpl, list(region_id), list (AFR,AMN,ASI,EUR,OCE) with default)',*
FROM admin.create_automatic_table_like('public.tbl', 'public.tb_tpl', 'list(region_id)', 'list (AFR,AMN,ASI,EUR,OCE) with default');

SET client_min_messages = NOTICE;
SELECT 'admin.create_automatic_table_like(public.tbr1,public.tb_tpl,range(stamp),INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT)',*
FROM admin.create_automatic_table_like('public.tbr1', 'public.tb_tpl', 'range(stamp)','INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT');

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id CHAR(3));

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, hash(id), modulus 2, list(region_id), list (AFR,AMN,ASI,EUR,OCE) with default, range(stamp),INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT)',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl', 'hash(id)', 'modulus 2','list(region_id)', 'list (AFR,AMN,ASI,EUR,OCE) with default', 'range(stamp)','INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT');
SET client_min_messages = NOTICE;

SELECT 'admin.create_automatic_table_like()',*
FROM admin.create_automatic_table_like('public.tbr2', 'public.tb_tpl', 'range()','');
SELECT 'admin.check_them_all',* FROM admin.check_them_all();

