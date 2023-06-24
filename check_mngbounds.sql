SET client_min_messages = notice;
\pset pager off
\set ON_ERROR_STOP on
\i partition_mngbounds.sql

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
SELECT 'admin.merge_list_partition_bounds',*
FROM admin.merge_list_partition_bounds(VARIADIC admin.get_partition_bounds(ARRAY[
																	admin.make_qualname('public','tb_4'),
																	admin.make_qualname('public','tb_2'),
																	admin.make_qualname('public','tb_7')
																  ]));

SELECT 'admin.merge_list_partition_bounds',*
FROM admin.merge_list_partition_bounds(VARIADIC admin.get_partition_bounds(ARRAY[
                                                                    admin.make_qualname('public','tb_2'),
                                                                    admin.make_qualname('public','tb_d')
                                                                  ]));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT,
	PRIMARY KEY (id, region_id))
	PARTITION BY RANGE(region_id,id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES FROM (1,1) TO (2,100000);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES FROM (3,1) TO (4,100000);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES FROM (5,1) TO (7,100000);
CREATE TABLE public.tb_7 PARTITION OF public.tb FOR VALUES FROM (8,1) TO (9,100000);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SET client_min_messages = NOTICE;
SELECT 'admin.merge_range_partition_bounds',*
FROM admin.merge_range_partition_bounds(VARIADIC admin.get_partition_bounds(ARRAY[
                                                                   admin.make_qualname('public','tb_4'),
                                                                   admin.make_qualname('public','tb_2')
																   ]));
SELECT 'admin.merge_range_partition_bounds',*
FROM admin.merge_range_partition_bounds(VARIADIC admin.get_partition_bounds(ARRAY[
                                                                   admin.make_qualname('public','tb_2'),
                                                                   admin.make_qualname('public','tb_d')
                                                                   ]));
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
SELECT 'admin.merge_hash_partition_bounds',*
FROM admin.merge_hash_partition_bounds(VARIADIC admin.get_partition_bounds(ARRAY[
                                                                  admin.make_qualname('public','tb_1'),
                                                                  admin.make_qualname('public','tb_7')
                                                                  ]));
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
SELECT 'admin.merge_partition_bounds',*
FROM admin.merge_partition_bounds(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
								  VARIADIC admin.get_partition_bounds(ARRAY[
                                                                  admin.make_qualname('public','tb_1'),
                                                                  admin.make_qualname('public','tb_7')
                                                                  ]));
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
SET client_min_messages = NOTICE;
SELECT 'admin.split_list_partition_bounds',*
FROM admin.split_list_partition_bound(admin.get_partition_bound(admin.make_qualname('public','tb_7')),
                                       admin.make_list_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
																 	   admin.make_list_bound(ARRAY[8]::BIGINT[])));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT,
    PRIMARY KEY (id, region_id))
    PARTITION BY RANGE(region_id,id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES FROM (1,1) TO (2,100000);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES FROM (3,1) TO (6,100000);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES FROM (7,1) TO (9,100000);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SET client_min_messages = NOTICE;
SELECT 'admin.split_range_partition_bound',*
FROM admin.split_range_partition_bound(admin.get_partition_bound(admin.make_qualname('public','tb_2')),
										admin.make_range_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
																	     ARRAY[admin.make_range_bound(3,4),
																		 	   admin.make_range_bound(1,10000)]));
SET client_min_messages = notice;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT) PARTITION BY HASH(region_id);
CREATE TABLE public.tb_0 PARTITION OF public.tb FOR VALUES WITH (MODULUS 6,REMAINDER 0);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES WITH (MODULUS 6,REMAINDER 1);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES WITH (MODULUS 6,REMAINDER 2);
CREATE TABLE public.tb_3 PARTITION OF public.tb FOR VALUES WITH (MODULUS 6,REMAINDER 3);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES WITH (MODULUS 6,REMAINDER 4);
CREATE TABLE public.tb_5 PARTITION OF public.tb FOR VALUES WITH (MODULUS 6,REMAINDER 5);
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 50));
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.split_hash_partition_bound',*
FROM admin.split_hash_partition_bound(admin.get_partition_bound(admin.make_qualname('public','tb_1')),2);
SELECT 'admin.split_hash_partition_bound',*
FROM admin.split_hash_partition_bound(admin.get_partition_bound(admin.make_qualname('public','tb_1')),3);
SET client_min_messages = notice;

SELECT 'admin.check_them_all',* FROM admin.check_them_all();

