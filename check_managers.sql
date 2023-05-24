SET client_min_messages = notice;
\i partition_managers.sql

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT)
    PARTITION BY RANGE(region_id);
CREATE TABLE public.tb_1 PARTITION OF public.tb FOR VALUES FROM (1) TO (3);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES FROM (3) TO (7);
CREATE TABLE public.tb_4 PARTITION OF public.tb FOR VALUES FROM (7) TO (9);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SELECT * FROM admin.relations;
SET client_min_messages = NOTICE;

SELECT 'admin.split_range_partition',*
FROM admin.split_range_partition(admin.make_qualname('public','tb'),
								 admin.get_partition(admin.make_qualname('public','tb_2')),
								 admin.make_partition(admin.make_qualname('public','tb_a'),
									   						admin.make_range_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
																					   		 ARRAY[admin.make_range_bound(3,4)])));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT,
    PRIMARY KEY (id, region_id, stamp))     
    PARTITION BY RANGE(region_id,id,stamp);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES FROM (8,99,'2023-01-01'::timestamp with time zone) TO (9,100,'2023-02-01'::timestamp with time zone);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SET client_min_messages = NOTICE;

SELECT 'admin.split_range_partition',*
FROM admin.split_range_partition(admin.make_qualname('public','tb'),
                                 admin.get_partition(admin.make_qualname('public','tb_d')),
								 admin.make_partition(admin.make_qualname('public','tb_a'),
									                  admin.make_range_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
                                                                                      		  ARRAY[admin.make_range_bound(3,4),
																									admin.make_range_bound(1,10),
																									admin.make_range_bound('2023-01-01'::timestamp with time zone,
																												   '2023-02-01'::timestamp with time zone)])));

SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
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
SELECT 'admin.split_list_partition',*
FROM admin.split_list_partition(admin.make_qualname('public','tb'),
								admin.get_partition(admin.make_qualname('public','tb_7')),
								admin.make_partition(admin.make_qualname('public','tb_a'),
                                						   admin.make_list_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
                                                        						  			admin.make_list_bound(ARRAY[8]::BIGINT[]))));
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
SELECT 'admin.split_hash_partition',*
FROM admin.split_hash_partition(admin.make_qualname('public','tb'),
								admin.get_partition(admin.make_qualname('public','tb_0')),
								admin.make_partition(admin.make_qualname('public','tb_a'),
														   admin.make_hash_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
																						   admin.make_hash_bound(12,6)))
									 );
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

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
SELECT 'admin.split_hash_partition',*
FROM admin.split_hash_partition(admin.make_qualname('public','tb'),
                                admin.get_partition(admin.make_qualname('public','tb_0')),
                                admin.make_partition(admin.make_qualname('public','tb_a'),
                                                           admin.make_hash_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
                                                                                           admin.make_hash_bound(18,6))),
								admin.make_partition(admin.make_qualname('public','tb_b'),
                                                           admin.make_hash_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
                                                                                           admin.make_hash_bound(18,12)))
                                );
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT)
    PARTITION BY RANGE(region_id);
CREATE TABLE public.tb_2 PARTITION OF public.tb FOR VALUES FROM (3) TO (9);
CREATE TABLE public.tb_d PARTITION OF public.tb DEFAULT;
INSERT INTO public.tb (region_id) VALUES (generate_series(1, 10));
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.split_partition',*
FROM admin.split_partition(admin.make_qualname('public','tb'),
                                 admin.get_partition(admin.make_qualname('public','tb_2')),
                                 admin.make_partition(admin.make_qualname('public','tb_a'),
                                                            admin.make_range_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
                                                                                             ARRAY[admin.make_range_bound(3,4)])));

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
SELECT 'admin.merge_list_partitions',*
FROM admin.merge_list_partitions(admin.make_qualname('public','tb'),
                                 admin.get_partition(admin.make_qualname('public','tb_4')),
                                 VARIADIC admin.get_partitions(admin.make_qualname('public','tb_2'),admin.make_qualname('public','tb_7')));

SELECT 'admin.merge_list_partitions',*
FROM admin.merge_list_partitions(admin.make_qualname('public','tb'),
								 admin.get_partition(admin.make_qualname('public','tb_2')),
								 VARIADIC admin.get_partitions(admin.make_qualname('public','tb_d')));
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
SELECT 'admin.merge_range_partitions',*
FROM admin.merge_range_partitions(admin.make_qualname('public','tb'),
                                  admin.get_partition(admin.make_qualname('public','tb_4')),
                                  admin.get_partition(admin.make_qualname('public','tb_2')));
SELECT 'admin.merge_range_partitions',*
FROM admin.merge_range_partitions(admin.make_qualname('public','tb'),
								  admin.get_partition(admin.make_qualname('public','tb_4')),
                                  VARIADIC admin.get_partitions(admin.make_qualname('public','tb_7'), admin.make_qualname('public','tb_d')));
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
SELECT 'admin.merge_hash_partitions',*
FROM admin.merge_hash_partitions(admin.make_qualname('public','tb'),
								 admin.get_partition(admin.make_qualname('public','tb_1')),
								 VARIADIC admin.get_partitions(admin.make_qualname('public','tb_7')));
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
SELECT 'admin.merge_partitions',*
FROM admin.merge_partitions(admin.make_qualname('public','tb'),
							admin.get_partition(admin.make_qualname('public','tb_1')),
							VARIADIC admin.get_partitions(admin.make_qualname('public','tb_3'),
												 admin.make_qualname('public','tb_5'),
                                                 admin.make_qualname('public','tb_7'),
                                                 admin.make_qualname('public','tb_9'),
                                                 admin.make_qualname('public','tb_b')));
SET client_min_messages = notice;

SELECT 'admin.check_them_all',* FROM admin.check_them_all();

