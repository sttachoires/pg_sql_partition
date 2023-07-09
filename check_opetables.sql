SET client_min_messages = notice;
\set ON_ERROR_STOP on
\pset pager off
\i partition_opetables.sql

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
SELECT 'admin.get_table_default_partition_name',*
FROM admin.get_table_default_partition_name(admin.string_to_qualname('tb'));
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
SELECT 'admin.get_table_default_partition_name',*
FROM admin.get_table_default_partition_name(admin.string_to_qualname('tb'));
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
SELECT * FROM admin.partitions;
SELECT 'admin.get_table_default_partition_name',*
FROM admin.get_table_default_partition_name(admin.string_to_qualname('tb'));
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
SET client_min_messages = NOTICE;
SELECT 'admin.get_table_partition_names',*
FROM admin.get_table_partition_names(admin.string_to_qualname('tb'));
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
SELECT 'admin.get_table_partition_names',*
FROM admin.get_table_partition_names(admin.string_to_qualname('tb'));
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

SELECT 'admin.get_table_partition_names',pg_catalog.unnest(tbn)
FROM admin.get_table_partition_names(admin.string_to_qualname('public.tb')) AS tbn;

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
SET client_min_messages = debug;
SELECT 'admin.get_partition_bound',*
FROM admin.get_partition_bound(admin.make_qualname('public','tb_1'));
SET client_min_messages = notice;

SELECT 'admin.get_partition_bounds',pg_catalog.unnest(tpb)
FROM admin.get_partition_bounds(ARRAY[admin.make_qualname('public'::TEXT,'tb_1'),
                                      admin.make_qualname('public'::TEXT,'tb_2'),
                                      admin.make_qualname('public'::TEXT,'tb_d')]) AS tpb;

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
SET client_min_messages = NOTICE;
SELECT 'admin.detach_partition',*
FROM admin.detach_partition(admin.make_qualname('public','tb'),admin.make_qualname('public','tb_2'),true);
SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.attach_partition',*
FROM admin.attach_partition(admin.make_qualname('public','tb'),
                            admin.make_qualname('public','tb_2'),
                            admin.make_hash_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
															 admin.make_hash_bound(12,1)));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;
 
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
SET client_min_messages = NOTICE;
SELECT 'admin.detach_partition',*
FROM admin.detach_partition(admin.make_qualname('public','tb'),admin.make_qualname('public','tb_2'),true);
SET client_min_messages = notice;
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.attach_partition',*
FROM admin.attach_partition(admin.make_qualname('public','tb'),
							admin.make_qualname('public','tb_2'),
							admin.make_range_partition_bound(admin.get_partition_keyspec(admin.make_qualname('public','tb')),
													   		 ARRAY[admin.make_range_bound(3,6),
															 	   admin.make_range_bound(1,100000)]));
SET client_min_messages = notice;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);

SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tb'),
						admin.make_qualname('public','tb_tpl'));
SET client_min_messages = notice;
SELECT * FROM admin.relations;

SELECT * FROM admin.relations;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbr'), 
                        admin.make_qualname('public','tb_tpl'),
						admin.make_partition_keyspec('Range',ARRAY['region_id','stamp','id'],ARRAY['BigInt','TimeStamp WITH Time Zone','BIGINT']));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbl'),
                        admin.make_qualname('public','tb_tpl'),
                        admin.make_partition_keyspec('lIST',ARRAY['region_id'],ARRAY['bigInt']));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbh'),
                        admin.make_qualname('public','tb_tpl'),
                        admin.make_partition_keyspec('HASH',ARRAY['region_id','stamp','id'],ARRAY['BIGINT','TIMESTAMP WITH TIME ZONE','BIGINT']));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = notice;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tbr (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT)
	PARTITION BY RANGE(region_id,stamp,id);

SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_partition'
FROM admin.create_partition(admin.make_qualname('public','tbr'),
							admin.make_qualname('public','tbr'),
							admin.make_partition(admin.make_qualname('public','tbr_1'),
                                             admin.make_range_partition_bound(admin.make_partition_keyspec('Range',
                                                                                                            ARRAY['region_id','stamp','id'],
                                                                                                            ARRAY['BigInt','TimeStamp WITH Time Zone','BIGINT']),
                                                                              ARRAY[admin.make_range_bound(1,4),
                                                                                    admin.make_range_bound('2023-04-01'::timestamp with time zone,
                                                                                                           '2023-04-30'::timestamp with time zone),
                                                                                    admin.make_range_bound(1,5)])));
SELECT 'admin.create_partition'
FROM admin.create_partition(admin.make_qualname('public','tbr'),
							admin.make_qualname('public','tbr'),
                            admin.make_partition(admin.make_qualname('public','tbr_d'),
                                             admin.make_default_partition_bound(admin.make_partition_keyspec('Range',
                                                                                                            ARRAY['region_id','stamp','id'],
                                                                                                            ARRAY['BigInt','TimeStamp WITH Time Zone','BIGINT']))));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = notice;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tbl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT)
    PARTITION BY list (region_id);
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_partition'
FROM admin.create_partition(admin.make_qualname('public','tbl'),
							admin.make_qualname('public','tbl'),
                            admin.make_partition(admin.make_qualname('public','tbr_1'),
                                             admin.make_list_partition_bound(admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']),
                                                                             admin.make_list_bound(ARRAY[1,2,3]::bigint[]))));


SELECT 'admin.create_partition'
FROM admin.create_partition(admin.make_qualname('public','tbl'),
							admin.make_qualname('public','tbl'),
                            admin.make_partition(admin.make_qualname('public','tbr_d'),
                                             admin.make_default_partition_bound(admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']))));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = notice;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tbh (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT)
    PARTITION BY Hash (region_id,stamp,id);
SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_partition'
FROM admin.create_partition(admin.make_qualname('public','tbh'),
							admin.make_qualname('public','tbh'),
                            admin.make_partition(admin.make_qualname('public','tbh_0'),
                                             admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                                                         ARRAY['region_id','stamp','id'],
                                                                                                         ARRAY['BIGINT','TIMESTAMP WITH TIME ZONE','BIGINT']),
                                                                             admin.make_hash_bound(2,0))));
SELECT 'admin.create_partition'
FROM admin.create_partition(admin.make_qualname('public','tbh'),
							admin.make_qualname('public','tbh'),
                            admin.make_partition(admin.make_qualname('public','tbh_1'),
                                             admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                                                         ARRAY['region_id','stamp','id'],
                                                                                                         ARRAY['BIGINT','TIMESTAMP WITH TIME ZONE','BIGINT']),
                                                                             admin.make_hash_bound(2,1))));


SET client_min_messages = notice;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbl'),
                        admin.make_qualname('public','tb_tpl'),
                        admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']),
                        admin.make_partition(admin.make_qualname('public','tbl_1'),
                                             admin.make_list_partition_bound(admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']),
                                                                             admin.make_list_bound(ARRAY[1,2,3]::bigint[]))),
                        admin.make_partition(admin.make_qualname('public','tbl_d'),
                                             admin.make_default_partition_bound(admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']))));

SET client_min_messages = notice;
SELECT * FROM admin.partitions;


SET client_min_messages = notice;
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id BIGINT);

SELECT * FROM admin.partitions;
SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbr'),      
                        admin.make_qualname('public','tb_tpl'),
                        admin.make_partition_keyspec('Range',ARRAY['region_id','stamp','id'],ARRAY['BigInt','TimeStamp WITH Time Zone','BIGINT']),
						admin.make_partition(admin.make_qualname('public','tbr_1'),
											 admin.make_range_partition_bound(admin.make_partition_keyspec('Range',
																											ARRAY['region_id','stamp','id'],
																											ARRAY['BigInt','TimeStamp WITH Time Zone','BIGINT']),
																			  ARRAY[admin.make_range_bound(1,4),
																					admin.make_range_bound('2023-04-01'::timestamp with time zone,
																										   '2023-04-30'::timestamp with time zone),
																					admin.make_range_bound(1,5)])),
						admin.make_partition(admin.make_qualname('public','tbr_d'),
                                             admin.make_default_partition_bound(admin.make_partition_keyspec('Range',
                                                                                                            ARRAY['region_id','stamp','id'],
                                                                                                            ARRAY['BigInt','TimeStamp WITH Time Zone','BIGINT']))));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbl'),
                        admin.make_qualname('public','tb_tpl'),
                        admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']),
						admin.make_partition(admin.make_qualname('public','tbl_1'),
                                             admin.make_list_partition_bound(admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']),
											 								 admin.make_list_bound(ARRAY[1,2,3]::bigint[]))),
						admin.make_partition(admin.make_qualname('public','tbl_d'),
                                             admin.make_default_partition_bound(admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigInt']))));

SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbh'),
                        admin.make_qualname('public','tb_tpl'),
                        admin.make_partition_keyspec('hash',ARRAY['region_id','stamp','id'],ARRAY['BIGINT','TIMESTAMP WITH TIME ZONE','BIGINT']),
						admin.make_partition(admin.make_qualname('public','tbh_1'),
                                             admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
																										 ARRAY['region_id','stamp','id'],
																										 ARRAY['BIGINT','TIMESTAMP WITH TIME ZONE','BIGINT']),
																			 admin.make_hash_bound(2,0))),
						admin.make_partition(admin.make_qualname('public','tbh_2'),
                                             admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                                                         ARRAY['region_id','stamp','id'],
                                                                                                         ARRAY['BIGINT','TIMESTAMP WITH TIME ZONE','BIGINT']),
                                                                             admin.make_hash_bound(2,1))));
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

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
SET client_min_messages = NOTICE;

SELECT 'admin.create_table',*
FROM admin.create_table(admin.make_qualname('public','tbh'),
                        admin.make_qualname('public','tb'),
						admin.make_partition_keyspec('hash',ARRAY['region_id','id'],ARRAY['BIGINT','BIGINT']));

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
SET client_min_messages = NOTICE;
SELECT 'admin.get_partition',*
FROM admin.get_partition(admin.make_qualname('public','tb_2'));
SET client_min_messages = notice;
SET client_min_messages = NOTICE;
SELECT 'admin.get_partitions',pg_catalog.unnest(tp)
FROM admin.get_partitions(admin.make_qualname('public','tb_2'), admin.make_qualname('public','tb_d')) AS tp;
SET client_min_messages = notice;

SELECT 'admin.get_table_constraint_defs',*
FROM admin.get_table_constraint_defs(admin.make_qualname('public','tb_2'));

SELECT 'admin.get_table_constraint_qualifier',*
FROM admin.get_table_constraint_qualifier(admin.make_qualname('public','tb_2'));

SELECT 'admin.check_them_all',* FROM admin.check_them_all();


