SET client_min_messages=ERROR;
\set ON_ERROR_STOP on
\pset pager off
\i partition_types.sql
\set ECHO all

SET client_min_messages=NOTICE;
SELECT 'admin.make_partition_keyspec',*
FROM admin.make_partition_keyspec('range',ARRAY['id','region_id'],ARRAY['bigint','bigint']);

SET client_min_messages=notice;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
CREATE TABLE public.tb (id bigint,label text) PARTITION BY RANGE(id);
SELECT 'admin.get_partition_keyspec',*
FROM admin.get_partition_keyspec(admin.string_to_qualname('public.tb'));

SET client_min_messages=NOTICE;
SELECT 'admin.make_hash_bound',*
FROM admin.make_hash_bound(1,0);

SET client_min_messages=NOTICE;
SELECT 'admin.make_range_bound',*
FROM admin.make_range_bound('2023-01-01'::DATE,'2023-02-01'::DATE);
SET client_min_messages=NOTICE;
SELECT 'admin.make_range_bound',*
FROM admin.make_range_bound(0,7);
SET client_min_messages=NOTICE;
SELECT 'admin.make_range_bound',*
FROM admin.make_range_bound('a'::TEXT,'e'::TEXT);

SET client_min_messages=NOTICE;
SELECT 'admin.make_list_bound',*
FROM admin.make_list_bound(ARRAY[0,1,3,6,12,2,5,4,8]);
SET client_min_messages=NOTICE;
SELECT 'admin.make_list_bound',*
FROM admin.make_list_bound(ARRAY['2023-01-01'::DATE,'2023-02-01'::DATE,'2023-03-01'::DATE]);
SET client_min_messages=NOTICE;
SELECT 'admin.make_list_bound',*
FROM admin.make_list_bound(ARRAY['a','b','c','z','q','p']);

SET client_min_messages=NOTICE;
SELECT 'admin.make_default_partition_bound',*
FROM admin.make_default_partition_bound(admin.make_partition_keyspec('range',
                                                                      ARRAY['id','region_id'],
                                                                      ARRAY['bigint','bigint']));
SET client_min_messages=NOTICE;
SELECT 'admin.make_hash_partition_bound',*
FROM admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                      ARRAY['id','region_id'],
                                                                      ARRAY['bigint','bigint']),
									admin.make_hash_bound(1,0));
SET client_min_messages=NOTICE;
SELECT 'admin.make_list_partition_bound',*
FROM admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                     ARRAY['stamp'],
									 ARRAY['DATE']),
								admin.make_list_bound(ARRAY['2023-01-01'::DATE,'2023-02-01'::DATE,'2023-03-01'::DATE]));

SET client_min_messages=NOTICE;
SELECT 'admin.make_list_partition_bound',*
FROM admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                     ARRAY['label'],
                                     ARRAY['text']),
								admin.make_list_bound(ARRAY['a','b','c','z','q','p']));
SET client_min_messages=NOTICE;
SELECT 'admin.make_range_partition_bound',*
FROM admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                      ARRAY['id','region_id'],
                                                                      ARRAY['bigint','bigint']),
								ARRAY[admin.make_range_bound(50,100),
										admin.make_range_bound(1,5)]);


SET client_min_messages=NOTICE;
SELECT 'admin.make_partition',*
FROM admin.make_partition(admin.make_qualname('public'::TEXT,'tb5'::TEXT),
                                        admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                        ARRAY['id','region_id'],
                                                                        ARRAY['bigint','bigint']),
																	admin.make_hash_bound(1,0)));


SET client_min_messages=notice;
\set ECHO none
SELECT * FROM admin.check_them_all();



