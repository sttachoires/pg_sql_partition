SET client_min_messages = notice;
\set ON_ERROR_STOP on
\i partition_opeautobounds.sql

\x auto
SELECT 'admin.generate_hash_partition_bounds(4)',*
FROM admin.generate_hash_bounds(4);

SELECT 'admin.generate_list_partition_bounds(''A'',''B'',''C'',''D'',''E'')',*
FROM admin.generate_list_bounds('A','B','C','D','E');

SELECT 'admin.generate_list_partition_bounds(ARRAY[''A''],ARRAY[''B'',''C''],ARRAY[''E''])',*
FROM admin.generate_list_bounds(ARRAY['A','D']::TEXT,ARRAY['B','C']::TEXT,ARRAY['E']::TEXT);

SELECT 'admin.generate_range_partition_bounds(2, 0, 2, 2)',*
FROM admin.generate_range_bounds(2, 0, 2, 2);

SELECT 'admin.generate_range_partition_bounds(''1 month''::INTERVAL, ''2023-06-01''::DATE, 7, 7)',*
FROM admin.generate_range_bounds(INTERVAL '1 month', '2023-06-01'::DATE, 2, 2);

SELECT 'admin.generate_range_partition_bounds()',*
FROM admin.generate_range_bounds(ARRAY['A','C','G','N','Z']);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec(''hash'',ARRAY[''id'',''region_id''],ARRAY[''bigint'',''date'']), 4)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('hash',ARRAY['id','region_id'],ARRAY['bigint','date']), 4);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec(''hash'',ARRAY[''id'',''region_id''],ARRAY[''bigint'',''date'']), 4, false)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('hash',ARRAY['id','region_id'],ARRAY['bigint','date']), 4, false);

SET client_min_messages=notice;
SELECT 'admin.check_them_all',* FROM admin.check_them_all();
