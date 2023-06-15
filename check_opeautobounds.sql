SET client_min_messages = notice;
\set ON_ERROR_STOP on
\i partition_opeautobounds.sql

SELECT 'admin.generate_hash_partition_bounds(4)',*
FROM admin.generate_hash_partition_bounds(4);

SELECT 'admin.generate_list_partition_bounds(''A'',''B'',''C'',''D'',''E'')',*
FROM admin.generate_list_partition_bounds('A','B','C','D','E');

SELECT 'admin.generate_list_partition_bounds(ARRAY[''A''],ARRAY[''B'',''C''],ARRAY[''E''])',*
FROM admin.generate_list_partition_bounds(ARRAY['A','D']::TEXT,ARRAY['B','C']::TEXT,ARRAY['E']::TEXT);

SET client_min_messages = debug;
SELECT 'admin.generate_range_partition_bounds(boundrange ANYCOMPATIBLE, center ANYCOMPATIBLE, back INT=0, ahead INT=0)',*
FROM admin.generate_range_partition_bounds('1 month'::INTERVAL, '2023-06-01'::DATE, 7, 7);
