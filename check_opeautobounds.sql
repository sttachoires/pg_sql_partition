SET client_min_messages = ERROR;
\set ON_ERROR_STOP on
\pset pager off
\x off
\set ECHO none
\i partition_opeautobounds.sql
\set ECHO all

SELECT 'admin.generate_hash_partition_bounds(4)',*
FROM admin.generate_hash_bounds(4);

SELECT 'admin.generate_list_partition_bounds(admin.make_partition_keyspec(''list'',ARRAY[''id''],ARRAY[''TEXT'']),''A'',''B'',''C'',''D'',''E'')',*
FROM admin.generate_list_bounds(admin.make_partition_keyspec('list',ARRAY['id'],ARRAY['TEXT']),'A','B','C','D','E');

SET client_min_messages = debug;
SELECT 'admin.generate_list_partition_bounds(admin.make_partition_keyspec(''list'',ARRAY[''id''],ARRAY[''TEXT'']),ARRAY[''A'']::TEXT[],ARRAY[''B'',''C'']::TEXT[],ARRAY[''E'']::TEXT[])',*
FROM admin.generate_list_bounds(admin.make_partition_keyspec('list',ARRAY['id'],ARRAY['TEXT']),ARRAY['A','D'],ARRAY['B','C'],ARRAY['E']);

SELECT 'admin.generate_range_partition_bounds(2, 0, 2, 2)',*
FROM admin.generate_range_bounds(2, 0, 2, 2);

SELECT 'admin.generate_range_partition_bounds(''1 month''::INTERVAL, ''2023-06-01''::DATE, 7, 7)',*
FROM admin.generate_range_bounds(INTERVAL '1 month', '2023-06-01'::DATE, 2, 2);

SELECT 'admin.generate_range_partition_bounds()',*
FROM admin.generate_range_bounds(VARIADIC '{A,C,G,N,Z}'::character[]);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec(''hash'',ARRAY[''id'',''region_id''],ARRAY[''bigint'',''date'']), 4)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('hash',ARRAY['id','region_id'],ARRAY['bigint','bigint']), 4);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec(''hash'',ARRAY[''id'',''region_id''],ARRAY[''bigint'',''date'']), 4, false)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('hash',ARRAY['id','region_id'],ARRAY['bigint','bigint']), 4, false);

SELECT 'admin.generate_partition_bounds((''list'',{''id''},{''character''}), {''A'',''B'',''C'',''D'',''E''})',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('list',ARRAY['id'],ARRAY['character']), ARRAY['A','B','C','D','E']);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec(''list'',ARRAY[''id''],ARRAY[''TEXT'']), ARRAY[ARRAY[''A'',''B''],ARRAY[''B'',''C''],ARRAY[''D'',''E'']], true)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('list',ARRAY['id'],ARRAY['TEXT']), ARRAY[ARRAY['A','D']::TEXT,ARRAY['B','C']::TEXT,ARRAY['D','E']::TEXT], true);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec((''range'',{''stamp''},{''DATE''}),''1 month''::INTERVAL,''2023-06-01''::DATE,2,2)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('range',ARRAY['stamp'],ARRAY['DATE']), '1 month'::INTERVAL, '2023-06-01'::DATE, 2, 2);

SELECT 'admin.generate_partition_bounds(admin.make_partition_keyspec(''range'',{''stamp''},{''DATE''}),''1 month''::INTERVAL,''2023-06-01''::DATE,2,2,false)',*
FROM admin.generate_partition_bounds(admin.make_partition_keyspec('range',ARRAY['stamp'],ARRAY['DATE']), '1 month'::INTERVAL, '2023-06-01'::DATE, 2, 2, false);

SELECT 'admin.string_to_automatic_partitions((public.tb),(hash,{id,region_id},{bigint,bigint}),''OVER AUTOMATIC MODULUS 4'')',*
FROM admin.string_to_automatic_partitions(admin.make_qualname('public','tb'), admin.make_partition_keyspec('hash',ARRAY['id','region_id'],ARRAY['bigint','bigint']), 'OVER AUTOMATIC MODULUS 4');

SELECT 'admin.string_to_automatic_partitions((public.tb),(list,{id,region_id},{bigint,bigint}),''OVER AUTOMATIC LIST (AFR,AME,ASI,EUR,OCE) WITH DEFAULT'')',*
FROM admin.string_to_automatic_partitions(admin.make_qualname('public','tb'), admin.make_partition_keyspec('list',ARRAY['region_id'],ARRAY['bigint']), 'OVER AUTOMATIC LIST (''AFR'',''AME'',''ASI'',''EUR'',''OCE'') WITH DEFAULT');

SELECT 'admin.string_to_automatic_partitions((public.tb),(range,{stamp},{DATE}),''OVER AUTOMATIC INTERVAL (1 month) CENTER (2023-06-23) BACK 2 AHEAD 2 WITH DEFAULT)''',*
FROM admin.string_to_automatic_partitions(admin.make_qualname('public','tb'), admin.make_partition_keyspec('range','{stamp}','{DATE}'),'OVER AUTOMATIC INTERVAL (1 month) CENTER (2023-06-23) BACK 2 AHEAD 2 WITH DEFAULT');

SET client_min_messages=notice;
\set ECHO none
SELECT 'admin.check_them_all',* FROM admin.check_them_all();
