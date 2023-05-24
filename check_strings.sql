SET client_min_messages=notice;
\set ON_ERROR_STOP on
\i partition_strings.sql

SELECT 'admin.string_to_partition_keyspec',*
FROM admin.string_to_partition_keyspec('Range( id,   region_id)');

SELECT 'admin.partition_keyspec_to_string',*
FROM admin.partition_keyspec_to_string(admin.make_partition_keyspec('hash',
                                                                 ARRAY['id','region_id'],
                                                                 ARRAY['bigint','bigint']));

SELECT 'admin.string_to_hash_partition_bound',*
FROM admin.string_to_hash_partition_bound(admin.make_partition_keyspec('hash',
																 ARRAY['id','region_id'],
																 ARRAY['bigint','bigint']),
									'WITH (MODULUS 8, REMAINDER 0)');

SELECT 'admin.hash_partition_bound_to_string',*
FROM admin.hash_partition_bound_to_string(admin.string_to_hash_partition_bound(admin.make_partition_keyspec('hash',
																								ARRAY['id','region_id'],
																								ARRAY['bigint','bigint']),
																   'WITH (MODULUS 8, REMAINDER 0)'));

SELECT 'admin.string_to_range_partition_bound',*
FROM admin.string_to_range_partition_bound(admin.make_partition_keyspec('range',
																  ARRAY['id','region_id'],
																  ARRAY['bigint','bigint']),
									 'FROM (1) TO (5)');
SELECT 'admin.string_to_range_partition_bound',*
FROM admin.string_to_range_partition_bound(admin.make_partition_keyspec('range',
																  ARRAY['id','region_id'],
																  ARRAY['bigint','bigint']),
									 'FROM (1,1) (5000,5)');
SELECT 'admin.string_to_range_partition_bound',*
FROM admin.string_to_range_partition_bound(admin.make_partition_keyspec('range',
																  ARRAY['stamp'],
																  ARRAY['DATE']),
								     'FROM (''2023-01-01'') TO (''2023-06-01'')');

SELECT 'admin.range_partition_bound_to_string',*
FROM admin.range_partition_bound_to_string(admin.string_to_range_partition_bound(admin.make_partition_keyspec('range',
																								  ARRAY['id','region_id'],
																								  ARRAY['bigint','bigint']),
																	 'FROM (1) TO (5)'));
SELECT 'admin.range_partition_bound_to_string',*
FROM admin.range_partition_bound_to_string(admin.string_to_range_partition_bound(admin.make_partition_keyspec('range',
																								  ARRAY['id','region_id'],
																								  ARRAY['bigint','bigint']),
																	 'FROM (1,1) (5000,5)'));
SELECT 'admin.range_partition_bound_to_string',*
FROM admin.range_partition_bound_to_string(admin.string_to_range_partition_bound(admin.make_partition_keyspec('range',
																								  ARRAY['stamp','id','label'],
																								  ARRAY['DATE','BIGINT','TEXT']),
																	 'FROM (''2023-01-01'',0,''a'') TO (''2023-06-01'',5,''l'')'));

SELECT 'admin.string_to_list_partition_bound',*
FROM admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																 ARRAY['region_id'],
																 ARRAY['integer[]']),
									'IN (0,1,3,6,12,2,5,4,8)');
SELECT 'admin.string_to_list_partition_bound',*
FROM admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																 ARRAY['stamp'],
																 ARRAY['DATE[]']),
									'IN (''2023-1-1'',''2023-02-01'',''2023-03-01'')');
SELECT 'admin.string_to_list_partition_bound',*
FROM admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																 ARRAY['stamp'],
																 ARRAY['TEXT[]']),
									'IN (''2023-1-1'',''2023-02-01'',''2023-03-01'')');
SELECT 'admin.string_to_list_partition_bound',*
FROM admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																 ARRAY['label'],
																 ARRAY['TEXT[]']),
									'IN (''a'',''b'',''c'',''z'',''q'',''p'')');

SELECT 'admin.list_partition_bound_to_string',*
FROM admin.list_partition_bound_to_string(admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																								ARRAY['region_id'],
																								ARRAY['integer[]']),
									'IN (0,1,3,6,12,2,5,4,8)'));
SELECT 'admin.list_partition_bound_to_string',*
FROM admin.list_partition_bound_to_string(admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																								ARRAY['stamp'],
																								ARRAY['DATE[]']),
									'IN (''2023-1-1'',''2023-02-01'',''2023-03-01'')'));
SELECT 'admin.list_partition_bound_to_string',*
FROM admin.list_partition_bound_to_string(admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																								ARRAY['stamp'],
																								ARRAY['TEXT[]']),
									'IN (''2023-1-1'',''2023-02-01'',''2023-03-01'')'));
SELECT 'admin.list_partition_bound_to_string',*
FROM admin.list_partition_bound_to_string(admin.string_to_list_partition_bound(admin.make_partition_keyspec('list',
																								ARRAY['label'],
																								ARRAY['TEXT[]']),
									'IN (''a'',''b'',''c'',''z'',''q'',''p'')'));

SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition_bound(admin.make_partition_keyspec('range',
															ARRAY['id','region_id'],
															ARRAY['bigint','bigint']),
							   'FROM (1,1) (5000,5)');
SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition_bound(admin.make_partition_keyspec('list',
															ARRAY['region_id'],
															ARRAY['integer[]']),
								'IN (0,1 ,3,6, 12    ,2,5,4,8)');
SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition_bound(admin.make_partition_keyspec('hash',
															ARRAY['id','region_id'],
															ARRAY['bigint','bigint']),
								'WITH (MODULUS 8, REMAINDER 0)');
SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition_bound(admin.make_partition_keyspec('list',
															ARRAY['region_id'],
															ARRAY['integer[]']),
								'DEFAULT');

SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition_bound(
        admin.make_partition_keyspec('range',
                                     ARRAY['id'],
                                     ARRAY['bigint']),
        'tb_1 FOR VALUES FROM (1) TO (5)');

SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition(
		admin.make_partition_keyspec('range',
        	                         ARRAY['id','region_id'],
            	                     ARRAY['bigint','bigint']),
		'tb_1 FROM (1,1) TO (5,5000)');

SELECT 'admin.string_to_partition_bound',*
FROM admin.string_to_partition(
        admin.make_partition_keyspec('range',
                                     ARRAY['id','region_id'],
                                     ARRAY['bigint','bigint']),
        'tb_1(1,1)(5,5000)');

SELECT 'admin.partition_bound_to_string',*
FROM admin.partition_bound_to_string(admin.make_default_partition_bound(admin.make_partition_keyspec('range',
                                     ARRAY['id','region_id'],
                                     ARRAY['bigint','bigint'])));

SELECT 'admin.partition_bound_to_string',*
FROM admin.partition_bound_to_string(admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                     ARRAY['id','region_id'],
                                     ARRAY['bigint','bigint']),
                                admin.make_hash_bound(1,0)));

SELECT 'admin.partition_bound_to_string',*
FROM admin.partition_bound_to_string(admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                     ARRAY['id'],
                                     ARRAY['bigint']),
                                admin.make_list_bound(ARRAY[0,1,3,6,12,2,5,4,8])));

SELECT 'admin.check_them_all',* FROM admin.check_them_all();

SET client_min_messages=notice;


