SET client_min_messages=notice;
\set ON_ERROR_STOP on
\i partition_opebounds.sql

SELECT 'admin.hash_partition_bound_to_qualifier',*
FROM admin.hash_partition_bound_to_qualifier(admin.make_qualname('public'::TEXT,'tb5'::TEXT),
                                        admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                        ARRAY['id','region_id'],
                                                                        ARRAY['bigint','bigint']),
                                                                    admin.make_hash_bound(1,0)));

SELECT 'admin.list_partition_bound_to_qualifier',*
FROM admin.list_partition_bound_to_qualifier(admin.make_default_partition_bound(admin.make_partition_keyspec('list',
                                     ARRAY['id'],
                                     ARRAY['bigint'])));

SELECT 'admin.list_partition_bound_to_qualifier',*
FROM admin.list_partition_bound_to_qualifier(admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                     ARRAY['id'],
                                     ARRAY['bigint']),
                                admin.make_list_bound(ARRAY[0,1,3,6,12,2,5,4,8])));

SELECT 'admin.range_partition_bound_to_qualifier',*,'SELECT * FROM public.tb WHERE '||rpq.clause
FROM admin.range_partition_bound_to_qualifier(admin.make_default_partition_bound(admin.make_partition_keyspec('RANGE',
                                     ARRAY['id','region_id'],
                                     ARRAY['bigint','bigint']))) rpq(clause);

SELECT 'admin.range_partition_bound_to_qualifier',rpq.clause,'SELECT * FROM public.tb WHERE '||rpq.clause
FROM admin.range_partition_bound_to_qualifier(admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                     ARRAY['id'],
                                     ARRAY['bigint']),
                                    ARRAY[admin.make_range_bound(1,5)])) rpq(clause);

SELECT 'admin.range_partition_bound_to_qualifier',rpq.clause,'SELECT * FROM public.tb WHERE '||rpq.clause
FROM admin.range_partition_bound_to_qualifier(admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                     ARRAY['id','stamp'],
                                     ARRAY['bigint','timestamp with time zone']),
                                     ARRAY[admin.make_range_bound(1,5),
										   admin.make_range_bound('2023-01-01 00:00:00+01'::TIMESTAMP WITH TIME ZONE,'2023-01-01 00:00:00+01'::timestamp with time zone)])) rpq(clause);

SELECT 'admin.range_partition_bound_to_qualifier',rpq.clause,'SELECT * FROM public.tb WHERE '||rpq.clause
FROM admin.range_partition_bound_to_qualifier(admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                     ARRAY['id','region_id','stamp'],
                                     ARRAY['bigint','bigint','timestamp with time zone']),
                                     ARRAY[admin.make_range_bound(1,5),admin.make_range_bound(1,5),
										   admin.make_range_bound('2023-01-01 00:00:00+01'::TIMESTAMP WITH TIME ZONE,'2023-01-01 00:00:00+01'::timestamp with time zone)])) rpq(clause);

SELECT 'admin.sort_partition_bounds',*
FROM admin.sort_partition_bounds(
	ARRAY[
		admin.make_list_partition_bound(admin.make_partition_keyspec('list',
																	ARRAY['region_id'],
																	ARRAY['integer[]']),
									   admin.make_list_bound('{3,5,4,8}'::integer[])),
		admin.make_list_partition_bound(admin.make_partition_keyspec('list',
																	ARRAY['region_id'],
																	ARRAY['integer[]']),
									   admin.make_list_bound('{14,6,9}'::integer[])),
		admin.make_list_partition_bound(admin.make_partition_keyspec('list',
																	ARRAY['region_id'],
																	ARRAY['integer[]']),
									   admin.make_list_bound('{2,12}'::integer[]))
	]::admin.partition_bound[]);

SELECT 'admin.sort_list_partition_bounds',*
FROM admin.sort_partition_bounds(
	ARRAY[
		admin.make_list_partition_bound(admin.make_partition_keyspec('list',
																	ARRAY['stamp'],
																	ARRAY['DATE[]']),
									   admin.make_list_bound('{''2023-1-1''}'::DATE[])),
		admin.make_list_partition_bound(admin.make_partition_keyspec('list',
																	ARRAY['stamp'],
																	ARRAY['DATE[]']),
									   admin.make_list_bound('{''2023-03-01''}'::DATE[])),
		admin.make_list_partition_bound(admin.make_partition_keyspec('list',
																	ARRAY['stamp'],
																	ARRAY['DATE[]']),
									   admin.make_list_bound('{''2023-02-01''}'::DATE[]))
	]::admin.partition_bound[]);
SELECT 'admin.sort_partition_bounds',*
FROM admin.sort_partition_bounds(
    ARRAY[
        admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                                                    ARRAY['stamp'],
                                                                    ARRAY['TEXT[]']),
                                       admin.make_list_bound('{''2023-1-1''}'::TEXT[])),
        admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                                                    ARRAY['stamp'],
                                                                    ARRAY['TEXT[]']),
                                       admin.make_list_bound('{''2023-03-01''}'::TEXT[])),
        admin.make_list_partition_bound(admin.make_partition_keyspec('list',
                                                                    ARRAY['stamp'],
                                                                    ARRAY['TEXT[]']),
                                       admin.make_list_bound('{''2023-02-01''}'::TEXT[]))
    ]::admin.partition_bound[]);

SELECT 'admin.sort_partition_bounds',*
FROM admin.sort_partition_bounds(
	ARRAY[
        admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                      ARRAY['id','region_id'],
                                                                      ARRAY['bigint','bigint']),
                                        ARRAY[admin.make_range_bound(50,100),
											  admin.make_range_bound(1,5)
										]),
		admin.make_range_partition_bound(admin.make_partition_keyspec('range',
        	                                                          ARRAY['id','region_id'],
            	                                                      ARRAY['bigint','bigint']),
										ARRAY[admin.make_range_bound(1,50),
											  admin.make_range_bound(1,5)
										]),
		admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                      ARRAY['id','region_id'],
                                                                      ARRAY['bigint','bigint']),
                                        ARRAY[admin.make_range_bound(100,1000),
                                        	  admin.make_range_bound(1,5)
										])
		]);

SELECT 'admin.sort_partition_bounds',*
FROM admin.sort_partition_bounds(
    ARRAY[
		admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                     ARRAY['stamp'],
                                                                     ARRAY['TEXT']),
										ARRAY[admin.make_range_bound('2023-1-1'::TEXT,'2023-2-1'::TEXT)]),
		admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                     ARRAY['stamp'],
                                                                     ARRAY['TEXT']),
										ARRAY[admin.make_range_bound('2023-03-01'::TEXT,'2023-04-01'::TEXT)]),
		admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                     ARRAY['stamp'],
                                                                     ARRAY['TEXT']),
                                        ARRAY[admin.make_range_bound('2023-02-01'::TEXT,'2023-03-01'::TEXT)])
	]::admin.partition_bound[]);

SELECT 'admin.sort_partition_bounds',*
FROM admin.sort_partition_bounds(
    ARRAY[
        admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                     ARRAY['stamp'],
                                                                     ARRAY['DATE']),
                                        ARRAY[admin.make_range_bound('2023-1-1'::DATE,'2023-2-1'::DATE)]),
        admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                     ARRAY['stamp'],
                                                                     ARRAY['DATE']),
                                        ARRAY[admin.make_range_bound('2023-03-01'::DATE,'2023-04-01'::DATE)]),
        admin.make_range_partition_bound(admin.make_partition_keyspec('range',
                                                                     ARRAY['stamp'],
                                                                     ARRAY['DATE']),
                                        ARRAY[admin.make_range_bound('2023-02-01'::DATE,'2023-03-01'::DATE)])
    ]::admin.partition_bound[]);

SELECT 'admin.sort_partition',*
FROM admin.sort_partitions(ARRAY[admin.make_partition(admin.make_qualname('public'::TEXT,'tb5'::TEXT),
                                        admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                        ARRAY['id','region_id'],
                                                                        ARRAY['bigint','bigint']),
																	admin.make_hash_bound(8,0))),
								admin.make_partition(admin.make_qualname('public'::TEXT,'tb5'::TEXT),
                                        admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                        ARRAY['id','region_id'],
                                                                        ARRAY['bigint','bigint']),
                                                                    admin.make_hash_bound(4,1))),
								admin.make_partition(admin.make_qualname('public'::TEXT,'tb5'::TEXT),
                                        admin.make_hash_partition_bound(admin.make_partition_keyspec('hash',
                                                                        ARRAY['id','region_id'],
                                                                        ARRAY['bigint','bigint']),
                                                                    admin.make_hash_bound(8,4)))
							]);

SELECT 'admin.check_them_all',* FROM admin.check_them_all();

SET client_min_messages=notice;


