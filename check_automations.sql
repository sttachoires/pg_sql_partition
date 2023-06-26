SET client_min_messages = notice;
\pset pager off
\set ON_ERROR_STOP on
\i partition_automations.sql

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id CHAR(3));
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_automatic_table_like(public.tbh, public.tb_tpl, hash(id), modulus 2)',*
FROM admin.create_automatic_table_like('public.tbh', 'public.tb_tpl', 'hash(id)', 'modulus 2');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tbl, public.tb_tpl, list(region_id), list (''AFR'',''AMN'',''ASI'',''EUR'',''OCE'') with default)',*
FROM admin.create_automatic_table_like('public.tbl', 'public.tb_tpl', 'list(region_id)', 'list (''AFR'',''AMN'',''ASI'',''EUR'',''OCE'') with default');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = NOTICE;
SELECT 'admin.create_automatic_table_like(public.tbr1,public.tb_tpl,range(stamp),INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT)',*
FROM admin.create_automatic_table_like('public.tbr1', 'public.tb_tpl', 'range(stamp)','INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id TEXT);

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, range(stamp),INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT, list(region_id),list (''AFR'',''AMN'') with default),hash(id), modulus 2)',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl','range(stamp)','INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT','list(region_id)','list (''AFR'',''AMN'') with default','hash(id)', 'modulus 2');

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id TEXT);

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, hash(id), modulus 2, list(region_id), list (''AFR'',''AMN'') with default)',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl','hash(id)', 'modulus 2', 'list(region_id)', 'list (''AFR'',''AMN'') with default');

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, hash(id), modulus 2, list(id), list (1,2) with default)',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl','hash(id)', 'modulus 2', 'list(id)', 'list (1,2) with default');

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, list(id), list (1,2) with default, hash(id), modulus 2)',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl', 'list(id)', 'list (1,2) with default','hash(id)', 'modulus 2');

SET client_min_messages = notice;
DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
CREATE TABLE public.tb_tpl (id BIGSERIAL, label TEXT, stamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp, region_id TEXT);

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, list(region_id), list (''AFR'',''AMN'') with default))',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl', 'list(region_id)', 'list (''AFR'',''AMN'') with default');

INSERT INTO public.tb (label,region_id) SELECT 'public.tb/'||"s"::TEXT, 'AFR' FROM generate_series(1, 10) AS "s";
INSERT INTO public.tb (label,region_id) SELECT 'public.tb/'||"s"::TEXT, 'AMN' FROM generate_series(1, 10) AS "s";
SET client_min_messages = notice;
SELECT * FROM admin.partitions;

SET client_min_messages = debug;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, hash(id), modulus 2, list(region_id), list (''AFR'',''AMN'') with default))',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl', 'hash(id)', 'modulus 2','list(region_id)', 'list (''AFR'',''AMN'') with default');

SET client_min_messages = NOTICE;
SELECT 'admin.create_automatic_table_like(public.tb, public.tb_tpl, hash(id), modulus 2, list(region_id), list (''AFR'',''AMN'',''ASI'',''EUR'',''OCE'') with default, range(stamp),INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT)',*
FROM admin.create_automatic_table_like('public.tb', 'public.tb_tpl', 'hash(id)', 'modulus 2','list(region_id)', 'list (''AFR'',''AMN'',''ASI'',''EUR'',''OCE'') with default', 'range(stamp)','INTERVAL (1 month) CENTER (2023-06-01) BACK 2 AHEAD 3 WITH DEFAULT');

SET client_min_messages = notice;
SELECT * FROM admin.partitions;

--SET client_min_messages = NOTICE;
--SELECT 'admin.create_automatic_table_like()',*
--FROM admin.create_automatic_table_like('public.tbr2', 'public.tb_tpl', 'range()','');

SET client_min_messages = notice;
SELECT 'admin.check_them_all',* FROM admin.check_them_all();
