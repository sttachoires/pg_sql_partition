SET client_min_messages = notice;
\pset pager off
\set ON_ERROR_STOP on
\i partition_commands.sql

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

