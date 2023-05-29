-
-- Complex partition operations concurently
--

\i partition_managers.sql

CREATE PROCEDURE admin.split_range_partition_concurently(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
END
$$;

CREATE PROCEDURE admin.split_hash_partition_concurently(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
END
$$;

CREATE PROCEDURE admin.split_list_partition_concurently(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
END
$$;

CREATE PROCEDURE admin.split_partition_concurently(tabqname admin.qualified_name, srcpart admin.partition, VARIADIC dstparts admin.partition[])
RETURNS VOID
LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
END
$$;
