--
-- Init schema for bench check
--

SET client_min_messages = error;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

CREATE TABLE public.tb (
	id				BIGSERIAL NOT NULL,
	expire			DATE,
	closed			BOOLEAN,
	regionid		VARCHAR(6)
);

SET client_min_messages = NOTICE;
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('EUR%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('ASI%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('AFR%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('AMN%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
--WITH seq AS (SELECT * FROM generate_series(1,120))
--INSERT INTO public.tb (expire,closed,regionid)
--SELECT (current_date - (concat((s.v % 12),' month'))::INTERVAL), false, format('AMS%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
--WITH seq AS (SELECT * FROM generate_series(1,120))
--INSERT INTO public.tb (expire,closed,regionid)
--SELECT (current_date - (concat((s.v % 12),' month'))::INTERVAL), false, format('OCE%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);

SELECT * FROM (
	SELECT e.expire AS "expire", substr(e.regionid,1,3) AS "region", count(*) AS "count"
	FROM public.tb e
	GROUP BY "expire", "region"
	ORDER BY e.expire, substr(e.regionid,1,3)
	) counts
WHERE counts."count" != 20
\quit
SELECT DISTINCT (e.expire, substr(e.regionid,1,3)), count(c)
FROM public.tb e
LEFT OUTER JOIN public.tb r ON e.expire = r.expire
RIGHT JOIN public.tb c ON r.regionid = e.regionid AND e.expire = c.expire
GROUP BY e.expire, substr(e.regionid,1,3)
ORDER BY e.expire, substr(e.regionid,1,3)
;
\quit
SELECT DISTINCT * FROM (
	SELECT 'ALL' AS "expire", substr(regionid,1,3) AS "region", 20 AS "number"
	FROM public.tb
	WHERE closed IS NOT TRUE
	GROUP BY expire,substr(regionid,1,3)
	ORDER BY expire, substr(regionid,1,3)
	) corrects
UNION ALL
SELECT * FROM (
	SELECT expire::TEXT AS "expire", substr(regionid,1,3) AS "region", count(*) AS "number"
	FROM public.tb
	WHERE closed IS NOT TRUE
	GROUP BY expire,substr(regionid,1,3)
	ORDER BY expire, substr(regionid,1,3)
	) errors
	WHERE errors."number" != 20
;


\quit
SELECT count(*) "total" FROM public.tb t WHERE t.closed IS NOT TRUE;

SELECT DISTINCT pg_catalog.substr(t.regionid,1,3) "regions", count(DISTINCT pg_catalog.substr(t.regionid,1,3)) FROM public.tb t WHERE t.closed IS NOT TRUE ORDER BY regions;

WITH regions AS (
	SELECT DISTINCT pg_catalog.substr(t.regionid,1,3) "r" FROM public.tb t WHERE t.closed IS NOT TRUE ORDER BY "r"
	)
SELECT DISTINCT 

SELECT min(t.id) "min id", max(t.id) "id max" FROM public.tb t WHERE t.closed IS NOT TRUE;
SELECT DISTINCT t.expire "expire" FROM public.tb t WHERE t.closed IS NOT TRUE ORDER BY t.expire;


SELECT dates.expire, 
FROM (
	SELECT DISTINCT t.expire "expire" FROM public.tb t WHERE t.closed IS NOT TRUE ORDER BY t.expire
	) dates(expire)
JOIN public.tb regions
  ON SELECT DISTINCT pg_catalog.substr(t.regionid,1,3) "regions",DISTINCT t.expire "expire" FROM public.tb t WHERE t.closed IS NOT TRUE ORDER BY t.expire;
	SELECT DISTINCT pg_catalog.substr(t.regionid,1,3) "regions", count(DISTINCT pg_catalog.substr(t.regionid,1,3)) FROM public.tb t WHERE t.closed IS NOT TRUE ORDER BY regions
\quit

SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(expire)');
SELECT * FROM admin.partitions;
SELECT * FROM pg_sleep(5);
SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_1 FOR VALUES FROM (''2023-03-01'') TO (''2023-04-01'')','public.tb_4 FOR VALUES FROM (''2023-04-01'') TO (''2023-05-01'')','public.tb_z FOR VALUES FROM (''2023-06-01'') TO (''2023-07-01'')');
SELECT * FROM admin.partitions;
\quit
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(comptype)');

SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_1 FOR VALUES FROM (''2023-03-01'') TO (''2023-04-01'')');
\quit
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(comptype)');
