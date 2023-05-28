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

CREATE INDEX tb_expire_idx ON public.tb (expire);
CREATE INDEX tb_regionid_idx ON public.tb (substr(regionid,1,3));

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
SELECT * FROM (
	SELECT e.expire AS "expire", substr(e.regionid,1,3) AS "region", count(*) AS "count"
	FROM public.tb e
	WHERE e.closed IS FALSE
	GROUP BY "expire", "region"
	ORDER BY e.expire, substr(e.regionid,1,3)
	) counts
WHERE counts."count" != 20;

WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('EUR%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('ASI%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('AFR%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('AMN%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
\quit
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(comptype)');
