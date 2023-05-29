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
	regionid		VARCHAR(6),
	contid			VARCHAR(3)
);
ALTER TABLE public.tb ADD CONSTRAINT tb_regid_chk CHECK (substr(regionid,1,3) IN ('EUR','ASI','AFR','AMN'));
ALTER TABLE public.tb ADD CONSTRAINT tb_exp_chk CHECK (expire > '2000-12-31');

CREATE INDEX tb_expire_idx ON public.tb (expire);
CREATE INDEX tb_regionid_idx ON public.tb (substr(regionid,1,3));

SET client_min_messages = NOTICE;
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('EUR%3s',substr(md5(random()::text),1,3)), 'EUR' FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('ASI%3s',substr(md5(random()::text),1,3)), 'ASI' FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('AFR%3s',substr(md5(random()::text),1,3)), 'AFR' FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), false, format('AMN%3s',substr(md5(random()::text),1,3)), 'AMN' FROM seq AS s(v);
SELECT * FROM (
	SELECT e.expire AS "expire", substr(e.regionid,1,3) AS "region", count(*) AS "count"
	FROM public.tb e
	WHERE e.closed IS FALSE
	GROUP BY "expire", "region"
	ORDER BY e.expire, substr(e.regionid,1,3)
	) counts;

WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('EUR%3s',substr(md5(random()::text),1,3)), 'EUR' FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('ASI%3s',substr(md5(random()::text),1,3)), 'ASI' FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('AFR%3s',substr(md5(random()::text),1,3)), 'AFR' FROM seq AS s(v);
WITH seq AS (SELECT * FROM generate_series(1,120000))
INSERT INTO public.tb (expire,closed,regionid,contid)
SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('AMN%3s',substr(md5(random()::text),1,3)), 'AMN' FROM seq AS s(v);

SELECT * FROM (
    SELECT e.expire AS "expire", substr(e.regionid,1,3) AS "region", count(*) AS "count"
    FROM public.tb e
    WHERE e.closed IS FALSE
    GROUP BY "expire", "region"
    ORDER BY e.expire, substr(e.regionid,1,3)
    ) counts
WHERE counts."count" != 20;

\quit
SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_partition_by('public.tb','HASH(id)');
SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d','public.tb_i0 FOR VALUES WITH (MODULUS 3, REMAINDER 0)','public.tb_i1 FOR VALUES WITH (MODULUS 3, REMAINDER 1)','public.tb_i2 FOR VALUES WITH (MODULUS 3, REMAINDER 2');
SELECT * FROM admin.partitions;

SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_partition_by('public.tb','LIST(contid)');
SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d1','public.tb_c1 FOR VALUES IN (''EUR'',''ASI'')','public.tb_c2 FOR VALUES IN (''ASI'')','public.tb_c3 FOR VALUES IN (''t'')');
SELECT * FROM admin.partitions;

SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_partition_by('public.tb','RANGE(expire)');
SELECT * FROM admin.partitions;
SELECT * FROM admin.alter_table_split_partition('public.tb','public.tb_d2','public.tb_e1 FOR VALUES FROM (''2023-03-01'') TO (''2023-04-01'')','public.tbe_4 FOR VALUES FROM (''2023-04-01'') TO (''2023-05-01'')','public.tbe_z FOR VALUES FROM (''2023-06-01'') TO (''2023-07-01'')');
SELECT * FROM admin.partitions;

