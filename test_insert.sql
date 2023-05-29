--
-- Init schema for bench check
--

SET client_min_messages = error;
BEGIN;
	WITH seq AS (SELECT * FROM generate_series(1,120))
	INSERT INTO public.tb (expire,closed,regionid,contid)
	SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('EUR%3s',substr(md5(random()::text),1,3)), 'EUR' FROM seq AS s(v);
	WITH seq AS (SELECT * FROM generate_series(1,120))
	INSERT INTO public.tb (expire,closed,regionid,contid)
	SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('ASI%3s',substr(md5(random()::text),1,3)), 'ASI' FROM seq AS s(v);
	WITH seq AS (SELECT * FROM generate_series(1,120))
	INSERT INTO public.tb (expire,closed,regionid,contid)
	SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('AFR%3s',substr(md5(random()::text),1,3)), 'AFR' FROM seq AS s(v);
	WITH seq AS (SELECT * FROM generate_series(1,120))
	INSERT INTO public.tb (expire,closed,regionid,contid)
	SELECT (current_date - (concat((s.v % 6),' month'))::INTERVAL), true, format('AMN%3s',substr(md5(random()::text),1,3)), 'AMN' FROM seq AS s(v);
	--WITH seq AS (SELECT * FROM generate_series(1,120))
	--INSERT INTO public.tb (expire,closed,regionid)
	--SELECT (current_date - (concat((s.v % 12),' month'))::INTERVAL), true, format('AMS%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
	--WITH seq AS (SELECT * FROM generate_series(1,120))
	--INSERT INTO public.tb (expire,closed,regionid)
	--SELECT (current_date - (concat((s.v % 12),' month'))::INTERVAL), true, format('OCE%3s',substr(md5(random()::text),1,3)) FROM seq AS s(v);
END;
