--
-- Init schema for bench check
--

SET client_min_messages = error;
SELECT * FROM (
	SELECT e.expire AS "expire", substr(e.regionid,1,3) AS "region", count(*) AS "count"
	FROM public.tb e
	GROUP BY "expire", "region"
	ORDER BY e.expire, substr(e.regionid,1,3)
	) counts
WHERE counts."count" != 20
