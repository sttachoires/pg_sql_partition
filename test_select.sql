--
-- Init schema for bench check
--

BEGIN;
SELECT * FROM (
	SELECT e.expire AS "expire", substr(e.regionid,1,3) AS "region", count(*) AS "count"
	FROM public.tb e
	WHERE e.closed IS FALSE
	GROUP BY "expire", "region"
	ORDER BY e.expire, substr(e.regionid,1,3)
	) counts
WHERE counts."count" != 20;
END;
