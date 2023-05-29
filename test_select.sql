--
-- Init schema for bench check
--

BEGIN;
	SELECT * FROM public.tb t WHERE t.closed = true;
	SELECT * FROM public.tb t WHERE t.closed = false;
END;
