-- Observação livre por bico em Bombas e Bicos (Pix, convênio...), 25/11/2026.

BEGIN;

ALTER TABLE public.bombas_bicos
    ADD COLUMN IF NOT EXISTS observacao character varying;

COMMIT;
