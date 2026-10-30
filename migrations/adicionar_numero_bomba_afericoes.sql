-- Aferições: campo "número da bomba" (30/10/2026) — cada bomba tem vários
-- bicos (bomba 1 / bico 1, bomba 1 / bico 2, bomba 2 / bico 1...), e até
-- aqui só o bico era registrado. Texto livre, digitado no formulário, mesma
-- convenção de numero_bico (não existe cadastro de bombas/bicos no sistema).
-- Registros já existentes ficam com numero_bomba = '', usuário corrige na
-- mão se precisar.

ALTER TABLE public.afericoes
    ADD COLUMN IF NOT EXISTS numero_bomba character varying;

UPDATE public.afericoes SET numero_bomba = '' WHERE numero_bomba IS NULL;

ALTER TABLE public.afericoes
    ALTER COLUMN numero_bomba SET NOT NULL;
