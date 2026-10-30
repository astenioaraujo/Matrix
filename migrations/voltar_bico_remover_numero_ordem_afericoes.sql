-- Aferições: volta o número do bico (texto livre, digitado pelo usuário) no
-- lugar do número de ordem automático (30/10/2026) — o cliente testou o
-- número de ordem e decidiu voltar a pedir o bico. Os registros feitos
-- enquanto só existia número de ordem (29-30/10/2026) não têm o bico
-- gravado (a coluna antiga já tinha sido apagada antes dessa janela) —
-- ficam com numero_bico = '' e o usuário corrige na mão se precisar.

ALTER TABLE public.afericoes
    ADD COLUMN IF NOT EXISTS numero_bico character varying;

UPDATE public.afericoes SET numero_bico = '' WHERE numero_bico IS NULL;

ALTER TABLE public.afericoes
    ALTER COLUMN numero_bico SET NOT NULL,
    DROP COLUMN IF EXISTS numero_ordem;
