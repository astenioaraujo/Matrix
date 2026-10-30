-- Aferições: numero_bomba é numérico (bomba 1, bomba 2, bomba 3...), não
-- texto livre como o bico (30/10/2026). Os poucos registros feitos com o
-- campo em texto ficam sem bomba (''  não converte pra número) — vira NULL,
-- coluna deixa de ser NOT NULL porque não há como inventar um número.

ALTER TABLE public.afericoes
    ALTER COLUMN numero_bomba DROP NOT NULL;

ALTER TABLE public.afericoes
    ALTER COLUMN numero_bomba TYPE integer USING NULLIF(numero_bomba, '')::integer;
