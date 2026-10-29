-- Aferições: remove o conceito de "bico", usa sequência numérica por dia (29/10/2026)
--
-- O cliente decidiu não rastrear mais qual bico físico foi aferido. No lugar,
-- cada aferição ganha um número de ordem sequencial que reinicia a cada dia,
-- por filial — um contador único do dia, independente do combustível aferido
-- (não é por produto). Campo observação também sai: não era usado (nenhuma
-- das 6 aferições de teste já cadastradas tinha valor preenchido).
--
-- Retenção também muda: antes mantinha as 2 aferições mais recentes de cada
-- (filial, produto, bico); agora mantém as aferições das 2 **datas** mais
-- recentes de cada filial, sem olhar produto/ordem.

DROP INDEX IF EXISTS public.ix_afericoes_bico;

ALTER TABLE public.afericoes
    DROP COLUMN IF EXISTS numero_bico,
    DROP COLUMN IF EXISTS observacao,
    ADD COLUMN IF NOT EXISTS numero_ordem integer;

-- Backfill das poucas aferições de teste já cadastradas (sem isso ficariam
-- com numero_ordem nulo).
WITH numeradas AS (
    SELECT id_afericao,
           ROW_NUMBER() OVER (
               PARTITION BY cod_empresa, cod_filial, data_afericao
               ORDER BY criado_em
           ) AS rn
    FROM public.afericoes
)
UPDATE public.afericoes a
SET numero_ordem = n.rn
FROM numeradas n
WHERE a.id_afericao = n.id_afericao;

ALTER TABLE public.afericoes ALTER COLUMN numero_ordem SET NOT NULL;
