-- Vendedora é de uma loja, não da empresa.
--
-- A tabela nasceu chaveada só por `cod_empresa`, o que faria a vendedora de
-- uma filial aparecer no seletor de venda da outra. Com mais de uma loja isso
-- vira erro de atribuição de venda — e é justamente pelo vendedor que se
-- responde "quem vendeu isso".
--
-- Todas as vendedoras existentes são da filial 1 (a EMP013 só tem essa loja),
-- então o backfill é direto.

ALTER TABLE public.pdv_vendedores
    ADD COLUMN IF NOT EXISTS cod_filial integer;

UPDATE public.pdv_vendedores SET cod_filial = 1 WHERE cod_filial IS NULL;

ALTER TABLE public.pdv_vendedores
    ALTER COLUMN cod_filial SET NOT NULL,
    ALTER COLUMN cod_filial SET DEFAULT 1;

DROP INDEX IF EXISTS ix_pdv_vendedores_empresa;
CREATE INDEX IF NOT EXISTS ix_pdv_vendedores_filial
    ON public.pdv_vendedores (cod_empresa, cod_filial, ativo);
