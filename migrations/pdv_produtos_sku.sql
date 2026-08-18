-- O código do produto passa a se chamar SKU.
--
-- É o nome que a loja usa e o que vem no arquivo de estoque. Mais importante:
-- é o que o **leitor de código de barras** lê, então ele é a chave de entrada
-- da venda — não um campo secundário ao lado da descrição.
--
-- Único por empresa: dois produtos com o mesmo SKU tornariam a leitura do
-- código de barras ambígua, que é justamente o que não pode acontecer.

ALTER TABLE public.pdv_produtos RENAME COLUMN codigo TO sku;

DROP INDEX IF EXISTS ix_pdv_produtos_codigo;
CREATE UNIQUE INDEX IF NOT EXISTS uq_pdv_produtos_sku
    ON public.pdv_produtos (cod_empresa, sku)
    WHERE sku IS NOT NULL;

-- busca por descrição sem diferenciar maiúscula/minúscula
CREATE INDEX IF NOT EXISTS ix_pdv_produtos_descricao
    ON public.pdv_produtos (cod_empresa, upper(descricao));
