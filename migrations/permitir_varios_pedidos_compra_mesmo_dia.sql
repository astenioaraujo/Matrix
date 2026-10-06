-- Vários pedidos de compra para o mesmo posto, produto e dia (06/10/2026).
--
-- O índice único (empresa, data, filial, produto, fornecedor) e o
-- ON CONFLICT DO UPDATE da tela faziam o segundo pedido SOBRESCREVER o
-- primeiro — sem aviso. Mas no mesmo dia podem vir caminhões diferentes,
-- com preços diferentes: cada pedido é uma linha (id_compra), e é a ela que
-- o descarrego se liga. A tela passa a pedir confirmação quando já existe
-- pedido do mesmo produto para a mesma ponta naquele dia.

ALTER TABLE public.compras_combustiveis
    DROP CONSTRAINT IF EXISTS compras_combustiveis_cod_empresa_data_compra_cod_filial_cod_key;

DROP INDEX IF EXISTS public.uq_compras_combustiveis_coligada;

-- a tela e as consultas filtram por empresa + data
CREATE INDEX IF NOT EXISTS ix_compras_combustiveis_empresa_data
    ON public.compras_combustiveis (cod_empresa, data_compra);
