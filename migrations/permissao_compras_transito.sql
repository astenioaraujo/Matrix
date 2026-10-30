-- Compras em Trânsito (Operações) — 30/10/2026
--
-- Lista, sem filtro de data, toda compra com saldo a descarregar: comprada e
-- ainda não descarregada, ou descarregada só em parte. Nada gravado.
--
-- Concedida, a pedido do cliente, só à Heloísa (id 32) na EMP010 e à Camila
-- (id 65) na EMP012. Re-executável.

INSERT INTO public.permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('OPERACOES', 'CONSULTAR_COMPRAS_TRANSITO', 'Consultar Compras em Trânsito', 405, true)
ON CONFLICT DO NOTHING;

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo) VALUES
    (32, 'EMP010', 'OPERACOES', 'CONSULTAR_COMPRAS_TRANSITO', TRUE),
    (65, 'EMP012', 'OPERACOES', 'CONSULTAR_COMPRAS_TRANSITO', TRUE)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = TRUE;
