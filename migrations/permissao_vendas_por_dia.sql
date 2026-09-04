-- Consulta de Vendas por Dia (Vendas)
--
-- Mesmo grid da Margem Unitária por Dia, com os volumes do dia: litros
-- vendidos, valor vendido e margem bruta em dinheiro, por posto.
--
-- Não concede a ninguém: só o superusuário passa por bypass.
-- Liberar em Usuários → Permissões.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('VENDAS', 'VENDAS_POR_DIA', 'Consulta de Vendas por Dia', 790, true)
ON CONFLICT DO NOTHING;
