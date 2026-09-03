-- Consulta de Margem Unitária por Dia (Vendas)
--
-- Grid produto x posto, com preço de compra, preço de venda e margem unitária
-- de um dia, lidos de vendas_diarias.
--
-- Não concede a ninguém: só o superusuário passa por bypass.
-- Liberar em Usuários → Permissões.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('VENDAS', 'MARGEM_UNITARIA', 'Consulta de Margem Unitária por Dia', 780, true)
ON CONFLICT DO NOTHING;
