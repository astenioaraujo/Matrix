-- Resumo de Vendas (Vendas)
--
-- Um mês e um posto (ou todos): uma linha por produto com preço de custo,
-- preço de venda, margem unitária, quantidade, valor e margem bruta, lidos de
-- vendas_diarias. Nada gravado.
--
-- Não concede a ninguém: só o superusuário passa por bypass.
-- Liberar em Usuários → Permissões.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('VENDAS', 'RESUMO_VENDAS', 'Resumo de Vendas', 775, true)
ON CONFLICT DO NOTHING;
