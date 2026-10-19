-- Análise de Vendas (Vendas)
--
-- Diagnóstico das vendas de um período (rede e posto a posto) com sugestões de
-- preço escritas por IA, sobre números lidos de vendas_diarias. Nada gravado.
--
-- Não concede a ninguém: só o superusuário passa por bypass.
-- Liberar em Usuários → Permissões.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('VENDAS', 'ANALISE_VENDAS', 'Análise de Vendas (IA)', 810, true)
ON CONFLICT DO NOTHING;
