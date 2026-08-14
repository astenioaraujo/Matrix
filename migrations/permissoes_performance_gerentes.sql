-- Reorganiza o menu de Performances em dois blocos:
--   Avaliações de Funcionários (Executar / Consultar / Configurar, que já existiam)
--   Performance de Gerentes    (Vendas, e o que vier depois)
--
-- O submenu de Avaliações não ganha permissão própria: abre com qualquer uma
-- das três que já existem. Só o bloco novo precisa de catálogo.
--
-- Não concede nada a ninguém: só o superusuário passa por bypass.
-- Liberar em Usuários → Permissões para os gerentes.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('PERFORMANCES', 'PERFORMANCE_GERENTES', 'Performance de Gerentes', 1140, true),
    ('PERFORMANCES', 'GERENTES_VENDAS', 'Performance de Gerentes - Vendas', 1150, true)
ON CONFLICT DO NOTHING;
